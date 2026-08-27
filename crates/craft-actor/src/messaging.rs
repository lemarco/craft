//! Cross-node actor message delivery (backlog E8,
//! [ADR 013](../../../docs/decisions/013-cross-node-actors.md),
//! [ADR 019](../../../docs/decisions/019-cluster-routing.md)).
//!
//! [`ClusterMessaging`] turns a logical send to a group name into delivery to a
//! concrete instance: it resolves a target through the cluster directory (E7)
//! using round-robin or keyed routing, then either hands the payload to the
//! local [`ActorRegistry`] or ships an [`ActorEnvelope`] to the owning node over
//! `/actor/deliver`. It also serves inbound `/actor/deliver` requests as a
//! [`RequestHandler`], decoding the envelope into the target actor's message
//! via [`UserActor::decode_message`](crate::UserActor::decode_message).
//!
//! Delivery covers both **fire-and-forget** (`cast`) and cross-node
//! **request/reply** (`ask`): an `ask` sets `reply_expected` on the
//! [`ActorEnvelope`], the receiver decodes it via
//! [`UserActor::decode_ask`](crate::UserActor::decode_ask) with a wire reply
//! port, and the handler's reply rides back in the [`DeliverAck`].

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use craft_net::transport::{Body, BoxFuture};
use craft_net::{
    RemoteError, RequestHandler, Route, Transport, TransportError, decode_body, encode_body,
    send_actor_deliver,
};
use craft_proto::{ActorEnvelope, ActorRegistration, DeliverAck, NodeId};

use crate::ActorRegistry;
use crate::directory::ActorDirectory;
use crate::registry::DeliverError;

/// How long the receiving node waits for an actor to answer a cross-node `ask`
/// before giving up and returning an empty reply. Bounds how long a
/// `/actor/deliver` stream is held open by a slow or non-replying handler.
const REMOTE_ASK_TIMEOUT: Duration = Duration::from_secs(30);

/// Upper bound on remembered `(origin, req_id)` ask results. A bounded FIFO, so
/// a long-lived node cannot grow the dedup table without limit; the window only
/// has to outlast a resend, not the whole session.
const DEDUP_CAPACITY: usize = 4096;

/// Remembers the reply produced for each already-served `(origin, req_id)` ask,
/// so an at-least-once *resend* replays the recorded answer instead of invoking
/// a side-effecting handler a second time (E8, ADR 013). Eviction is FIFO once
/// [`DEDUP_CAPACITY`] entries are held.
///
/// This coalesces **sequential** resends (a retry issued after the first
/// attempt returned): the first attempt records its result before the retry
/// arrives. Concurrent in-flight duplicates of the same key are *not* coalesced
/// — the current transport never duplicates a single in-flight request.
#[derive(Default)]
struct DedupCache {
    replies: HashMap<(NodeId, u64), Option<Vec<u8>>>,
    order: VecDeque<(NodeId, u64)>,
}

impl DedupCache {
    fn get(&self, key: &(NodeId, u64)) -> Option<Option<Vec<u8>>> {
        self.replies.get(key).cloned()
    }

    fn record(&mut self, key: (NodeId, u64), reply: Option<Vec<u8>>) {
        if self.replies.insert(key, reply).is_none() {
            self.order.push_back(key);
            if self.order.len() > DEDUP_CAPACITY
                && let Some(evicted) = self.order.pop_front()
            {
                self.replies.remove(&evicted);
            }
        }
    }
}

/// Why a cross-node `cast` failed (E8).
#[derive(Debug, thiserror::Error)]
pub enum CastError {
    /// The directory holds no live instance of the group anywhere.
    #[error("no live instance of group `{0}` in the cluster")]
    NoTarget(String),
    /// Local delivery to a resolved same-node instance failed.
    #[error(transparent)]
    Deliver(#[from] DeliverError),
    /// The envelope could not be shipped to the owning node, or that node
    /// rejected delivery.
    #[error(transparent)]
    Remote(#[from] RemoteError),
}

/// Why a cross-node `ask` (request/reply) failed (E8, ADR 013/019).
#[derive(Debug, thiserror::Error)]
pub enum AskError {
    /// The directory holds no live instance of the group anywhere.
    #[error("no live instance of group `{0}` in the cluster")]
    NoTarget(String),
    /// Local delivery to a resolved same-node instance failed.
    #[error(transparent)]
    Deliver(#[from] DeliverError),
    /// The envelope could not be shipped to the owning node, or that node
    /// rejected the ask.
    #[error(transparent)]
    Remote(#[from] RemoteError),
    /// The message was delivered but the actor never replied (dropped the reply
    /// port or exceeded the deadline).
    #[error("actor did not reply")]
    NoReply,
    /// The message was delivered to a same-node instance but no reply arrived
    /// within the ask deadline, so the caller stops waiting rather than
    /// blocking forever on a wedged handler.
    #[error("actor did not reply within {0:?}")]
    Timeout(Duration),
    /// The handler replied, but its reply value could not be serialized for the
    /// wire — a real error, distinct from the actor never answering.
    #[error("reply could not be encoded: {reason}")]
    ReplyEncode {
        /// The underlying serialization failure.
        reason: String,
    },
}

/// Routes actor messages to their target instance, locally or across the
/// cluster (E8). Cheap to share behind an `Arc`.
pub struct ClusterMessaging {
    node_id: NodeId,
    directory: Arc<ActorDirectory>,
    registry: ActorRegistry,
    transport: Arc<dyn Transport>,
    next_req: AtomicU64,
    /// Serve-side dedup of already-answered cross-node asks (ADR 013).
    dedup: Arc<Mutex<DedupCache>>,
}

impl ClusterMessaging {
    /// Wire messaging for `node_id`, resolving targets through `directory`,
    /// delivering locally through `registry`, and remotely over `transport`.
    #[must_use]
    pub fn new(
        node_id: NodeId,
        directory: Arc<ActorDirectory>,
        registry: ActorRegistry,
        transport: Arc<dyn Transport>,
    ) -> Self {
        Self {
            node_id,
            directory,
            registry,
            transport,
            next_req: AtomicU64::new(0),
            dedup: Arc::new(Mutex::new(DedupCache::default())),
        }
    }

    /// This node's id.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Cast `payload` to some instance of `group`, chosen round-robin across
    /// every node hosting the group (ADR 019).
    ///
    /// # Errors
    /// Returns [`CastError::NoTarget`] if the group has no instances, or a
    /// delivery/transport error otherwise.
    pub async fn cast(&self, group: &str, payload: Vec<u8>) -> Result<(), CastError> {
        let target = self
            .directory
            .pick_rr(group)
            .ok_or_else(|| CastError::NoTarget(group.to_string()))?;
        self.deliver(target, payload).await
    }

    /// Cast `payload` to the instance of `group` that `key` maps to, so all
    /// messages for a key reach the same instance while membership is stable.
    ///
    /// # Errors
    /// Returns [`CastError::NoTarget`] if the group has no instances, or a
    /// delivery/transport error otherwise.
    pub async fn cast_keyed<K: Hash>(
        &self,
        group: &str,
        key: &K,
        payload: Vec<u8>,
    ) -> Result<(), CastError> {
        let target = self
            .directory
            .pick_keyed(group, key)
            .ok_or_else(|| CastError::NoTarget(group.to_string()))?;
        self.deliver(target, payload).await
    }

    /// Ask some instance of `group` (round-robin) and await its reply
    /// (ADR 013/019). The target actor must implement
    /// [`UserActor::decode_ask`](crate::UserActor::decode_ask).
    ///
    /// # Errors
    /// Returns [`AskError`] if the group has no instances, delivery fails, or
    /// the actor never replies.
    pub async fn ask(&self, group: &str, payload: Vec<u8>) -> Result<Vec<u8>, AskError> {
        let target = self
            .directory
            .pick_rr(group)
            .ok_or_else(|| AskError::NoTarget(group.to_string()))?;
        self.deliver_ask(target, payload).await
    }

    /// Ask the instance of `group` that `key` maps to and await its reply, so
    /// all requests for a key reach the same instance while membership is
    /// stable.
    ///
    /// # Errors
    /// Returns [`AskError`] if the group has no instances, delivery fails, or
    /// the actor never replies.
    pub async fn ask_keyed<K: Hash>(
        &self,
        group: &str,
        key: &K,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, AskError> {
        let target = self
            .directory
            .pick_keyed(group, key)
            .ok_or_else(|| AskError::NoTarget(group.to_string()))?;
        self.deliver_ask(target, payload).await
    }

    /// Deliver an inbound envelope to a local instance and report the result,
    /// answering a cross-node `ask` (`reply_expected`) with the actor's encoded
    /// reply. Called by the `/actor/deliver` handler.
    pub async fn serve_deliver(&self, envelope: &ActorEnvelope) -> DeliverAck {
        serve_envelope(&self.registry, &self.dedup, envelope).await
    }

    /// Deliver a fire-and-forget (`cast`) envelope to a local instance.
    #[must_use]
    pub fn handle_deliver(&self, envelope: &ActorEnvelope) -> DeliverAck {
        deliver_cast(&self.registry, envelope)
    }

    async fn deliver_ask(
        &self,
        target: ActorRegistration,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, AskError> {
        if target.id.node == self.node_id {
            let rx =
                self.registry
                    .deliver_local_ask(&target.id.name, target.id.instance, &payload)?;
            return match tokio::time::timeout(REMOTE_ASK_TIMEOUT, rx).await {
                Ok(Ok(Ok(reply))) => Ok(reply),
                Ok(Ok(Err(reason))) => Err(AskError::ReplyEncode { reason }),
                Ok(Err(_)) => Err(AskError::NoReply),
                Err(_) => Err(AskError::Timeout(REMOTE_ASK_TIMEOUT)),
            };
        }
        let node = target.id.node;
        let envelope = ActorEnvelope {
            to: target.id,
            from: None,
            origin: Some(self.node_id),
            req_id: self.next_req.fetch_add(1, Ordering::Relaxed),
            payload,
            reply_expected: true,
        };
        let ack = send_actor_deliver(self.transport.as_ref(), node, &envelope)
            .await
            .map_err(|e| RemoteError::transport(node, e))?;
        if !ack.delivered {
            return Err(RemoteError::rejected(
                node,
                ack.error.unwrap_or_else(|| "unknown".to_string()),
            )
            .into());
        }
        match ack.reply {
            Some(reply) => Ok(reply),
            // Delivered but no reply bytes: prefer the server-reported reason
            // (e.g. a reply-encode failure or the deadline) over the opaque
            // "no reply", so a real error is not masked as a dropped port.
            None => match ack.error {
                Some(reason) => Err(RemoteError::rejected(node, reason).into()),
                None => Err(AskError::NoReply),
            },
        }
    }

    async fn deliver(&self, target: ActorRegistration, payload: Vec<u8>) -> Result<(), CastError> {
        if target.id.node == self.node_id {
            self.registry
                .deliver_local(&target.id.name, target.id.instance, &payload)?;
            return Ok(());
        }
        let node = target.id.node;
        let envelope = ActorEnvelope {
            to: target.id,
            from: None,
            origin: Some(self.node_id),
            req_id: self.next_req.fetch_add(1, Ordering::Relaxed),
            payload,
            reply_expected: false,
        };
        let ack = send_actor_deliver(self.transport.as_ref(), node, &envelope)
            .await
            .map_err(|e| RemoteError::transport(node, e))?;
        if ack.delivered {
            Ok(())
        } else {
            Err(
                RemoteError::rejected(node, ack.error.unwrap_or_else(|| "unknown".to_string()))
                    .into(),
            )
        }
    }
}

/// Serve an inbound envelope against `registry`, dispatching `ask`
/// (`reply_expected`) to the reply-bearing path and `cast` to fire-and-forget.
///
/// An `ask` that carries an [`ActorEnvelope::origin`] is deduplicated on
/// `(origin, req_id)`: a resend of an already-served request replays the
/// recorded reply instead of re-invoking the handler, so a side-effecting
/// handler runs at most once per logical request (ADR 013). Casts are
/// fire-and-forget and pass straight through.
async fn serve_envelope(
    registry: &ActorRegistry,
    dedup: &Mutex<DedupCache>,
    envelope: &ActorEnvelope,
) -> DeliverAck {
    if !envelope.reply_expected {
        return deliver_cast(registry, envelope);
    }
    let Some(origin) = envelope.origin else {
        return serve_ask(registry, envelope).await;
    };
    let key = (origin, envelope.req_id);
    if let Some(reply) = dedup.lock().unwrap().get(&key) {
        return DeliverAck {
            delivered: true,
            error: None,
            reply,
        };
    }
    let ack = serve_ask(registry, envelope).await;
    // Record only once the message reached a mailbox: a delivery that never ran
    // (unknown group / no instance) stays retryable against a later placement.
    if ack.delivered {
        dedup.lock().unwrap().record(key, ack.reply.clone());
    }
    ack
}

/// Deliver a fire-and-forget (`cast`) envelope to a local instance.
fn deliver_cast(registry: &ActorRegistry, envelope: &ActorEnvelope) -> DeliverAck {
    match registry.deliver_local(&envelope.to.name, envelope.to.instance, &envelope.payload) {
        Ok(()) => DeliverAck {
            delivered: true,
            error: None,
            reply: None,
        },
        Err(e) => DeliverAck {
            delivered: false,
            error: Some(e.to_string()),
            reply: None,
        },
    }
}

/// Deliver an `ask` envelope to a local instance and await the encoded reply
/// (bounded by [`REMOTE_ASK_TIMEOUT`]).
async fn serve_ask(registry: &ActorRegistry, envelope: &ActorEnvelope) -> DeliverAck {
    let rx = match registry.deliver_local_ask(
        &envelope.to.name,
        envelope.to.instance,
        &envelope.payload,
    ) {
        Ok(rx) => rx,
        Err(e) => {
            return DeliverAck {
                delivered: false,
                error: Some(e.to_string()),
                reply: None,
            };
        }
    };
    match tokio::time::timeout(REMOTE_ASK_TIMEOUT, rx).await {
        Ok(Ok(Ok(reply))) => DeliverAck {
            delivered: true,
            error: None,
            reply: Some(reply),
        },
        // The handler replied, but the reply value failed to serialize: a real
        // error, surfaced instead of masquerading as a dropped reply.
        Ok(Ok(Err(reason))) => DeliverAck {
            delivered: true,
            error: Some(format!("reply encode failed: {reason}")),
            reply: None,
        },
        // Delivered, but the actor dropped the reply port before answering.
        Ok(Err(_)) => DeliverAck {
            delivered: true,
            error: Some("actor dropped the reply".to_string()),
            reply: None,
        },
        Err(_) => DeliverAck {
            delivered: true,
            error: Some("actor did not reply before the deadline".to_string()),
            reply: None,
        },
    }
}

impl RequestHandler for ClusterMessaging {
    fn handle(&self, route: Route, body: Body) -> BoxFuture<'static, Result<Body, TransportError>> {
        match route {
            Route::ActorDeliver => {
                // An `ask` handler may await the actor's reply, so serving is
                // async; the serve path needs the registry plus the dedup table
                // to replay an at-least-once resend (ADR 013).
                let registry = self.registry.clone();
                let dedup = Arc::clone(&self.dedup);
                Box::pin(async move {
                    let envelope: ActorEnvelope = decode_body(&body)?;
                    let ack = serve_envelope(&registry, &dedup, &envelope).await;
                    Ok(encode_body(&ack)?)
                })
            }
            other => {
                let msg = format!("messaging handler received unexpected route {other:?}");
                Box::pin(async move { Err(TransportError::Io(msg)) })
            }
        }
    }
}
