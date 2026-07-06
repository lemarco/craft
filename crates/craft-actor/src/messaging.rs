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
//! E8 covers **fire-and-forget** delivery (`cast`). Cross-node request/reply
//! (`ask`) — correlating a reply back over the wire — is a later increment; the
//! [`ActorEnvelope`] already carries the `req_id` / `reply_expected` fields it
//! will need.

use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use craft_net::transport::{Body, BoxFuture};
use craft_net::{
    RequestHandler, Route, Transport, TransportError, decode_body, encode_body, send_actor_deliver,
};
use craft_proto::{ActorEnvelope, ActorRegistration, DeliverAck, NodeId};

use crate::ActorRegistry;
use crate::directory::ActorDirectory;
use crate::registry::DeliverError;

/// Why a cross-node `cast` failed (E8).
#[derive(Debug, thiserror::Error)]
pub enum CastError {
    /// The directory holds no live instance of the group anywhere.
    #[error("no live instance of group `{0}` in the cluster")]
    NoTarget(String),
    /// Local delivery to a resolved same-node instance failed.
    #[error(transparent)]
    Deliver(#[from] DeliverError),
    /// The envelope could not be shipped to the owning node.
    #[error("remote delivery to {node:?} failed: {reason}")]
    Transport {
        /// The node the envelope was addressed to.
        node: NodeId,
        /// The underlying transport error.
        reason: String,
    },
    /// The owning node received the envelope but could not deliver it.
    #[error("node {node:?} rejected delivery: {reason}")]
    Rejected {
        /// The node that rejected the message.
        node: NodeId,
        /// The reason it reported.
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

    /// Deliver an inbound envelope to a local instance and report the result.
    /// Called by the `/actor/deliver` handler.
    #[must_use]
    pub fn handle_deliver(&self, envelope: &ActorEnvelope) -> DeliverAck {
        match self.registry.deliver_local(
            &envelope.to.name,
            envelope.to.instance,
            &envelope.payload,
        ) {
            Ok(()) => DeliverAck {
                delivered: true,
                error: None,
            },
            Err(e) => DeliverAck {
                delivered: false,
                error: Some(e.to_string()),
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
            req_id: self.next_req.fetch_add(1, Ordering::Relaxed),
            payload,
            reply_expected: false,
        };
        let ack = send_actor_deliver(self.transport.as_ref(), node, &envelope)
            .await
            .map_err(|e| CastError::Transport {
                node,
                reason: e.to_string(),
            })?;
        if ack.delivered {
            Ok(())
        } else {
            Err(CastError::Rejected {
                node,
                reason: ack.error.unwrap_or_else(|| "unknown".to_string()),
            })
        }
    }
}

impl RequestHandler for ClusterMessaging {
    fn handle(&self, route: Route, body: Body) -> BoxFuture<'static, Result<Body, TransportError>> {
        let result = match route {
            Route::ActorDeliver => decode_body::<ActorEnvelope>(&body)
                .map_err(TransportError::from)
                .and_then(|envelope| Ok(encode_body(&self.handle_deliver(&envelope))?)),
            other => Err(TransportError::Io(format!(
                "messaging handler received unexpected route {other:?}"
            ))),
        };
        Box::pin(async move { result })
    }
}
