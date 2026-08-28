//! Tests for cross-node actor delivery (backlog E8): round-robin and keyed
//! routing that resolves through the directory and delivers either to a local
//! mailbox or over `/actor/deliver` to the owning node.

#![allow(clippy::unused_async_trait_impl)] // test mock actors have sync handle bodies

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use craft_actor::craft_net::{LocalNetwork, RemoteError, Transport};
use craft_actor::craft_proto::{
    self, ActorEnvelope, ActorId, ActorRegistration, ActorTypeId, DeliverAck, DirectoryUpdate,
    NodeId,
};
use craft_actor::{
    ActorDirectory, ActorRegistry, CastError, ClusterAskError, ClusterMessaging, DeliverError,
    DirectoryPolicy, DirectoryRetry, MessageDecodeError, RpcReplyPort, UserActor, WireReplyPort,
};
use serde::{Deserialize, Serialize, Serializer};

// ---------------------------------------------------------------------------
// A remotely-addressable counting actor
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
#[error("worker error")]
struct WorkerError;

#[derive(Debug, Serialize, Deserialize)]
enum Work {
    Add(u64),
}

/// Increments a shared counter so the test can observe delivery. Its config is
/// the counter it should bump.
struct Worker {
    counter: Arc<AtomicU64>,
}

impl UserActor for Worker {
    type Config = Arc<AtomicU64>;
    type Message = Work;
    type Error = WorkerError;

    fn start(counter: Self::Config) -> Result<Self, Self::Error> {
        Ok(Worker { counter })
    }

    async fn handle(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        match msg {
            Work::Add(n) => {
                self.counter.fetch_add(n, Ordering::SeqCst);
            }
        }
        Ok(())
    }

    fn decode_message(payload: &[u8]) -> Result<Self::Message, MessageDecodeError> {
        craft_proto::decode(payload).map_err(|e| MessageDecodeError::Decode(e.to_string()))
    }
}

/// A local-only actor that never opts into remote addressing.
#[derive(Debug, thiserror::Error)]
#[error("local error")]
struct LocalError;

struct LocalOnly;

impl UserActor for LocalOnly {
    type Config = ();
    type Message = ();
    type Error = LocalError;

    fn start(_config: Self::Config) -> Result<Self, Self::Error> {
        Ok(LocalOnly)
    }

    async fn handle(&mut self, _msg: Self::Message) -> Result<(), Self::Error> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// An actor supporting cross-node request/reply (`ask`)
// ---------------------------------------------------------------------------

/// Wire request for the fire-and-forget `Add`.
#[derive(Debug, Serialize, Deserialize)]
struct AddReq(u64);

/// Wire request for the `Get` ask (unit — the reply carries the value).
#[derive(Debug, Serialize, Deserialize)]
struct GetReq;

/// Accumulates casts and answers asks with the running total.
enum AccMsg {
    Add(u64),
    Get(RpcReplyPort<u64>),
}

struct Accum {
    total: u64,
}

impl UserActor for Accum {
    type Config = ();
    type Message = AccMsg;
    type Error = WorkerError;

    fn start(_config: Self::Config) -> Result<Self, Self::Error> {
        Ok(Accum { total: 0 })
    }

    async fn handle(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        match msg {
            AccMsg::Add(n) => self.total += n,
            AccMsg::Get(reply) => {
                let _ = reply.reply(self.total);
            }
        }
        Ok(())
    }

    fn decode_message(payload: &[u8]) -> Result<Self::Message, MessageDecodeError> {
        let AddReq(n) =
            craft_proto::decode(payload).map_err(|e| MessageDecodeError::Decode(e.to_string()))?;
        Ok(AccMsg::Add(n))
    }

    fn decode_ask(
        payload: &[u8],
        reply: WireReplyPort,
    ) -> Result<Self::Message, MessageDecodeError> {
        let _req: GetReq =
            craft_proto::decode(payload).map_err(|e| MessageDecodeError::Decode(e.to_string()))?;
        Ok(AccMsg::Get(reply.reply_port::<u64>()))
    }
}

// ---------------------------------------------------------------------------
// A side-effecting `ask` actor (for dedup)
// ---------------------------------------------------------------------------

/// Wire request for the side-effecting `Bump` ask (unit — the reply carries the
/// new value).
#[derive(Debug, Serialize, Deserialize)]
struct BumpReq;

/// A single ask that both mutates (bumps a shared counter) and replies with the
/// new value, so a test can distinguish "handler ran" from "reply replayed".
enum BumpMsg {
    Bump(RpcReplyPort<u64>),
}

struct Bump {
    counter: Arc<AtomicU64>,
}

impl UserActor for Bump {
    type Config = Arc<AtomicU64>;
    type Message = BumpMsg;
    type Error = WorkerError;

    fn start(counter: Self::Config) -> Result<Self, Self::Error> {
        Ok(Bump { counter })
    }

    async fn handle(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        match msg {
            BumpMsg::Bump(reply) => {
                let value = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
                let _ = reply.reply(value);
            }
        }
        Ok(())
    }

    fn decode_ask(
        payload: &[u8],
        reply: WireReplyPort,
    ) -> Result<Self::Message, MessageDecodeError> {
        let _req: BumpReq =
            craft_proto::decode(payload).map_err(|e| MessageDecodeError::Decode(e.to_string()))?;
        Ok(BumpMsg::Bump(reply.reply_port::<u64>()))
    }
}

// ---------------------------------------------------------------------------
// An `ask` actor whose reply value cannot be serialized
// ---------------------------------------------------------------------------

/// A reply type whose `Serialize` always fails, to exercise the wire
/// reply-encode error path.
struct Unencodable;

impl Serialize for Unencodable {
    fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("this reply never serializes"))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct BadReq;

enum BadMsg {
    Get(RpcReplyPort<Unencodable>),
}

struct BadReply;

impl UserActor for BadReply {
    type Config = ();
    type Message = BadMsg;
    type Error = WorkerError;

    fn start(_config: Self::Config) -> Result<Self, Self::Error> {
        Ok(BadReply)
    }

    async fn handle(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        match msg {
            BadMsg::Get(reply) => {
                let _ = reply.reply(Unencodable);
            }
        }
        Ok(())
    }

    fn decode_ask(
        payload: &[u8],
        reply: WireReplyPort,
    ) -> Result<Self::Message, MessageDecodeError> {
        let _req: BadReq =
            craft_proto::decode(payload).map_err(|e| MessageDecodeError::Decode(e.to_string()))?;
        Ok(BadMsg::Get(reply.reply_port::<Unencodable>()))
    }
}

/// Build a messaging plane on `net` for node `id` over `registry`.
fn messaging_on(
    net: &LocalNetwork,
    id: u64,
    registry: ActorRegistry,
) -> (Arc<ClusterMessaging>, Arc<ActorDirectory>) {
    let directory = ActorDirectory::new();
    let transport: Arc<dyn Transport> = Arc::new(net.clone());
    let messaging = Arc::new(ClusterMessaging::new(
        NodeId(id),
        Arc::clone(&directory),
        registry,
        transport,
    ));
    net.attach(NodeId(id), messaging.clone());
    (messaging, directory)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn reg(node: u64, name: &str, instance: u32) -> ActorRegistration {
    ActorRegistration {
        id: ActorId {
            node: NodeId(node),
            name: name.to_string(),
            instance,
            generation: 0,
        },
        actor_type: ActorTypeId("Worker".to_string()),
        migratable: false,
    }
}

fn update(node: u64, regs: Vec<ActorRegistration>) -> DirectoryUpdate {
    DirectoryUpdate {
        node: NodeId(node),
        epoch: 1,
        registrations: regs,
    }
}

fn add(n: u64) -> Vec<u8> {
    craft_proto::encode(&Work::Add(n)).unwrap()
}

/// A node: a production registry hosting one `worker` singleton (bumping
/// `counter`), a directory, and messaging attached to the switch.
struct Node {
    messaging: Arc<ClusterMessaging>,
    directory: Arc<ActorDirectory>,
    counter: Arc<AtomicU64>,
}

fn node(net: &LocalNetwork, id: u64) -> Node {
    let counter = Arc::new(AtomicU64::new(0));
    let registry = ActorRegistry::new();
    registry
        .spawn::<Worker>("worker", counter.clone())
        .expect("spawn worker");
    let directory = ActorDirectory::new();
    let transport: Arc<dyn Transport> = Arc::new(net.clone());
    let messaging = Arc::new(ClusterMessaging::new(
        NodeId(id),
        Arc::clone(&directory),
        registry,
        transport,
    ));
    net.attach(NodeId(id), messaging.clone());
    Node {
        messaging,
        directory,
        counter,
    }
}

async fn eventually(mut cond: impl FnMut() -> bool) {
    for _ in 0..200 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("condition not met within the deadline");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn round_robin_spreads_across_local_and_remote_nodes() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1);
    let n2 = node(&net, 2);

    // Node 1's directory knows both singletons (as E7 would have converged).
    n1.directory.apply(&update(1, vec![reg(1, "worker", 0)]));
    n1.directory.apply(&update(2, vec![reg(2, "worker", 0)]));

    // Four RR casts land two on the local node and two on the remote node.
    for _ in 0..4 {
        n1.messaging.cast("worker", add(1)).await.unwrap();
    }

    eventually(|| n1.counter.load(Ordering::SeqCst) == 2).await;
    eventually(|| n2.counter.load(Ordering::SeqCst) == 2).await;
}

#[tokio::test]
async fn keyed_routing_pins_a_key_to_one_node() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1);
    let n2 = node(&net, 2);
    n1.directory.apply(&update(1, vec![reg(1, "worker", 0)]));
    n1.directory.apply(&update(2, vec![reg(2, "worker", 0)]));

    for _ in 0..6 {
        n1.messaging
            .cast_keyed("worker", &"tenant-42", add(1))
            .await
            .unwrap();
    }

    // Every message for the key went to exactly one node (total is 6).
    eventually(|| n1.counter.load(Ordering::SeqCst) + n2.counter.load(Ordering::SeqCst) == 6).await;
    let (a, b) = (
        n1.counter.load(Ordering::SeqCst),
        n2.counter.load(Ordering::SeqCst),
    );
    assert!(
        (a == 6 && b == 0) || (a == 0 && b == 6),
        "keyed routing split the key across nodes: {a} + {b}"
    );
}

#[tokio::test]
async fn cast_to_unknown_group_yields_no_target() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1);
    let err = n1.messaging.cast("nope", add(1)).await.unwrap_err();
    assert!(matches!(err, CastError::NoTarget(g) if g == "nope"));
}

#[tokio::test]
async fn remote_node_missing_the_instance_is_reported_as_rejected() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1);
    let _n2 = node(&net, 2);

    // The directory claims node 2 hosts a "ghost" group it never spawned.
    n1.directory.apply(&update(2, vec![reg(2, "ghost", 0)]));

    let err = n1.messaging.cast("ghost", add(1)).await.unwrap_err();
    match err {
        CastError::Remote(RemoteError::Rejected { node, reason }) => {
            assert_eq!(node, NodeId(2));
            assert!(reason.contains("ghost"), "reason surfaced: {reason}");
        }
        other => panic!("expected Rejected, got {other:?}"),
    }
}

#[tokio::test]
async fn ask_round_trips_a_reply_across_the_wire() {
    let net = LocalNetwork::new();

    // Node 1 hosts the `accum` actor; node 2 is a bare client plane.
    let reg1 = ActorRegistry::new();
    reg1.spawn::<Accum>("accum", ()).expect("spawn accum");
    let (_m1, _d1) = messaging_on(&net, 1, reg1);
    let (m2, d2) = messaging_on(&net, 2, ActorRegistry::new());

    // Node 2 knows node 1 hosts the group (as E7 directory sync would have set).
    d2.apply(&update(1, vec![reg(1, "accum", 0)]));

    // A remote cast bumps the total, then a remote ask reads it back.
    m2.cast("accum", craft_proto::encode(&AddReq(7)).unwrap())
        .await
        .expect("remote cast");
    let reply = m2
        .ask("accum", craft_proto::encode(&GetReq).unwrap())
        .await
        .expect("remote ask");
    let total: u64 = craft_proto::decode(&reply).expect("decode reply");
    assert_eq!(total, 7, "ask returned the actor's running total");
}

#[tokio::test]
async fn ask_to_a_non_addressable_actor_is_rejected() {
    let net = LocalNetwork::new();

    // `worker` only overrides decode_message (cast), not decode_ask.
    let reg1 = ActorRegistry::new();
    reg1.spawn::<Worker>("worker", Arc::new(AtomicU64::new(0)))
        .unwrap();
    let (_m1, _d1) = messaging_on(&net, 1, reg1);
    let (m2, d2) = messaging_on(&net, 2, ActorRegistry::new());
    d2.apply(&update(1, vec![reg(1, "worker", 0)]));

    let err = m2.ask("worker", Vec::new()).await.unwrap_err();
    match err {
        ClusterAskError::Remote(RemoteError::Rejected { node, reason }) => {
            assert_eq!(node, NodeId(1));
            assert!(
                reason.contains("not remotely addressable"),
                "reason surfaced: {reason}"
            );
        }
        other => panic!("expected Rejected, got {other:?}"),
    }
}

#[tokio::test]
async fn ask_to_unknown_group_yields_no_target() {
    let net = LocalNetwork::new();
    let (m1, _d1) = messaging_on(&net, 1, ActorRegistry::new());
    let err = m1.ask("nope", Vec::new()).await.unwrap_err();
    assert!(matches!(err, ClusterAskError::NoTarget(g) if g == "nope"));
}

#[tokio::test]
async fn local_only_actor_rejects_remote_style_delivery() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1);

    // A local-only group (never overrides decode_message) advertised in the
    // directory: routing resolves it locally, but delivery cannot decode.
    let reg2 = ActorRegistry::new();
    reg2.spawn::<LocalOnly>("local", ()).unwrap();
    // Rebuild a messaging over the *same* registry so the local group exists.
    let transport: Arc<dyn Transport> = Arc::new(net.clone());
    let dir = ActorDirectory::new();
    dir.apply(&update(1, vec![reg(1, "local", 0)]));
    let messaging = ClusterMessaging::new(NodeId(1), Arc::clone(&dir), reg2, transport);

    let err = messaging.cast("local", add(1)).await.unwrap_err();
    assert!(matches!(
        err,
        CastError::Deliver(DeliverError::Decode(MessageDecodeError::NotAddressable))
    ));
    // Keep n1 alive for the duration.
    drop(n1);
}

#[tokio::test]
async fn duplicate_ask_runs_the_handler_once_and_replays_the_reply() {
    let net = LocalNetwork::new();
    let counter = Arc::new(AtomicU64::new(0));
    let registry = ActorRegistry::new();
    registry.spawn::<Bump>("bump", counter.clone()).unwrap();
    let (messaging, _dir) = messaging_on(&net, 1, registry);

    // Two identical envelopes: same origin + req_id (an at-least-once resend).
    let envelope = ActorEnvelope {
        to: ActorId {
            node: NodeId(1),
            name: "bump".to_string(),
            instance: 0,
            generation: 0,
        },
        from: None,
        origin: Some(NodeId(2)),
        req_id: 42,
        payload: craft_proto::encode(&BumpReq).unwrap(),
        reply_expected: true,
    };

    let decode = |ack: &DeliverAck| -> u64 {
        craft_proto::decode(ack.reply.as_ref().expect("reply present")).unwrap()
    };

    let first = messaging.serve_deliver(&envelope).await;
    let second = messaging.serve_deliver(&envelope).await;

    assert!(first.delivered && second.delivered);
    assert_eq!(decode(&first), 1, "first ask ran the handler");
    assert_eq!(decode(&second), 1, "resend replays the recorded reply");
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "the side-effecting handler ran exactly once"
    );

    // A different req_id from the same origin is a distinct request: it runs.
    let mut fresh = envelope.clone();
    fresh.req_id = 43;
    let third = messaging.serve_deliver(&fresh).await;
    assert_eq!(decode(&third), 2, "a new req_id is served afresh");
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reply_encode_failure_surfaces_as_a_real_error() {
    let net = LocalNetwork::new();
    let registry = ActorRegistry::new();
    registry.spawn::<BadReply>("bad", ()).unwrap();
    let (messaging, _dir) = messaging_on(&net, 1, registry);

    let envelope = ActorEnvelope {
        to: ActorId {
            node: NodeId(1),
            name: "bad".to_string(),
            instance: 0,
            generation: 0,
        },
        from: None,
        origin: Some(NodeId(2)),
        req_id: 1,
        payload: craft_proto::encode(&BadReq).unwrap(),
        reply_expected: true,
    };

    let ack = messaging.serve_deliver(&envelope).await;
    assert!(ack.delivered, "the message reached the handler");
    assert!(ack.reply.is_none(), "no reply bytes on encode failure");
    let error = ack
        .error
        .expect("an encode failure is surfaced, not silent");
    assert!(
        error.contains("reply encode failed"),
        "distinct from a dropped reply: {error}"
    );
}

#[tokio::test(start_paused = true)]
async fn ask_linearizable_retries_until_directory_has_a_target() {
    let net = LocalNetwork::new();
    let counter = Arc::new(AtomicU64::new(0));
    let registry = ActorRegistry::new();
    registry.spawn::<Bump>("bump", counter.clone()).unwrap();
    let directory = ActorDirectory::new();
    let transport: Arc<dyn Transport> = Arc::new(net.clone());
    let messaging = Arc::new(ClusterMessaging::with_policy(
        NodeId(1),
        Arc::clone(&directory),
        registry,
        transport,
        DirectoryPolicy::ReadYourWrites,
        DirectoryRetry {
            max_attempts: 10,
            backoff: Duration::from_millis(10),
        },
    ));
    net.attach(NodeId(1), messaging.clone());

    let payload = craft_proto::encode(&BumpReq).unwrap();
    let ask = {
        let messaging = Arc::clone(&messaging);
        tokio::spawn(async move { messaging.ask_linearizable("bump", payload).await })
    };

    tokio::time::advance(Duration::from_millis(15)).await;
    directory.apply(&update(1, vec![reg(1, "bump", 0)]));

    let reply = ask.await.expect("task").expect("linearizable ask");
    let got: u64 = craft_proto::decode(&reply).unwrap();
    assert_eq!(got, 1);
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cast_session_pins_to_the_same_instance() {
    let net = LocalNetwork::new();
    let counter = Arc::new(AtomicU64::new(0));
    let registry = ActorRegistry::new();
    registry.spawn::<Worker>("w", counter.clone()).unwrap();
    let (messaging, directory) = messaging_on(&net, 1, registry);
    directory.apply(&update(1, vec![reg(1, "w", 0)]));

    let cluster = directory.cluster("w");
    let session = cluster
        .session_keyed(&"tenant-1", Some(Duration::from_secs(60)))
        .expect("session");
    let payload = craft_proto::encode(&Work::Add(1)).unwrap();
    messaging
        .cast_session(&session, payload.clone())
        .await
        .unwrap();
    messaging.cast_session(&session, payload).await.unwrap();
    eventually(|| counter.load(Ordering::SeqCst) == 2).await;
}
