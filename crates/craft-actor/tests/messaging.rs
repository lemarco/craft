//! Tests for cross-node actor delivery (backlog E8): round-robin and keyed
//! routing that resolves through the directory and delivers either to a local
//! mailbox or over `/actor/deliver` to the owning node.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use craft_actor::craft_net::{LocalNetwork, Transport};
use craft_actor::craft_proto::{
    self, ActorId, ActorRegistration, ActorTypeId, DirectoryUpdate, NodeId,
};
use craft_actor::{
    ActorDirectory, ActorRegistry, CastError, ClusterMessaging, DeliverError, MessageDecodeError,
    UserActor,
};
use serde::{Deserialize, Serialize};

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
        CastError::Rejected { node, reason } => {
            assert_eq!(node, NodeId(2));
            assert!(reason.contains("ghost"), "reason surfaced: {reason}");
        }
        other => panic!("expected Rejected, got {other:?}"),
    }
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
