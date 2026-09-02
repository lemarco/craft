//! Tests for graceful drain-with-timeout (backlog E12, drain-timeout) and stateful
//! actor migration across nodes (E12, cross-node-actors), driven over a `LocalNetwork`.

#![allow(clippy::unused_async_trait_impl)] // test mock actors have sync handle bodies

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use trembita_runtime::trembita_net::{LocalNetwork, Transport};
use trembita_runtime::trembita_proto::{self, ActorId, NodeId};
use trembita_runtime::{
    ActorDirectory, ActorRegistry, ClusterControl, ConfigCodecError, DrainOutcome, MigrateError,
    MigrationError, RpcReplyPort, UserActor,
};

// ---------------------------------------------------------------------------
// Drain: a counter actor and a blocking actor
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
#[error("actor error")]
struct ActorErr;

/// Increments a shared counter per message; no in-handler delay.
struct Counting {
    processed: Arc<AtomicUsize>,
}

impl UserActor for Counting {
    type Config = Arc<AtomicUsize>;
    type Message = ();
    type Error = ActorErr;

    fn start(processed: Self::Config) -> Result<Self, Self::Error> {
        Ok(Self { processed })
    }

    async fn handle(&mut self, _msg: Self::Message) -> Result<(), Self::Error> {
        self.processed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Sleeps inside every handler, so a drain cannot complete quickly.
struct Blocking;

impl UserActor for Blocking {
    type Config = ();
    type Message = ();
    type Error = ActorErr;

    fn start(_config: Self::Config) -> Result<Self, Self::Error> {
        Ok(Blocking)
    }

    async fn handle(&mut self, _msg: Self::Message) -> Result<(), Self::Error> {
        tokio::time::sleep(Duration::from_secs(3600)).await;
        Ok(())
    }
}

#[tokio::test]
async fn drain_completes_when_work_finishes_in_time() {
    let registry = ActorRegistry::new();
    let processed = Arc::new(AtomicUsize::new(0));
    let actor = registry.spawn::<Counting>("c", processed.clone()).unwrap();

    for _ in 0..5 {
        actor.send(()).unwrap();
    }

    let outcome = registry
        .stop_graceful("c", Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(outcome, DrainOutcome::Completed);
    assert_eq!(processed.load(Ordering::SeqCst), 5, "all queued work ran");
    assert!(!registry.contains("c"), "group removed after drain");
}

#[tokio::test]
async fn drain_times_out_and_rejects_new_messages() {
    let registry = ActorRegistry::new();
    let actor = registry.spawn::<Blocking>("b", ()).unwrap();

    // Occupy the single handler with a message that never finishes.
    actor.send(()).unwrap();

    let reg = registry.clone();
    let drain =
        tokio::spawn(async move { reg.stop_graceful("b", Duration::from_millis(50)).await });

    // Once draining begins, new sends are rejected (drain-timeout step 1).
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        actor.send(()),
        Err(trembita_runtime::SendError::Draining),
        "draining group rejects new messages"
    );

    let outcome = drain.await.unwrap().unwrap();
    assert_eq!(outcome, DrainOutcome::TimedOut, "force stopped on timeout");
}

#[tokio::test]
async fn per_group_drain_timeout_overrides_cluster_default() {
    let registry = ActorRegistry::new();
    let processed = Arc::new(AtomicUsize::new(0));
    let actor = registry.spawn::<Counting>("c", processed.clone()).unwrap();
    for _ in 0..3 {
        actor.send(()).unwrap();
    }
    registry
        .set_group_drain_timeout("c", Some(Duration::from_secs(30)))
        .unwrap();
    assert_eq!(
        registry.group_drain_timeout("c"),
        Some(Duration::from_secs(30))
    );
    let outcome = registry
        .stop_graceful("c", Duration::from_millis(1))
        .await
        .unwrap();
    assert_eq!(outcome, DrainOutcome::Completed);
    assert_eq!(processed.load(Ordering::SeqCst), 3);
}

// ---------------------------------------------------------------------------
// Migration: a stateful counter that snapshots/restores its count
// ---------------------------------------------------------------------------

enum CounterMsg {
    Inc,
    Get(RpcReplyPort<u64>),
}

struct Counter {
    count: u64,
}

impl UserActor for Counter {
    type Config = u64;
    type Message = CounterMsg;
    type Error = ActorErr;

    const MIGRATABLE: bool = true;

    fn start(initial: Self::Config) -> Result<Self, Self::Error> {
        Ok(Self { count: initial })
    }

    async fn handle(&mut self, msg: Self::Message) -> Result<(), Self::Error> {
        match msg {
            CounterMsg::Inc => self.count += 1,
            CounterMsg::Get(port) => {
                let _ = port.reply(self.count);
            }
        }
        Ok(())
    }

    fn encode_config(config: &Self::Config) -> Result<Vec<u8>, ConfigCodecError> {
        trembita_proto::encode(config).map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }

    fn decode_config(bytes: &[u8]) -> Result<Self::Config, ConfigCodecError> {
        trembita_proto::decode(bytes).map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }

    fn migration_snapshot(&self) -> Result<Vec<u8>, MigrationError> {
        trembita_proto::encode(&self.count).map_err(MigrationError::new)
    }

    fn restore_migration(&mut self, snapshot: &[u8]) -> Result<(), MigrationError> {
        self.count = trembita_proto::decode(snapshot).map_err(MigrationError::new)?;
        Ok(())
    }
}

struct Node {
    control: Arc<ClusterControl>,
    registry: ActorRegistry,
}

fn node(net: &LocalNetwork, id: u64) -> Node {
    let registry = ActorRegistry::new();
    let directory = ActorDirectory::new();
    let transport: Arc<dyn Transport> = Arc::new(net.clone());
    let control = Arc::new(ClusterControl::new(
        NodeId(id),
        registry.clone(),
        directory,
        transport,
    ));
    control.register_type::<Counter>();
    net.attach(NodeId(id), control.clone());
    Node { control, registry }
}

#[tokio::test]
async fn migration_transfers_state_and_stops_the_source() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1);
    let n2 = node(&net, 2);

    // Start a counter on node 1 and advance its state to 3.
    n1.control
        .spawn_remote::<Counter>(NodeId(1), "c", 0)
        .await
        .unwrap();
    let src = n1.registry.get::<Counter>("c").unwrap();
    src.send(CounterMsg::Inc).unwrap();
    src.send(CounterMsg::Inc).unwrap();
    src.send(CounterMsg::Inc).unwrap();

    let from = ActorId {
        node: NodeId(1),
        name: "c".to_string(),
        instance: 0,
        generation: 0,
    };
    let new_id = n1
        .control
        .migrate::<Counter>(from, NodeId(2), 0, Duration::from_secs(5))
        .await
        .unwrap();

    assert_eq!(new_id.node, NodeId(2), "replacement is on the target node");
    assert_eq!(new_id.generation, 1, "generation bumped past the source");
    assert!(!n1.registry.contains("c"), "source drained and stopped");

    // The migrated actor carries the source's state (count == 3), because the
    // snapshot rode the source mailbox after the three increments.
    let moved = n2.registry.get::<Counter>("c").unwrap();
    let count = moved.ask(CounterMsg::Get).await.unwrap();
    assert_eq!(count, 3, "state migrated with the actor");
}

#[tokio::test]
async fn migrate_rejects_a_non_local_instance() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1);
    let _n2 = node(&net, 2);

    let foreign = ActorId {
        node: NodeId(2),
        name: "c".to_string(),
        instance: 0,
        generation: 0,
    };
    let err = n1
        .control
        .migrate::<Counter>(foreign, NodeId(1), 0, Duration::from_secs(5))
        .await
        .unwrap_err();
    assert!(matches!(err, MigrateError::NotLocal(_)));
}

#[tokio::test]
async fn migrate_to_the_same_node_is_rejected() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1);

    n1.control
        .spawn_remote::<Counter>(NodeId(1), "c", 0)
        .await
        .unwrap();
    let from = ActorId {
        node: NodeId(1),
        name: "c".to_string(),
        instance: 0,
        generation: 0,
    };
    let err = n1
        .control
        .migrate::<Counter>(from, NodeId(1), 0, Duration::from_secs(5))
        .await
        .unwrap_err();
    assert!(matches!(err, MigrateError::SameNode(NodeId(1))));
}
