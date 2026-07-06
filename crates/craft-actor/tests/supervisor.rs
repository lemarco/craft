//! Tests for the leader-only cluster supervisor (backlog E10): reconciliation
//! runs only on the leader, places one worker per node, surfaces placement
//! errors, and is idempotent once the directory reflects the placement.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use craft_actor::craft_net::{LocalNetwork, Transport};
use craft_actor::craft_proto::{
    self, ActorId, ActorRegistration, ActorTypeId, DirectoryUpdate, NodeId,
};
use craft_actor::{
    ActorDirectory, ActorRegistry, ClusterControl, ClusterState, ClusterSupervisor,
    ConfigCodecError, UserActor,
};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
#[error("worker error")]
struct WorkerError;

struct Worker;

impl UserActor for Worker {
    type Config = u32;
    type Message = ();
    type Error = WorkerError;

    fn start(_seed: Self::Config) -> Result<Self, Self::Error> {
        Ok(Worker)
    }

    async fn handle(&mut self, _msg: Self::Message) -> Result<(), Self::Error> {
        Ok(())
    }

    fn encode_config(config: &Self::Config) -> Result<Vec<u8>, ConfigCodecError> {
        craft_proto::encode(config).map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }

    fn decode_config(bytes: &[u8]) -> Result<Self::Config, ConfigCodecError> {
        craft_proto::decode(bytes).map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }
}

/// A test double for cluster leadership + membership.
struct MockState {
    leader: AtomicBool,
    nodes: Vec<NodeId>,
}

impl MockState {
    fn new(leader: bool, nodes: &[u64]) -> Self {
        Self {
            leader: AtomicBool::new(leader),
            nodes: nodes.iter().copied().map(NodeId).collect(),
        }
    }
}

impl ClusterState for MockState {
    fn is_leader(&self) -> bool {
        self.leader.load(Ordering::SeqCst)
    }
    fn live_nodes(&self) -> Vec<NodeId> {
        self.nodes.clone()
    }
}

struct NodeCtx {
    control: Arc<ClusterControl>,
    registry: ActorRegistry,
    directory: Arc<ActorDirectory>,
}

fn node(net: &LocalNetwork, id: u64) -> NodeCtx {
    let registry = ActorRegistry::new();
    let directory = ActorDirectory::new();
    let transport: Arc<dyn Transport> = Arc::new(net.clone());
    let control = Arc::new(ClusterControl::new(
        NodeId(id),
        registry.clone(),
        Arc::clone(&directory),
        transport,
    ));
    // Every node knows how to reconstruct Worker on request.
    control.register_type::<Worker>();
    net.attach(NodeId(id), control.clone());
    NodeCtx {
        control,
        registry,
        directory,
    }
}

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_follower_skips_reconciliation() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1);
    let state = MockState::new(false, &[1, 2]);
    let sup = ClusterSupervisor::new(Arc::clone(&n1.control), state);
    sup.manage::<Worker>("w", 2, 0);

    let report = sup.reconcile().await;
    assert!(!report.ran_as_leader);
    assert_eq!(report.spawns(), 0);
    assert!(!n1.registry.contains("w"), "no placement on a follower");
}

#[tokio::test]
async fn the_leader_places_one_worker_per_node() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1);
    let n2 = node(&net, 2);
    let sup = ClusterSupervisor::new(Arc::clone(&n1.control), MockState::new(true, &[1, 2]));
    sup.manage::<Worker>("w", 2, 0);
    assert_eq!(sup.managed_names(), vec!["w".to_string()]);

    let report = sup.reconcile().await;
    assert!(report.ran_as_leader);
    assert!(report.is_ok());
    assert_eq!(report.spawns(), 2);
    assert!(n1.registry.contains("w"), "placed on the leader");
    assert!(n2.registry.contains("w"), "placed on the follower");
}

#[tokio::test]
async fn reconcile_is_idempotent_once_the_directory_reflects_placement() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1);
    let _n2 = node(&net, 2); // must stay attached so remote spawns land
    let sup = ClusterSupervisor::new(Arc::clone(&n1.control), MockState::new(true, &[1, 2]));
    sup.manage::<Worker>("w", 2, 0);

    let first = sup.reconcile().await;
    assert_eq!(first.spawns(), 2);

    // The directory converges (as DirectorySync would) to reflect both workers.
    n1.directory.apply(&DirectoryUpdate {
        node: NodeId(1),
        epoch: 1,
        registrations: vec![reg(1, "w", 0)],
    });
    n1.directory.apply(&DirectoryUpdate {
        node: NodeId(2),
        epoch: 1,
        registrations: vec![reg(2, "w", 0)],
    });

    let second = sup.reconcile().await;
    assert!(second.is_ok(), "second pass planned no failing spawns");
    assert_eq!(
        second.spawns(),
        0,
        "declarative reconcile is a no-op once satisfied"
    );
}

#[tokio::test]
async fn placement_error_is_surfaced_per_group_without_aborting_the_pass() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1);
    // Ask for more workers than there are live nodes.
    let sup = ClusterSupervisor::new(Arc::clone(&n1.control), MockState::new(true, &[1, 2]));
    sup.manage::<Worker>("w", 3, 0);

    let report = sup.reconcile().await;
    assert!(report.ran_as_leader);
    assert!(!report.is_ok(), "the over-capacity group failed to place");
    assert_eq!(report.groups.len(), 1);
    assert!(report.groups[0].result.is_err());
}
