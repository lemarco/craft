//! Auto-spawn-on-join: supervisor reconciles one worker per live cluster member.

use std::sync::Arc;

use crafty_actor::crafty_net::{LocalNetwork, Transport};
use crafty_actor::crafty_proto::{ActorId, ActorRegistration, ActorTypeId, NodeId};
use crafty_actor::{
    ActorDirectory, ActorRegistry, ClusterControl, ClusterState, ClusterSupervisor,
    ConfigCodecError, UserActor,
};

#[derive(Debug)]
struct WorkerError;

impl std::fmt::Display for WorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("worker error")
    }
}

impl std::error::Error for WorkerError {}

struct Worker;

impl UserActor for Worker {
    type Config = u32;
    type Message = ();
    type Error = WorkerError;

    fn start(_seed: Self::Config) -> Result<Self, Self::Error> {
        Ok(Worker)
    }

    fn handle(&mut self, _msg: Self::Message) -> impl Future<Output = Result<(), Self::Error>> {
        std::future::ready(Ok(()))
    }

    fn encode_config(config: &Self::Config) -> Result<Vec<u8>, ConfigCodecError> {
        crafty_actor::crafty_proto::encode(config)
            .map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }

    fn decode_config(bytes: &[u8]) -> Result<Self::Config, ConfigCodecError> {
        crafty_actor::crafty_proto::decode(bytes)
            .map_err(|e| ConfigCodecError::Codec(e.to_string()))
    }
}

struct MockState {
    leader: std::sync::atomic::AtomicBool,
    nodes: std::sync::Mutex<Vec<NodeId>>,
}

impl MockState {
    fn new(nodes: &[u64]) -> Self {
        Self {
            leader: std::sync::atomic::AtomicBool::new(true),
            nodes: std::sync::Mutex::new(nodes.iter().copied().map(NodeId).collect()),
        }
    }

    fn set_nodes(&self, nodes: &[u64]) {
        *self.nodes.lock().unwrap() = nodes.iter().copied().map(NodeId).collect();
    }
}

impl ClusterState for MockState {
    fn is_leader(&self) -> bool {
        self.leader.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn live_nodes(&self) -> Vec<NodeId> {
        self.nodes.lock().unwrap().clone()
    }

    fn reachable_nodes(&self) -> Vec<NodeId> {
        self.live_nodes()
    }
}

struct NodeCtx {
    control: Arc<ClusterControl>,
    registry: ActorRegistry,
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
    control.register_type::<Worker>();
    net.attach(NodeId(id), control.clone());
    NodeCtx { control, registry }
}

fn reg(node: u64, name: &str) -> ActorRegistration {
    ActorRegistration::new(
        ActorId {
            node: NodeId(node),
            name: name.to_string(),
            instance: 0,
            generation: 0,
        },
        ActorTypeId(std::any::type_name::<Worker>().to_string()),
        false,
    )
}

#[tokio::test]
async fn auto_worker_spawns_when_membership_grows() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1);
    let state = Arc::new(MockState::new(&[1]));
    let sup = ClusterSupervisor::new(Arc::clone(&n1.control), Arc::clone(&state));
    sup.manage_auto::<Worker>("w", 0);

    let first = sup.reconcile().await;
    assert_eq!(first.groups[0].total, 1);
    assert!(n1.registry.contains("w"));

    let n2 = node(&net, 2);
    state.set_nodes(&[1, 2]);

    let second = sup.reconcile().await;
    assert!(second.is_ok());
    assert_eq!(second.groups[0].total, 2);
    assert!(n2.registry.contains("w"), "joiner gets an auto worker");
    assert!(n1.registry.contains("w"), "existing worker untouched");

    let n3 = node(&net, 3);
    state.set_nodes(&[1, 2, 3]);

    let third = sup.reconcile().await;
    assert_eq!(third.groups[0].total, 3);
    assert!(n3.registry.contains("w"));
    assert_eq!(n1.registry.instance_count("w"), 1);
    assert_eq!(n2.registry.instance_count("w"), 1);
    assert_eq!(n3.registry.instance_count("w"), 1);
    assert_eq!(reg(3, "w").id.node, NodeId(3));
}
