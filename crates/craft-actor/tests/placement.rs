//! Tests for the actor control plane (backlog E9): the pure one-per-node
//! placement planner, and remote spawn / cluster scale over a `LocalNetwork`.

#![allow(clippy::unused_async_trait_impl)] // test mock actors have sync handle bodies

use std::sync::Arc;

use craft_actor::craft_net::{LocalNetwork, RemoteError, Transport};
use craft_actor::craft_proto::{
    self, ActorId, ActorRegistration, ActorTypeId, NodeId, ScaleRequest, StopRequest,
};
use craft_actor::{
    ActorDirectory, ActorRegistry, ClusterControl, ClusterScaleError, ClusterState,
    ConfigCodecError, RemoteSpawnError, ScaleError, UserActor, plan_scale,
};

// ---------------------------------------------------------------------------
// A remotely-spawnable actor
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

/// A local-spawn-only actor (never overrides the config codec).
struct LocalOnly;

impl UserActor for LocalOnly {
    type Config = ();
    type Message = ();
    type Error = WorkerError;

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

fn nodes(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

// ---------------------------------------------------------------------------
// Pure placement planner
// ---------------------------------------------------------------------------

#[test]
fn plan_fills_empty_cluster_one_per_node() {
    let plan = plan_scale(3, &nodes(&[1, 2, 3]), &[]).unwrap();
    assert_eq!(plan.spawns, nodes(&[1, 2, 3]));
    assert!(plan.removes.is_empty());
}

#[test]
fn plan_keeps_existing_and_spawns_the_rest() {
    let current = vec![reg(2, "w", 0)];
    let plan = plan_scale(3, &nodes(&[1, 2, 3]), &current).unwrap();
    assert_eq!(plan.spawns, nodes(&[1, 3]), "node 2 is kept");
    assert!(plan.removes.is_empty());
}

#[test]
fn plan_rejects_more_instances_than_nodes() {
    let err = plan_scale(4, &nodes(&[1, 2, 3]), &[]).unwrap_err();
    assert!(matches!(
        err,
        ScaleError::InsufficientNodes { total: 4, nodes: 3 }
    ));
}

#[test]
fn plan_scales_down_by_removing_demoted_nodes() {
    let current = vec![reg(1, "w", 0), reg(2, "w", 0)];
    let plan = plan_scale(1, &nodes(&[1, 2, 3]), &current).unwrap();
    assert!(plan.spawns.is_empty());
    // Lowest-NodeId host (1) is kept; node 2's instance is scheduled to stop.
    assert_eq!(plan.removes, vec![reg(2, "w", 0).id]);
}

#[test]
fn plan_removes_instances_on_dead_nodes() {
    // Node 3 hosts an instance but is no longer in the live membership.
    let current = vec![reg(3, "w", 0)];
    let plan = plan_scale(2, &nodes(&[1, 2]), &current).unwrap();
    assert_eq!(plan.spawns, nodes(&[1, 2]));
    assert_eq!(plan.removes, vec![reg(3, "w", 0).id]);
}

#[test]
fn plan_prunes_extra_instances_on_a_kept_node() {
    let current = vec![reg(2, "w", 0), reg(2, "w", 1)];
    let plan = plan_scale(1, &nodes(&[2]), &current).unwrap();
    assert!(plan.spawns.is_empty());
    assert_eq!(
        plan.removes,
        vec![reg(2, "w", 1).id],
        "keep the first, drop extras"
    );
}

// ---------------------------------------------------------------------------
// Remote spawn + scale over LocalNetwork
// ---------------------------------------------------------------------------

struct Node {
    control: Arc<ClusterControl>,
    registry: ActorRegistry,
    directory: Arc<ActorDirectory>,
}

fn node(net: &LocalNetwork, id: u64, register_worker: bool) -> Node {
    node_inner(net, id, register_worker, None)
}

/// A fixed leadership/membership view for leader-gating tests.
struct MockState {
    leader: bool,
    live: Vec<NodeId>,
}

impl ClusterState for MockState {
    fn is_leader(&self) -> bool {
        self.leader
    }
    fn live_nodes(&self) -> Vec<NodeId> {
        self.live.clone()
    }
}

fn node_with_state(net: &LocalNetwork, id: u64, register_worker: bool, state: MockState) -> Node {
    node_inner(net, id, register_worker, Some(Arc::new(state)))
}

fn node_inner(
    net: &LocalNetwork,
    id: u64,
    register_worker: bool,
    state: Option<Arc<dyn ClusterState>>,
) -> Node {
    let registry = ActorRegistry::new();
    let directory = ActorDirectory::new();
    let transport: Arc<dyn Transport> = Arc::new(net.clone());
    let mut control = ClusterControl::new(
        NodeId(id),
        registry.clone(),
        Arc::clone(&directory),
        transport,
    );
    if let Some(state) = state {
        control = control.with_cluster_state(state);
    }
    let control = Arc::new(control);
    if register_worker {
        control.register_type::<Worker>();
    }
    net.attach(NodeId(id), control.clone());
    Node {
        control,
        registry,
        directory,
    }
}

/// A forwarded scale request for `Worker` targeting `total` instances, carrying
/// `req_live` as the requester's observed voter set.
fn worker_scale_request(name: &str, total: u64, req_live: &[u64]) -> ScaleRequest {
    ScaleRequest {
        name: name.to_string(),
        actor_type: ClusterControl::type_id::<Worker>(),
        total,
        config: craft_proto::encode(&7u32).unwrap(),
        live_nodes: nodes(req_live),
    }
}

#[tokio::test]
async fn spawn_remote_on_self_spawns_locally() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1, true);

    let id = n1
        .control
        .spawn_remote::<Worker>(NodeId(1), "w", 7)
        .await
        .unwrap();
    assert_eq!(id.node, NodeId(1));
    assert!(n1.registry.contains("w"));
}

#[tokio::test]
async fn spawn_remote_starts_the_actor_on_the_target_node() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1, true);
    let n2 = node(&net, 2, true);

    let id = n1
        .control
        .spawn_remote::<Worker>(NodeId(2), "w", 42)
        .await
        .unwrap();

    assert_eq!(id.node, NodeId(2));
    assert!(
        n2.registry.contains("w"),
        "actor started on the target node"
    );
    assert!(!n1.registry.contains("w"), "not on the caller");
}

#[tokio::test]
async fn spawn_remote_for_an_unregistered_type_is_rejected() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1, true);
    let _n2 = node(&net, 2, false); // node 2 never registered Worker

    let err = n1
        .control
        .spawn_remote::<Worker>(NodeId(2), "w", 1)
        .await
        .unwrap_err();
    match err {
        RemoteSpawnError::Remote(RemoteError::Rejected { node, reason }) => {
            assert_eq!(node, NodeId(2));
            assert!(reason.contains("factory"), "surfaced reason: {reason}");
        }
        other => panic!("expected Remote rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn spawn_remote_of_a_local_only_actor_fails_to_encode() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1, true);
    let _n2 = node(&net, 2, true);

    let err = n1
        .control
        .spawn_remote::<LocalOnly>(NodeId(2), "local", ())
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        RemoteSpawnError::Config(ConfigCodecError::NotSpawnable)
    ));
}

#[tokio::test]
async fn scale_cluster_places_one_worker_on_each_live_node() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1, true);
    let n2 = node(&net, 2, true);

    // Fresh cluster (directory empty) → plan spawns on both nodes.
    let plan = n1
        .control
        .scale_cluster::<Worker>("w", 2, 1, &nodes(&[1, 2]))
        .await
        .unwrap();

    assert_eq!(plan.spawns, nodes(&[1, 2]));
    assert!(plan.removes.is_empty());
    assert!(n1.registry.contains("w"), "spawned locally");
    assert!(n2.registry.contains("w"), "spawned remotely");
}

#[tokio::test]
async fn scale_cluster_rejects_more_workers_than_nodes() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1, true);

    let err = n1
        .control
        .scale_cluster::<Worker>("w", 3, 1, &nodes(&[1, 2]))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        ClusterScaleError::Plan(ScaleError::InsufficientNodes { total: 3, nodes: 2 })
    ));
}

#[tokio::test]
async fn scale_cluster_stops_the_local_instance_when_this_node_is_demoted() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1, true);
    let n2 = node(&net, 2, true);

    // Node 1 currently hosts the worker (reflected in its directory), but the
    // live membership no longer includes it — placement must move to node 2.
    n1.registry.spawn::<Worker>("w", 0).unwrap();
    n1.directory
        .apply(&craft_actor::craft_proto::DirectoryUpdate {
            node: NodeId(1),
            epoch: 1,
            registrations: vec![reg(1, "w", 0)],
        });

    let plan = n1
        .control
        .scale_cluster::<Worker>("w", 1, 1, &nodes(&[2]))
        .await
        .unwrap();

    assert_eq!(plan.spawns, nodes(&[2]), "placed on the live node");
    assert_eq!(plan.removes, vec![reg(1, "w", 0).id]);
    assert!(!n1.registry.contains("w"), "local instance stopped");
    assert!(n2.registry.contains("w"), "moved to the live node");
}

#[tokio::test]
async fn handle_stop_removes_the_group_and_is_idempotent() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1, true);
    n1.registry.spawn::<Worker>("w", 0).unwrap();
    assert!(n1.registry.contains("w"));

    let first = n1.control.handle_stop(&StopRequest {
        name: "w".to_string(),
    });
    assert!(first.error.is_none());
    assert!(!n1.registry.contains("w"), "the group was stopped");

    // Stopping an already-absent group is still a success (it is already gone).
    let second = n1.control.handle_stop(&StopRequest {
        name: "w".to_string(),
    });
    assert!(second.error.is_none(), "idempotent: {:?}", second.error);
}

#[tokio::test]
async fn scale_down_stops_the_instance_on_a_remote_node() {
    let net = LocalNetwork::new();
    let n1 = node(&net, 1, true);
    let n2 = node(&net, 2, true);

    // Both nodes host the worker; n1's directory reflects the cluster-wide view.
    n1.registry.spawn::<Worker>("w", 0).unwrap();
    n2.registry.spawn::<Worker>("w", 0).unwrap();
    n1.directory
        .apply(&craft_actor::craft_proto::DirectoryUpdate {
            node: NodeId(1),
            epoch: 1,
            registrations: vec![reg(1, "w", 0)],
        });
    n1.directory
        .apply(&craft_actor::craft_proto::DirectoryUpdate {
            node: NodeId(2),
            epoch: 1,
            registrations: vec![reg(2, "w", 0)],
        });

    // Scale down to one: node 1 is kept, node 2's instance is removed — over the
    // wire via `/actor/stop`, not silently dropped.
    let plan = n1
        .control
        .scale_cluster::<Worker>("w", 1, 1, &nodes(&[1, 2]))
        .await
        .unwrap();

    assert_eq!(plan.removes, vec![reg(2, "w", 0).id]);
    assert!(n1.registry.contains("w"), "kept on the surviving node");
    assert!(
        !n2.registry.contains("w"),
        "the remote instance was stopped via /actor/stop"
    );
}

#[tokio::test]
async fn handle_scale_on_a_deposed_node_is_rejected_without_placing() {
    let net = LocalNetwork::new();
    // Node 1 received a forwarded scale but is no longer the leader.
    let n1 = node_with_state(
        &net,
        1,
        true,
        MockState {
            leader: false,
            live: nodes(&[1, 2]),
        },
    );
    let n2 = node(&net, 2, true);

    let reply = n1
        .control
        .handle_scale(&worker_scale_request("w", 2, &[1, 2]))
        .await;

    assert_eq!(
        reply.error.as_deref(),
        Some("not leader"),
        "a deposed node must refuse forwarded placement (supervisor-leader)"
    );
    assert!(!n1.registry.contains("w"), "no local placement");
    assert!(!n2.registry.contains("w"), "no remote placement");
}

#[tokio::test]
async fn handle_scale_uses_the_leaders_own_voters_not_the_request() {
    let net = LocalNetwork::new();
    // Leader sees both nodes live; the requester's set lagged a ConfChange and
    // only lists node 1. Planning against the request would fail (total 2 > 1
    // node); planning against the leader's own voters succeeds on both.
    let n1 = node_with_state(
        &net,
        1,
        true,
        MockState {
            leader: true,
            live: nodes(&[1, 2]),
        },
    );
    let n2 = node(&net, 2, true);

    let reply = n1
        .control
        .handle_scale(&worker_scale_request("w", 2, &[1]))
        .await;

    assert_eq!(reply.error, None, "planned against the leader's voters");
    assert!(n1.registry.contains("w"), "spawned on the leader");
    assert!(
        n2.registry.contains("w"),
        "spawned on the peer the request omitted"
    );
}
