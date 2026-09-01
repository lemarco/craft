//! JSON snapshot types and the [`Observer`] port the admin server reads from
//! (health-admin-port readiness, observability §4 introspection).
//!
//! The dashboard crate does not depend on the concrete runtime; instead the
//! facade/runtime implements [`Observer`], supplying point-in-time snapshots
//! that the admin HTTP server renders as JSON. This keeps observability
//! decoupled and lets tests drive the endpoints with a fake observer.

use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};

/// A boxed, `Send` future — object-safe return type for [`Observer`].
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Readiness snapshot for `GET /ready` (health-admin-port). `200` iff
/// [`is_ready`](Readiness::is_ready).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Readiness {
    /// This node's id.
    pub node_id: u64,
    /// Current role (`leader`/`follower`/`candidate`/…).
    pub role: String,
    /// Whether the node is a member of the current Raft configuration.
    pub member: bool,
    /// Whether the node is draining/leaving (drain-timeout).
    pub draining: bool,
    /// Auto-spawned workers currently hosted (auto-spawn-on-join).
    pub workers: Vec<String>,
    /// Human-readable reason when not ready (e.g. `"joining"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl Readiness {
    /// A node is ready when it is a cluster member and not draining.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.member && !self.draining
    }
}

/// One node's summary within a [`ClusterView`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSummary {
    /// Node id.
    pub id: u64,
    /// Role as seen by the responder.
    pub role: String,
    /// Whether it is a voting member.
    pub member: bool,
}

/// Cluster-wide view for `GET /introspect/cluster` (observability §4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterView {
    /// Current best-known leader.
    pub leader: Option<u64>,
    /// Current term.
    pub term: u64,
    /// Highest committed index.
    pub commit_index: u64,
    /// Known nodes and their roles.
    pub nodes: Vec<NodeSummary>,
}

/// One actor's introspection record (observability §4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActorView {
    /// Actor identity (registry key / instance id).
    pub id: String,
    /// Node currently hosting the actor.
    pub node: u64,
    /// User actor type name.
    pub actor_type: String,
    /// Current mailbox depth.
    pub mailbox_depth: u64,
    /// Uptime in seconds since (re)spawn.
    pub uptime_secs: u64,
    /// Restart/migration generation.
    pub generation: u32,
    /// Handled messages per second (group rate on the hosting node; `0` when unknown).
    pub messages_per_sec: f64,
}

/// Per-node view for `GET /introspect/node/{id}` (observability §4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeView {
    /// Node id.
    pub id: u64,
    /// Worker pools hosted here.
    pub workers: Vec<String>,
    /// Logical CPU/parallelism available (cross-node-actors resources).
    pub cpus: u32,
    /// Whether the external actor-state store is reachable (actor-state-redis).
    pub store_healthy: bool,
}

/// One multi-Raft group's consensus snapshot (Tier 1 observability).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftGroupSummary {
    /// Raft group id (shard coordinator index).
    pub group_id: u32,
    /// Role on this node for the group.
    pub role: String,
    /// Current leader node id, if known.
    pub leader: Option<u64>,
    /// Current term.
    pub term: u64,
    /// Highest committed index.
    pub commit_index: u64,
    /// Voting members.
    pub voters: Vec<u64>,
    /// Learner members (non-voting replicas).
    pub learners: Vec<u64>,
    /// Whether this node hosts the group's Raft runtime.
    pub hosted_on_this_node: bool,
}

/// Multi-Raft routing and per-group status for `GET /introspect/raft-groups`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftGroupsView {
    /// Active virtual shard count (may grow via expansion).
    pub shard_count: u32,
    /// Keyed routing mode: `modulus` (Tier 1) or `stable_virtual` (Tier 2).
    pub shard_routing: String,
    /// Number of catalogued Raft groups.
    pub catalog_size: u32,
    /// Monotonic catalog generation (bumps on each committed expansion).
    pub catalog_version: u32,
    /// Target voter replication factor per group.
    pub replication_factor: u32,
    /// Target learner replicas per group.
    pub learner_factor: u32,
    /// Group ids hosted on this node.
    pub hosted_groups: Vec<u32>,
    /// Per-group snapshots for groups hosted here.
    pub groups: Vec<RaftGroupSummary>,
}

/// One job stream's depth gauges for `GET /introspect/queues`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStreamView {
    /// Stream name (e.g. `jobs`).
    pub stream: String,
    /// Jobs eligible to lease now.
    pub pending: u64,
    /// Jobs currently leased.
    pub leased: u64,
    /// Jobs in dead letter.
    pub dead_letter: u64,
    /// Age of the oldest ready pending job in milliseconds.
    pub oldest_pending_age_ms: u64,
    /// Jobs that have already failed an attempt and will be delivered again.
    ///
    /// Non-zero means handlers on this stream are being re-run — they must be
    /// idempotent ([background-jobs](../../../docs/scenarios/background-jobs.md#delivery-semantics)).
    #[serde(default)]
    pub redelivered: u64,
}

/// All registered job streams on this node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuesView {
    /// Per-stream queue depth.
    pub streams: Vec<QueueStreamView>,
}

/// One saga journal record for `GET /introspect/sagas`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaRecordView {
    /// Saga id (hex-encoded bytes).
    pub saga_id: String,
    /// Latest phase (`running`, `completed`, `compensating`, `compensated`, `stuck`).
    pub phase: String,
    /// Forward steps committed so far.
    pub completed_steps: u32,
    /// Catalog version pinned at start, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog_version: Option<u32>,
    /// Forward step that failed before compensation (if any).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_step: Option<u32>,
    /// Compensate step index that failed when phase is `stuck`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compensate_failed_at: Option<u32>,
}

/// Read-only observability port implemented by the runtime/facade.
///
/// Object-safe (boxed futures) so the admin server can hold
/// `Arc<dyn Observer>` independent of the concrete `StateMachine`.
pub trait Observer: Send + Sync + 'static {
    /// Current readiness snapshot (health-admin-port).
    fn readiness(&self) -> BoxFuture<'_, Readiness>;

    /// Cluster-wide consensus/membership view.
    fn cluster(&self) -> BoxFuture<'_, ClusterView>;

    /// Multi-Raft shard routing and per-group status on this node.
    fn raft_groups(&self) -> BoxFuture<'_, RaftGroupsView>;

    /// All actors known to this node (cluster-wide when served by the leader).
    fn actors(&self) -> BoxFuture<'_, Vec<ActorView>>;

    /// A single actor by id, if present.
    fn actor(&self, id: &str) -> BoxFuture<'_, Option<ActorView>>;

    /// A single node's detail by id, if known.
    fn node(&self, id: u64) -> BoxFuture<'_, Option<NodeView>>;

    /// Registered job streams and depth gauges (background-jobs observability).
    fn queues(&self) -> BoxFuture<'_, QueuesView>;

    /// Saga journal records known on this node (workflow observability).
    fn sagas(&self) -> BoxFuture<'_, Vec<SagaRecordView>>;
}
