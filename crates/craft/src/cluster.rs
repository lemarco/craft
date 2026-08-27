//! [`CraftCluster`] — the running node handle returned by the builder.
//!
//! It bundles everything the facade wired together: the consensus/actor runtime
//! (via an in-process [`NodeHandle`] for zero-copy L1 clients), the actor
//! control/messaging/directory planes, the leader-only supervisor, and the
//! telemetry [`EventBus`] + [`Metrics`]. Background tasks (facts refresh,
//! directory anti-entropy, supervisor reconcile, admin server) run until
//! [`shutdown`](CraftCluster::shutdown) or the handle is dropped.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use craft_core::StateMachine;
use craft_dashboard::{CraftEvent, EventBus, Metrics, StopReason, TraceOpts};
use craft_net::RemoteError;
use craft_net::transport::RequestHandler;
use craft_net::{Transport, send_catalog_add_request, send_leave_request};
use craft_proto::{
    CatalogAddRequest, CatalogAddResponse, CatalogRejection, LeaveRejection, LeaveRequest,
    LeaveResponse, Membership, NodeId, PROTOCOL_VERSION, ScaleRequest,
};
use tokio::task::JoinHandle;

use craft_actor::{
    ActorDirectory, ActorObserver, ActorRegistry, ClusterControl, ClusterMessaging,
    ClusterScaleError, ClusterState, ClusterSupervisor, DirectorySync, NOT_LEADER_REASON,
    NodeHandle, NodeStatus, ResourceProfile, UserActor, VpsResources,
};

use crate::multi_raft::MultiRaftState;

use crate::CraftClusterBuilder;
use crate::certs::CertReloadHandle;

/// The live leadership/membership facts the supervisor reconciles against
/// (implements [`ClusterState`]), refreshed from the node's consensus status by
/// a background task. Exposed only so [`CraftCluster::supervisor`] has a nameable
/// type; you rarely construct or read it directly.
#[derive(Default)]
pub struct ClusterFacts {
    leader: AtomicBool,
    voters: Mutex<Vec<NodeId>>,
    reachable: Mutex<Vec<NodeId>>,
}

impl ClusterFacts {
    pub(crate) fn update(&self, status: &NodeStatus) {
        self.leader.store(
            matches!(status.role, craft_core::Role::Leader),
            Ordering::SeqCst,
        );
        *self.voters.lock().unwrap() = status.voters.clone();
        *self.reachable.lock().unwrap() = status.reachable.clone();
    }
}

impl ClusterState for ClusterFacts {
    fn is_leader(&self) -> bool {
        self.leader.load(Ordering::SeqCst)
    }

    fn live_nodes(&self) -> Vec<NodeId> {
        self.voters.lock().unwrap().clone()
    }

    fn reachable_nodes(&self) -> Vec<NodeId> {
        self.reachable.lock().unwrap().clone()
    }
}

/// Bridges the actor registry's lifecycle + per-message hooks (E14 / Track H)
/// to the telemetry planes (observability): spawns, stops, restarts, and escalations
/// emit [`CraftEvent`]s and bump counters, and — when opt-in tracing is enabled
/// for an actor via [`CraftCluster::trace`] — each handled message emits a
/// [`CraftEvent::MessageHandled`]. The registry owns no telemetry types, so the
/// facade installs this observer at build time (before any actor spawns).
///
/// Cumulative message rate / handle latency / mailbox depth are *not* pushed per
/// message here (that would serialize every actor on the metrics lock); instead
/// they are sampled periodically from the registry counters by the metrics
/// sampler background loop.
pub(crate) struct ActorTelemetry {
    node_id: NodeId,
    events: EventBus,
    metrics: Metrics,
    /// Fast gate so the per-message hook can early-return when nothing is being
    /// traced (the common case) without locking `traces`.
    tracing: AtomicBool,
    /// Per-actor-group trace expiry (auto-expires so tracing never runs forever).
    traces: Mutex<HashMap<String, Instant>>,
}

impl ActorTelemetry {
    pub(crate) fn new(node_id: NodeId, events: EventBus, metrics: Metrics) -> Self {
        Self {
            node_id,
            events,
            metrics,
            tracing: AtomicBool::new(false),
            traces: Mutex::new(HashMap::new()),
        }
    }

    /// A stable, human-readable actor id for events, e.g. `worker#0@n3`.
    fn id(&self, name: &str, instance: u32) -> String {
        format!("{name}#{instance}@n{}", self.node_id.0)
    }

    /// Enable per-message tracing for group `name` until `opts.duration` elapses.
    pub(crate) fn enable_trace(&self, name: &str, opts: &TraceOpts) {
        if !opts.messages {
            return;
        }
        let mut traces = self.traces.lock().unwrap();
        traces.insert(name.to_string(), Instant::now() + opts.duration);
        self.tracing.store(true, Ordering::Relaxed);
    }

    /// Whether group `name` is currently being traced, pruning any expired entry.
    fn is_traced(&self, name: &str) -> bool {
        let now = Instant::now();
        let mut traces = self.traces.lock().unwrap();
        match traces.get(name) {
            Some(expiry) if now < *expiry => true,
            Some(_) => {
                traces.remove(name);
                if traces.is_empty() {
                    self.tracing.store(false, Ordering::Relaxed);
                }
                false
            }
            None => false,
        }
    }
}

impl ActorObserver for ActorTelemetry {
    fn on_spawned(&self, name: &str, instance: u32) {
        self.metrics.incr(
            "craft_actor_spawns_total",
            "Cumulative actor instances spawned.",
            &[("actor", name)],
            1.0,
        );
        self.events.emit(CraftEvent::ActorSpawned {
            id: self.id(name, instance),
        });
    }

    fn on_stopped(&self, name: &str, instance: u32) {
        self.metrics.incr(
            "craft_actor_stops_total",
            "Cumulative actor instances stopped normally.",
            &[("actor", name)],
            1.0,
        );
        self.events.emit(CraftEvent::ActorStopped {
            id: self.id(name, instance),
            reason: StopReason::Normal,
        });
    }

    fn on_message_handled(&self, name: &str, instance: u32, elapsed: std::time::Duration) {
        if !self.tracing.load(Ordering::Relaxed) || !self.is_traced(name) {
            return;
        }
        self.events.emit(CraftEvent::MessageHandled {
            id: self.id(name, instance),
            latency_ms: elapsed.as_millis() as u64,
        });
    }

    fn on_restart(&self, name: &str, instance: u32, count: u32) {
        self.metrics.incr(
            "craft_actor_restarts_total",
            "Cumulative supervised actor restarts.",
            &[("actor", name)],
            1.0,
        );
        self.events.emit(CraftEvent::ActorRestarted {
            id: self.id(name, instance),
            count,
        });
    }

    fn on_escalated(&self, name: &str, instance: u32) {
        self.metrics.incr(
            "craft_actor_escalations_total",
            "Supervised actors that exhausted their restart budget and stopped.",
            &[("actor", name)],
            1.0,
        );
        self.events.emit(CraftEvent::ActorStopped {
            id: self.id(name, instance),
            reason: StopReason::RestartLimit,
        });
    }
}

/// Turns consensus-status transitions between facts-refresher ticks into
/// gauges/counters + lifecycle events (Track H), and reports the membership
/// delta so the caller can prune routing and trigger reconcile (E11/E12).
pub(crate) struct MembershipTelemetry {
    node_label: String,
    events: EventBus,
    metrics: Metrics,
    prev: Option<NodeStatus>,
}

/// What changed in the committed voter set and reachability since the previous
/// status tick.
pub(crate) struct StatusDelta {
    /// Voters present last tick but gone now (crash / leave).
    pub departed: Vec<NodeId>,
    /// Voters that dropped out of the reachable set but remain committed members
    /// (crash / partition without a `ConfChange`, liveness-vs-membership).
    pub unreachable: Vec<NodeId>,
    /// Whether the committed voter set changed at all (join or leave).
    pub membership_changed: bool,
    /// Whether the heartbeat-derived reachable set changed (crash, heal, or
    /// partition).
    pub reachability_changed: bool,
}

impl MembershipTelemetry {
    pub(crate) fn new(node_id: NodeId, events: EventBus, metrics: Metrics) -> Self {
        Self {
            node_label: node_id.0.to_string(),
            events,
            metrics,
            prev: None,
        }
    }

    /// Record a fresh status: publish consensus gauges, emit leader/membership
    /// change events, and return the membership delta vs the previous tick.
    pub(crate) fn record(&mut self, status: &NodeStatus) -> StatusDelta {
        let node = self.node_label.as_str();
        self.metrics.set(
            "craft_raft_term",
            "Current Raft term.",
            &[("node", node)],
            status.term.0 as f64,
        );
        self.metrics.set(
            "craft_raft_commit_index",
            "Highest committed log index.",
            &[("node", node)],
            status.commit_index.0 as f64,
        );
        self.metrics.set(
            "craft_raft_last_applied",
            "Highest applied log index.",
            &[("node", node)],
            status.last_applied.0 as f64,
        );
        self.metrics.set(
            "craft_raft_live_nodes",
            "Committed voter count.",
            &[("node", node)],
            status.voters.len() as f64,
        );
        self.metrics.set(
            "craft_raft_is_leader",
            "1 when this node currently believes it is the Raft leader.",
            &[("node", node)],
            f64::from(u8::from(matches!(status.role, craft_core::Role::Leader))),
        );

        let leader_changed = self
            .prev
            .as_ref()
            .is_some_and(|p| p.leader != status.leader || p.term != status.term);
        if leader_changed && let Some(leader) = status.leader {
            self.metrics.incr(
                "craft_raft_leader_changes_total",
                "Observed leadership changes.",
                &[("node", node)],
                1.0,
            );
            self.events.emit(CraftEvent::LeaderChanged {
                term: status.term.0,
                leader: leader.0,
            });
        }

        let mut departed = Vec::new();
        let mut unreachable = Vec::new();
        let mut membership_changed = false;
        let mut reachability_changed = false;
        if let Some(prev) = &self.prev {
            use std::collections::BTreeSet;
            let prev_v: BTreeSet<NodeId> = prev.voters.iter().copied().collect();
            let new_v: BTreeSet<NodeId> = status.voters.iter().copied().collect();
            for joined in new_v.difference(&prev_v) {
                membership_changed = true;
                self.metrics.incr(
                    "craft_cluster_node_joins_total",
                    "Nodes observed joining the committed voter set.",
                    &[("node", node)],
                    1.0,
                );
                self.events
                    .emit(CraftEvent::NodeJoined { node_id: joined.0 });
            }
            for left in prev_v.difference(&new_v) {
                membership_changed = true;
                departed.push(*left);
                self.metrics.incr(
                    "craft_cluster_node_leaves_total",
                    "Nodes observed leaving the committed voter set.",
                    &[("node", node)],
                    1.0,
                );
                self.events.emit(CraftEvent::NodeLeft {
                    node_id: left.0,
                    // Observed via membership diff, not a coordinated drain, so
                    // report non-graceful (crash-safe default, E12).
                    graceful: false,
                });
            }

            let prev_r: BTreeSet<NodeId> = prev.reachable.iter().copied().collect();
            let new_r: BTreeSet<NodeId> = status.reachable.iter().copied().collect();
            if prev_r != new_r {
                reachability_changed = true;
                for lost in prev_r.difference(&new_r) {
                    // Still a committed voter — crash/partition, not a leave.
                    if new_v.contains(lost) {
                        unreachable.push(*lost);
                    }
                }
            }
        }
        self.prev = Some(status.clone());
        StatusDelta {
            departed,
            unreachable,
            membership_changed,
            reachability_changed,
        }
    }
}

/// A running craft node: the facade's single entry point once
/// [`start`](crate::CraftClusterBuilder::start_local) returns.
///
/// Clone-free but cheap to pass by reference; drop it (or call
/// [`shutdown`](Self::shutdown)) to stop the node.
pub struct CraftCluster<M: StateMachine> {
    pub(crate) node_id: NodeId,
    pub(crate) handle: NodeHandle<M>,
    pub(crate) group_handles: Vec<NodeHandle<M>>,
    pub(crate) raft_groups: u32,
    pub(crate) shard_count: u32,
    pub(crate) shard_routing: craft_core::ShardRoutingKind,
    pub(crate) registry: ActorRegistry,
    pub(crate) control: Arc<ClusterControl>,
    pub(crate) messaging: Arc<ClusterMessaging>,
    pub(crate) directory: Arc<ActorDirectory>,
    pub(crate) directory_sync: Arc<DirectorySync>,
    pub(crate) supervisor: Arc<ClusterSupervisor<Arc<ClusterFacts>>>,
    pub(crate) events: EventBus,
    pub(crate) metrics: Metrics,
    pub(crate) telemetry: Arc<ActorTelemetry>,
    pub(crate) members: Vec<NodeId>,
    pub(crate) resource_profile: ResourceProfile,
    pub(crate) vps_resources: VpsResources,
    pub(crate) actor_state_store: Option<Arc<dyn craft_actor::ActorStateStore>>,
    /// Full `/raft/v1/*` handler attached to the transport (stored so tests can
    /// re-attach a node after simulating partition on [`LocalNetwork`]).
    pub(crate) wire_handler: Arc<dyn RequestHandler>,
    pub(crate) transport: Arc<dyn Transport>,
    pub(crate) facts: Arc<ClusterFacts>,
    /// Live multi-Raft state when `raft_groups > 1` (handles move on rebalance).
    pub(crate) multi_raft: Option<Arc<MultiRaftState<M>>>,
    pub(crate) cert_reload: Option<Arc<CertReloadHandle>>,
    pub(crate) drain_timeout: Duration,
    pub(crate) tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl<M: StateMachine> CraftCluster<M> {
    /// Start describing a node running `machine`, identified by `node_id`. See
    /// [`CraftClusterBuilder`](crate::CraftClusterBuilder) for the options.
    #[must_use]
    pub fn builder(node_id: NodeId, machine: M) -> CraftClusterBuilder<M>
    where
        M: Default,
    {
        CraftClusterBuilder::new(node_id, machine)
    }

    /// This node's id.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// The configured cluster membership (initial voters).
    #[must_use]
    pub fn members(&self) -> &[NodeId] {
        &self.members
    }

    /// How much of this VPS the worker should use (one-worker-per-vps).
    #[must_use]
    pub fn resource_profile(&self) -> ResourceProfile {
        self.resource_profile
    }

    /// Detected VPS capacity for sizing the single worker's internal pools (one-worker-per-vps).
    #[must_use]
    pub fn vps_resources(&self) -> VpsResources {
        self.vps_resources
    }

    /// The workflow-state store wired by
    /// [`CraftClusterBuilder::actor_state_store`](crate::CraftClusterBuilder::actor_state_store),
    /// if any (actor-state-redis). Clone the `Arc` into actor `Config` when spawning
    /// stateful workers.
    #[must_use]
    pub fn actor_state_store(&self) -> Option<Arc<dyn craft_actor::ActorStateStore>> {
        self.actor_state_store.clone()
    }

    /// The in-process client handle for Raft group 0 (single-group default).
    /// With multi-Raft, prefer [`group_handle`](Self::group_handle) — the
    /// bootstrap handle may be retired after rebalance.
    #[must_use]
    pub fn handle(&self) -> &NodeHandle<M> {
        &self.handle
    }

    /// Active handle for Raft group `group`, if this node currently hosts it.
    #[must_use]
    pub fn group_handle(&self, group: u32) -> Option<NodeHandle<M>> {
        if let Some(mr) = &self.multi_raft {
            return mr.handles.lock().unwrap().get(&group).cloned();
        }
        if group == 0 {
            Some(self.handle.clone())
        } else {
            self.group_handles.get(group as usize).cloned()
        }
    }

    /// Group ids with a live Raft runtime on this node (updates after rebalance).
    #[must_use]
    pub fn hosted_groups(&self) -> Vec<u32> {
        if let Some(mr) = &self.multi_raft {
            mr.handles.lock().unwrap().keys().copied().collect()
        } else {
            (0..self.raft_groups).collect()
        }
    }

    /// One [`NodeHandle`] per catalog group at bootstrap (may include retired
    /// groups after rebalance; use [`group_handle`](Self::group_handle) for the
    /// live set).
    #[must_use]
    pub fn group_handles(&self) -> &[NodeHandle<M>] {
        &self.group_handles
    }

    /// Number of Raft groups in the live catalog (1 = default single-group).
    #[must_use]
    pub fn raft_groups(&self) -> u32 {
        if let Some(mr) = &self.multi_raft {
            mr.catalog.lock().unwrap().len() as u32
        } else {
            self.raft_groups
        }
    }

    /// Live catalog length for multi-Raft clusters.
    #[must_use]
    pub fn catalog_len(&self) -> u32 {
        self.raft_groups()
    }

    /// Shard count used for keyed client routing when `raft_groups > 1`.
    #[must_use]
    pub fn shard_count(&self) -> u32 {
        if let Some(mr) = &self.multi_raft {
            mr.sharded.shard_count()
        } else {
            self.shard_count
        }
    }

    /// Keyed routing mode for multi-Raft clusters.
    #[must_use]
    pub fn shard_routing(&self) -> craft_core::ShardRoutingKind {
        if let Some(mr) = &self.multi_raft {
            mr.sharded.routing_kind()
        } else {
            self.shard_routing
        }
    }

    /// Expand the virtual shard keyspace (Tier 1 modulus only). Keys **remap** when
    /// the modulus changes — drain keyed clients before calling.
    ///
    /// # Errors
    /// Returns [`craft_core::ShardExpansionError::NotMultiRaft`] on single-Raft
    /// clusters, [`craft_core::ShardExpansionError::StableRoutingActive`] when stable
    /// routing is configured, or planner errors when `new_count` is invalid.
    pub fn expand_shard_count(
        &self,
        new_count: u32,
    ) -> Result<craft_core::ShardCountExpansionPlan, craft_core::ShardExpansionError> {
        let mr = self
            .multi_raft
            .as_ref()
            .ok_or(craft_core::ShardExpansionError::NotMultiRaft)?;
        mr.sharded.expand_shard_count(new_count)
    }

    /// Grow the active virtual shard prefix (Tier 2 stable routing). Existing keyed
    /// traffic keeps the same virtual shard id.
    ///
    /// # Errors
    /// Returns [`craft_core::StableShardActivationError::NotMultiRaft`] on single-Raft
    /// clusters, [`craft_core::StableShardActivationError::ModulusRoutingActive`] when
    /// modulus routing is configured, or planner errors when `new_active` is invalid.
    pub fn activate_shards(
        &self,
        new_active: u32,
    ) -> Result<craft_core::StableShardActivationPlan, craft_core::StableShardActivationError> {
        let mr = self
            .multi_raft
            .as_ref()
            .ok_or(craft_core::StableShardActivationError::NotMultiRaft)?;
        mr.sharded.activate_shards(new_active)
    }

    /// The node-local actor registry (spawn / scale / drain local actors).
    #[must_use]
    pub fn registry(&self) -> &ActorRegistry {
        &self.registry
    }

    /// The cluster control plane (remote spawn, cluster-wide scale, migration).
    #[must_use]
    pub fn control(&self) -> &Arc<ClusterControl> {
        &self.control
    }

    /// Cross-node actor messaging (round-robin and keyed casts).
    #[must_use]
    pub fn messaging(&self) -> &Arc<ClusterMessaging> {
        &self.messaging
    }

    /// The cluster-wide actor directory.
    #[must_use]
    pub fn directory(&self) -> &Arc<ActorDirectory> {
        &self.directory
    }

    /// The leader-only supervisor driving managed / auto-worker groups.
    #[must_use]
    pub fn supervisor(&self) -> &Arc<ClusterSupervisor<Arc<ClusterFacts>>> {
        &self.supervisor
    }

    /// The telemetry event bus; subscribe with [`EventBus::subscribe`].
    #[must_use]
    pub fn events(&self) -> &EventBus {
        &self.events
    }

    /// The Prometheus metrics registry served on the admin port.
    #[must_use]
    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Enable opt-in per-message tracing (observability §7, H6) for the local actor
    /// group `name`. While active, every message that group handles emits a
    /// [`CraftEvent::MessageHandled`] (with handling latency) onto
    /// [`events`](Self::events). Tracing auto-expires after `opts.duration`, so
    /// it never runs unbounded. Off by default — steady-state message flow does
    /// not emit per-message events, only the sampled rate/latency metrics.
    pub fn trace(&self, name: &str, opts: TraceOpts) {
        self.telemetry.enable_trace(name, &opts);
    }

    /// A point-in-time consensus status snapshot, or `None` if the runtime has
    /// stopped.
    pub async fn status(&self) -> Option<NodeStatus> {
        self.handle.status().await
    }

    /// The node's wire handler (re-attach after simulated partition in tests).
    #[doc(hidden)]
    pub fn wire_handler(&self) -> Arc<dyn RequestHandler> {
        Arc::clone(&self.wire_handler)
    }

    /// Whether this node currently believes it is the Raft leader.
    pub async fn is_leader(&self) -> bool {
        let Some(handle) = self.group_handle(0) else {
            return false;
        };
        handle
            .status()
            .await
            .is_some_and(|s| matches!(s.role, craft_core::Role::Leader))
    }

    /// Request removal of this node from the cluster registry (group 0). Contact
    /// any live member; followers transparently forward to the leader (symmetric
    /// to dynamic join). On [`LeaveResponse::Accepted`], per-group membership sync
    /// removes this node from shard groups (per-group-raft-membership).
    ///
    /// # Errors
    /// Returns a transport error when `contact` is unreachable or the wire
    /// framing fails.
    pub async fn request_leave(
        &self,
        transport: &dyn Transport,
        contact: NodeId,
    ) -> Result<LeaveResponse, craft_net::TransportError> {
        send_leave_request(
            transport,
            contact,
            &LeaveRequest {
                protocol_version: PROTOCOL_VERSION,
                node_id: self.node_id,
            },
        )
        .await
    }

    /// Graceful self-removal from the cluster registry (group 0) via
    /// [`request_leave`](Self::request_leave), retrying live peers on this
    /// node's transport until the membership change commits or the deadline
    /// expires. Per-group sync removes this node from shard groups on the
    /// facts tick (per-group-raft-membership ADR).
    ///
    /// Callers hosting actors should drain or migrate workers first
    /// (cross-node-actors); this method only removes Raft membership.
    ///
    /// # Errors
    /// Returns [`LeaveError`] when no peer is reachable, the cluster refuses
    /// the leave, or the retry budget is exhausted.
    pub async fn leave(&self) -> Result<Membership, LeaveError> {
        let deadline = Instant::now() + LEAVE_TIMEOUT;
        let contacts = self.leave_contacts().await;
        if contacts.is_empty() {
            return Err(LeaveError::NoContact);
        }
        loop {
            for &contact in &contacts {
                match self.leave_via_contact(contact).await {
                    Ok(membership) => {
                        self.events.emit(CraftEvent::NodeLeft {
                            node_id: self.node_id.0,
                            graceful: true,
                        });
                        return Ok(membership);
                    }
                    Err(LeaveError::Rejected(LeaveRejection::NotMember)) => {
                        if let Some(status) = self.status().await {
                            return Ok(Membership {
                                voters: status.voters,
                                voters_outgoing: Vec::new(),
                                learners: Vec::new(),
                            });
                        }
                        return Err(LeaveError::Rejected(LeaveRejection::NotMember));
                    }
                    Err(LeaveError::Rejected(reason)) => {
                        return Err(LeaveError::Rejected(reason));
                    }
                    Err(LeaveError::Transport(_)) => {}
                    Err(e) => return Err(e),
                }
            }
            if Instant::now() >= deadline {
                return Err(LeaveError::Timeout);
            }
            tokio::time::sleep(LEAVE_RETRY).await;
        }
    }

    async fn leave_contacts(&self) -> Vec<NodeId> {
        if let Some(status) = self.status().await {
            status
                .voters
                .iter()
                .copied()
                .filter(|id| *id != self.node_id)
                .collect()
        } else {
            self.members
                .iter()
                .copied()
                .filter(|id| *id != self.node_id)
                .collect()
        }
    }

    async fn leave_via_contact(&self, contact: NodeId) -> Result<Membership, LeaveError> {
        let first = self
            .request_leave(self.transport.as_ref(), contact)
            .await
            .map_err(LeaveError::Transport)?;
        match first {
            LeaveResponse::Accepted { membership, .. } => Ok(membership),
            LeaveResponse::Redirect {
                leader: Some(leader),
            } if leader != contact => {
                let second = self
                    .request_leave(self.transport.as_ref(), leader)
                    .await
                    .map_err(LeaveError::Transport)?;
                match second {
                    LeaveResponse::Accepted { membership, .. } => Ok(membership),
                    LeaveResponse::Rejected { reason } => Err(LeaveError::Rejected(reason)),
                    _ => Err(LeaveError::Timeout),
                }
            }
            LeaveResponse::Rejected { reason } => Err(LeaveError::Rejected(reason)),
            _ => Err(LeaveError::Timeout),
        }
    }

    /// Grow the multi-Raft group catalog without restarting nodes (Tier 2).
    pub async fn add_raft_groups(&self, count: u32) -> Result<Vec<u32>, AddRaftGroupsError> {
        if self.multi_raft.is_none() {
            return Err(AddRaftGroupsError::NotMultiRaft);
        }
        if count == 0 {
            return Err(AddRaftGroupsError::InvalidCount);
        }
        let deadline = Instant::now() + CATALOG_ADD_TIMEOUT;
        loop {
            let status = self.status().await.ok_or(AddRaftGroupsError::Stopped)?;
            if matches!(status.role, craft_core::Role::Leader) {
                return self
                    .catalog_add_local(count)
                    .await
                    .map_err(AddRaftGroupsError::from);
            }
            let Some(leader) = status.leader else {
                if Instant::now() >= deadline {
                    return Err(AddRaftGroupsError::NoLeader);
                }
                tokio::time::sleep(CATALOG_ADD_RETRY).await;
                continue;
            };
            let request = CatalogAddRequest {
                protocol_version: PROTOCOL_VERSION,
                add_groups: count,
            };
            let response = send_catalog_add_request(&*self.transport, leader, &request)
                .await
                .map_err(AddRaftGroupsError::Transport)?;
            match response {
                CatalogAddResponse::Accepted { new_groups, .. } => return Ok(new_groups),
                CatalogAddResponse::Redirect { .. } if Instant::now() >= deadline => {
                    return Err(AddRaftGroupsError::NoLeader);
                }
                CatalogAddResponse::Rejected { reason } => {
                    return Err(AddRaftGroupsError::Rejected(reason));
                }
                CatalogAddResponse::Redirect { .. } => {
                    tokio::time::sleep(CATALOG_ADD_RETRY).await;
                }
            }
        }
    }

    async fn catalog_add_local(&self, count: u32) -> Result<Vec<u32>, CatalogAddLocalError> {
        let handle = self
            .group_handle(0)
            .ok_or(CatalogAddLocalError::NoGroup0Handle)?;
        let response = handle
            .catalog_add(CatalogAddRequest {
                protocol_version: PROTOCOL_VERSION,
                add_groups: count,
            })
            .await
            .map_err(|_| CatalogAddLocalError::Stopped)?;
        match response {
            CatalogAddResponse::Accepted { new_groups, .. } => Ok(new_groups),
            CatalogAddResponse::Redirect { leader } => {
                Err(CatalogAddLocalError::NotLeader { leader })
            }
            CatalogAddResponse::Rejected { reason } => Err(CatalogAddLocalError::Rejected(reason)),
        }
    }

    /// Hot-reload handle when the node was started with [`CraftClusterBuilder::start_quic_pem`]
    /// (cert-automation). `None` for in-memory or static `Security` starts.
    #[must_use]
    pub fn cert_reload(&self) -> Option<&CertReloadHandle> {
        self.cert_reload.as_deref()
    }

    /// Drive actor group `name` to `total` instances cluster-wide (one worker
    /// per node, one-worker-per-vps). Cluster-wide placement is the **leader's** job, so
    /// this transparently forwards to the leader when called on a follower
    /// (`/actor/scale`, supervisor-leader) — mirroring how client writes are forwarded
    /// (client-routing). On the leader it plans and executes directly against the
    /// current voter set.
    ///
    /// Every node that may host the group must have registered the type via
    /// [`ClusterControl::register_type`] (as for any remote spawn).
    ///
    /// # Errors
    /// Returns [`ScaleClusterError`] if the runtime has stopped, no leader is
    /// elected, the config cannot be encoded, forwarding fails, or the leader
    /// rejects the scale (e.g. too few nodes).
    pub async fn scale_cluster<A: UserActor>(
        &self,
        name: &str,
        total: usize,
        config: A::Config,
    ) -> Result<(), ScaleClusterError>
    where
        A::Config: Clone,
    {
        let encoded =
            A::encode_config(&config).map_err(|e| ScaleClusterError::Config(e.to_string()))?;
        // Leadership may still be settling (just elected / handed off): the live
        // status can name a leader whose own facts have not yet caught up, so a
        // forwarded scale can be transiently refused with `NOT_LEADER_REASON`.
        // Re-resolve and retry within a bounded deadline rather than surfacing a
        // spurious failure — a real placement error returns immediately.
        let deadline = Instant::now() + SCALE_FORWARD_TIMEOUT;
        loop {
            let status = self.status().await.ok_or(ScaleClusterError::Stopped)?;
            if matches!(status.role, craft_core::Role::Leader) {
                self.control
                    .scale_cluster::<A>(name, total, config.clone(), &status.voters)
                    .await?;
                return Ok(());
            }
            let Some(leader) = status.leader else {
                // No leader yet — keep waiting for one to emerge if there's time.
                if Instant::now() < deadline {
                    tokio::time::sleep(SCALE_FORWARD_RETRY).await;
                    continue;
                }
                return Err(ScaleClusterError::NoLeader);
            };
            let request = ScaleRequest {
                name: name.to_string(),
                actor_type: ClusterControl::type_id::<A>(),
                total: total as u64,
                config: encoded.clone(),
                live_nodes: status.voters.clone(),
            };
            let reply = self
                .control
                .request_scale(leader, &request)
                .await
                .map_err(|e| RemoteError::transport(leader, e))?;
            match reply.error {
                None => return Ok(()),
                Some(reason) if reason == NOT_LEADER_REASON && Instant::now() < deadline => {
                    tokio::time::sleep(SCALE_FORWARD_RETRY).await;
                }
                Some(reason) => return Err(RemoteError::rejected(leader, reason).into()),
            }
        }
    }

    /// Publish this node's local actor registrations to the rest of the cluster
    /// once, immediately (the runtime also does this periodically). Returns the
    /// number of peers that acknowledged.
    pub async fn publish_directory(&self) -> usize {
        let regs = self.registry.local_registrations(self.node_id);
        self.directory_sync.publish(&self.members, regs).await
    }

    /// Publish and wait until group `name` is visible locally (read-your-writes).
    /// Useful immediately after spawn/scale when
    /// [`DirectoryPolicy::ReadYourWrites`](craft_actor::DirectoryPolicy::ReadYourWrites) is enabled.
    pub async fn publish_directory_visible(
        &self,
        group: &str,
        min_instances: usize,
        timeout: Duration,
    ) -> bool {
        let _ = self.publish_directory().await;
        self.directory
            .wait_until(timeout, || {
                self.directory.has_at_least(group, min_instances)
            })
            .await
    }

    /// Cluster-wide default graceful-drain timeout ([drain-timeout]).
    #[must_use]
    pub fn drain_timeout(&self) -> Duration {
        self.drain_timeout
    }

    /// Per-group drain override on the local registry.
    pub fn set_group_drain_timeout(
        &self,
        name: &str,
        timeout: Option<Duration>,
    ) -> Result<(), craft_actor::StopError> {
        self.registry.set_group_drain_timeout(name, timeout)
    }

    /// Gracefully stop a local actor group using the cluster drain default.
    pub async fn stop_group_graceful(
        &self,
        name: &str,
    ) -> Result<craft_actor::DrainOutcome, craft_actor::StopError> {
        self.registry.stop_graceful(name, self.drain_timeout).await
    }

    /// Stop the node: shut the runtime down and abort all background tasks.
    pub fn shutdown(&self) {
        if let Some(mr) = &self.multi_raft {
            for handle in mr.handles.lock().unwrap().values() {
                handle.shutdown();
            }
        } else {
            for handle in &self.group_handles {
                handle.shutdown();
            }
        }
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
    }

    /// Stop the node and wait until every consensus runtime has exited so
    /// durable storage files can be reopened (for example on process restart).
    pub async fn shutdown_and_wait(&self) {
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
        if let Some(mr) = &self.multi_raft {
            let handles: Vec<_> = mr.handles.lock().unwrap().values().cloned().collect();
            for handle in handles {
                handle.shutdown_and_wait().await;
            }
        } else {
            for handle in &self.group_handles {
                handle.shutdown_and_wait().await;
            }
        }
    }
}

impl<M: StateMachine> Drop for CraftCluster<M> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// How long [`scale_cluster`](CraftCluster::scale_cluster) keeps re-resolving
/// the leader while a forwarded scale is transiently refused because leadership
/// is still settling. Comfortably exceeds the facts-refresh period so a scale
/// issued right after an election succeeds rather than failing spuriously.
const SCALE_FORWARD_TIMEOUT: Duration = Duration::from_secs(5);
/// Delay between forward retries within [`SCALE_FORWARD_TIMEOUT`].
const SCALE_FORWARD_RETRY: Duration = Duration::from_millis(25);
const CATALOG_ADD_TIMEOUT: Duration = Duration::from_secs(5);
const CATALOG_ADD_RETRY: Duration = Duration::from_millis(25);
/// Total budget for [`CraftCluster::leave`] peer retries.
const LEAVE_TIMEOUT: Duration = Duration::from_secs(30);
/// Delay between leave attempts within [`LEAVE_TIMEOUT`].
const LEAVE_RETRY: Duration = Duration::from_millis(50);

/// Why [`CraftCluster::leave`] failed.
#[derive(Debug, thiserror::Error)]
pub enum LeaveError {
    /// No other member is configured to contact.
    #[error("no peer to submit leave to")]
    NoContact,
    /// Retries against live peers were exhausted.
    #[error("leave did not commit before deadline")]
    Timeout,
    /// The leader refused the leave request.
    #[error("leave rejected: {0:?}")]
    Rejected(LeaveRejection),
    /// A peer was unreachable or the wire framing failed.
    #[error(transparent)]
    Transport(#[from] craft_net::TransportError),
}

/// Why a cluster-wide [`scale_cluster`](CraftCluster::scale_cluster) failed.
#[derive(Debug, thiserror::Error)]
pub enum ScaleClusterError {
    /// The node runtime has stopped, so its consensus status is unavailable.
    #[error("node runtime has stopped")]
    Stopped,
    /// No leader is currently elected to accept the scale.
    #[error("no leader is currently elected")]
    NoLeader,
    /// The actor config could not be encoded for forwarding.
    #[error("config encode failed: {0}")]
    Config(String),
    /// Planning or executing the scale on the leader failed.
    #[error(transparent)]
    Scale(#[from] ClusterScaleError),
    /// Forwarding the request to the leader failed (shipping to the leader, or
    /// the leader rejecting the scale).
    #[error(transparent)]
    Remote(#[from] RemoteError),
}

#[derive(Debug, thiserror::Error)]
pub enum AddRaftGroupsError {
    #[error("multi-raft catalog expansion is not enabled")]
    NotMultiRaft,
    #[error("add_groups must be at least 1")]
    InvalidCount,
    #[error("node runtime has stopped")]
    Stopped,
    #[error("no leader is currently elected")]
    NoLeader,
    #[error("catalog add rejected: {0:?}")]
    Rejected(CatalogRejection),
    #[error(transparent)]
    Transport(#[from] craft_net::TransportError),
}

impl From<CatalogAddLocalError> for AddRaftGroupsError {
    fn from(err: CatalogAddLocalError) -> Self {
        match err {
            CatalogAddLocalError::Stopped | CatalogAddLocalError::NoGroup0Handle => Self::Stopped,
            CatalogAddLocalError::NotLeader { .. } => Self::NoLeader,
            CatalogAddLocalError::Rejected(reason) => Self::Rejected(reason),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum CatalogAddLocalError {
    #[error("node runtime has stopped")]
    Stopped,
    #[error("group 0 is not hosted on this node")]
    NoGroup0Handle,
    #[error("not leader")]
    NotLeader { leader: Option<NodeId> },
    #[error("catalog add rejected: {0:?}")]
    Rejected(CatalogRejection),
}

#[cfg(test)]
mod tests {
    use craft_actor::NodeStatus;
    use craft_core::Role;
    use craft_dashboard::{EventBus, Metrics};
    use craft_proto::{LogIndex, NodeId, Term};

    use super::MembershipTelemetry;

    fn status(voters: &[u64], reachable: &[u64]) -> NodeStatus {
        NodeStatus {
            id: NodeId(1),
            role: Role::Leader,
            term: Term(1),
            leader: Some(NodeId(1)),
            commit_index: LogIndex(0),
            last_applied: LogIndex(0),
            voters: voters.iter().copied().map(NodeId).collect(),
            learners: vec![],
            reachable: reachable.iter().copied().map(NodeId).collect(),
        }
    }

    #[test]
    fn reachability_delta_flags_a_crashed_voter_without_membership_change() {
        let mut telemetry = MembershipTelemetry::new(NodeId(1), EventBus::new(16), Metrics::new());
        let _ = telemetry.record(&status(&[1, 2, 3], &[1, 2, 3]));

        let delta = telemetry.record(&status(&[1, 2, 3], &[1, 2]));

        assert!(!delta.membership_changed);
        assert!(delta.reachability_changed);
        assert_eq!(delta.unreachable, vec![NodeId(3)]);
        assert!(delta.departed.is_empty());
    }

    #[test]
    fn reachability_delta_triggers_on_heal_without_membership_change() {
        let mut telemetry = MembershipTelemetry::new(NodeId(1), EventBus::new(16), Metrics::new());
        let _ = telemetry.record(&status(&[1, 2, 3], &[1, 2]));

        let delta = telemetry.record(&status(&[1, 2, 3], &[1, 2, 3]));

        assert!(!delta.membership_changed);
        assert!(delta.reachability_changed);
        assert!(delta.unreachable.is_empty());
    }
}
