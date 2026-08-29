//! [`CraftyCluster`] — the running node handle returned by the builder.
//!
//! It bundles everything the facade wired together: the consensus/actor runtime
//! (via an in-process [`NodeHandle`] for zero-copy L1 clients), the actor
//! control/messaging/directory planes, the leader-only supervisor, and the
//! telemetry [`EventBus`] + [`Metrics`]. Background tasks (facts refresh,
//! directory anti-entropy, supervisor reconcile, admin server) run until
//! [`shutdown`](CraftyCluster::shutdown) or the handle is dropped.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crafty_core::StateMachine;
use crafty_dashboard::{CraftyEvent, EventBus, Metrics, StopReason, TraceOpts};
use crafty_net::RemoteError;
use crafty_net::transport::RequestHandler;
use crafty_net::{Transport, send_catalog_add_request, send_leave_request};
use crafty_proto::{
    CatalogAddRequest, CatalogAddResponse, CatalogRejection, LeaveRejection, LeaveRequest,
    LeaveResponse, Membership, NodeId, PROTOCOL_VERSION, ScaleRequest,
};
use tokio::task::JoinHandle;

use crafty_actor::{
    ActorDirectory, ActorObserver, ActorRegistry, ClusterControl, ClusterMessaging,
    ClusterScaleError, ClusterState, ClusterSupervisor, DirectorySync, NOT_LEADER_REASON,
    NodeHandle, NodeStatus, ResourceProfile, UserActor, VpsResources,
};

use crate::multi_raft::MultiRaftState;

use crate::CraftyClusterBuilder;
use crate::certs::CertReloadHandle;

#[allow(clippy::cast_precision_loss)] // Prometheus gauges use f64; Raft indices fit in practice.
fn metric_u64(v: u64) -> f64 {
    v as f64
}

#[allow(clippy::cast_precision_loss)] // Prometheus gauges use f64; actor counts fit in practice.
fn metric_usize(v: usize) -> f64 {
    v as f64
}

/// The live leadership/membership facts the supervisor reconciles against
/// (implements [`ClusterState`]), refreshed from the node's consensus status by
/// a background task. Exposed only so [`CraftyCluster::supervisor`] has a nameable
/// type; you rarely construct or read it directly.
#[derive(Default)]
pub struct ClusterFacts {
    leader: AtomicBool,
    leader_id: Mutex<Option<NodeId>>,
    voters: Mutex<Vec<NodeId>>,
    reachable: Mutex<Vec<NodeId>>,
}

impl ClusterFacts {
    pub(crate) fn update(&self, status: &NodeStatus) {
        self.leader.store(
            matches!(status.role, crafty_core::Role::Leader),
            Ordering::SeqCst,
        );
        *self.leader_id.lock().unwrap() = status.leader;
        self.voters.lock().unwrap().clone_from(&status.voters);
        self.reachable.lock().unwrap().clone_from(&status.reachable);
    }

    /// Current Raft leader hint (refreshed with consensus status).
    ///
    /// # Panics
    /// If the internal mutex is poisoned.
    #[must_use]
    pub fn leader_id(&self) -> Option<NodeId> {
        *self.leader_id.lock().unwrap()
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

    fn leader_id(&self) -> Option<NodeId> {
        *self.leader_id.lock().unwrap()
    }
}

/// Bridges the actor registry's lifecycle + per-message hooks (E14 / Track H)
/// to the telemetry planes (observability): spawns, stops, restarts, and escalations
/// emit [`CraftyEvent`]s and bump counters, and — when opt-in tracing is enabled
/// for an actor via [`CraftyCluster::trace`] — each handled message emits a
/// [`CraftyEvent::MessageHandled`]. The registry owns no telemetry types, so the
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
            "crafty_actor_spawns_total",
            "Cumulative actor instances spawned.",
            &[("actor", name)],
            1.0,
        );
        let _ = self.events.emit(CraftyEvent::ActorSpawned {
            id: self.id(name, instance),
        });
    }

    fn on_stopped(&self, name: &str, instance: u32) {
        self.metrics.incr(
            "crafty_actor_stops_total",
            "Cumulative actor instances stopped normally.",
            &[("actor", name)],
            1.0,
        );
        let _ = self.events.emit(CraftyEvent::ActorStopped {
            id: self.id(name, instance),
            reason: StopReason::Normal,
        });
    }

    fn on_message_handled(&self, name: &str, instance: u32, elapsed: std::time::Duration) {
        if !self.tracing.load(Ordering::Relaxed) || !self.is_traced(name) {
            return;
        }
        let _ = self.events.emit(CraftyEvent::MessageHandled {
            id: self.id(name, instance),
            latency_ms: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        });
    }

    fn on_restart(&self, name: &str, instance: u32, count: u32) {
        self.metrics.incr(
            "crafty_actor_restarts_total",
            "Cumulative supervised actor restarts.",
            &[("actor", name)],
            1.0,
        );
        let _ = self.events.emit(CraftyEvent::ActorRestarted {
            id: self.id(name, instance),
            count,
        });
    }

    fn on_escalated(&self, name: &str, instance: u32) {
        self.metrics.incr(
            "crafty_actor_escalations_total",
            "Supervised actors that exhausted their restart budget and stopped.",
            &[("actor", name)],
            1.0,
        );
        let _ = self.events.emit(CraftyEvent::ActorStopped {
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
            "crafty_raft_term",
            "Current Raft term.",
            &[("node", node)],
            metric_u64(status.term.0),
        );
        self.metrics.set(
            "crafty_raft_commit_index",
            "Highest committed log index.",
            &[("node", node)],
            metric_u64(status.commit_index.0),
        );
        self.metrics.set(
            "crafty_raft_last_applied",
            "Highest applied log index.",
            &[("node", node)],
            metric_u64(status.last_applied.0),
        );
        self.metrics.set(
            "crafty_raft_live_nodes",
            "Committed voter count.",
            &[("node", node)],
            metric_usize(status.voters.len()),
        );
        self.metrics.set(
            "crafty_raft_is_leader",
            "1 when this node currently believes it is the Raft leader.",
            &[("node", node)],
            f64::from(u8::from(matches!(status.role, crafty_core::Role::Leader))),
        );

        let leader_changed = self
            .prev
            .as_ref()
            .is_some_and(|p| p.leader != status.leader || p.term != status.term);
        if leader_changed && let Some(leader) = status.leader {
            self.metrics.incr(
                "crafty_raft_leader_changes_total",
                "Observed leadership changes.",
                &[("node", node)],
                1.0,
            );
            let _ = self.events.emit(CraftyEvent::LeaderChanged {
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
                    "crafty_cluster_node_joins_total",
                    "Nodes observed joining the committed voter set.",
                    &[("node", node)],
                    1.0,
                );
                let _ = self
                    .events
                    .emit(CraftyEvent::NodeJoined { node_id: joined.0 });
            }
            for left in prev_v.difference(&new_v) {
                membership_changed = true;
                departed.push(*left);
                self.metrics.incr(
                    "crafty_cluster_node_leaves_total",
                    "Nodes observed leaving the committed voter set.",
                    &[("node", node)],
                    1.0,
                );
                let _ = self.events.emit(CraftyEvent::NodeLeft {
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

/// A running crafty node: the facade's single entry point once
/// [`start`](crate::CraftyClusterBuilder::start_local) returns.
///
/// Clone-free but cheap to pass by reference; drop it (or call
/// [`shutdown`](Self::shutdown)) to stop the node.
pub struct CraftyCluster<M: StateMachine> {
    pub(crate) node_id: NodeId,
    pub(crate) handle: NodeHandle<M>,
    pub(crate) group_handles: Vec<NodeHandle<M>>,
    /// Meta-Raft coordinator handle when `raft_groups > 1`.
    pub(crate) meta_handle: Option<crafty_actor::NodeHandle<crafty_actor::MetaStateMachine>>,
    pub(crate) raft_groups: u32,
    pub(crate) shard_count: u32,
    pub(crate) shard_routing: crafty_core::ShardRoutingKind,
    pub(crate) registry: ActorRegistry,
    pub(crate) control: Arc<ClusterControl>,
    pub(crate) messaging: Arc<ClusterMessaging>,
    pub(crate) directory: Arc<ActorDirectory>,
    pub(crate) directory_sync: Arc<DirectorySync>,
    pub(crate) supervisor: Arc<ClusterSupervisor<Arc<ClusterFacts>>>,
    pub(crate) events: EventBus,
    pub(crate) metrics: Metrics,
    pub(crate) catalog_version: Arc<AtomicU32>,
    pub(crate) saga_registry: crate::saga::SagaRegistry,
    pub(crate) two_phase_registry: crate::two_phase::TwoPhaseRegistry,
    pub(crate) queue_autoscale_registry: Arc<crafty_actor::QueueAutoscaleRegistry>,
    pub(crate) telemetry: Arc<ActorTelemetry>,
    pub(crate) members: Vec<NodeId>,
    pub(crate) resource_profile: ResourceProfile,
    pub(crate) vps_resources: VpsResources,
    pub(crate) actor_state_store: Option<Arc<dyn crafty_actor::ActorStateStore>>,
    /// Cluster-facing queue clients keyed by stream name.
    pub(crate) job_queues: HashMap<String, Arc<dyn crafty_actor::JobQueue>>,
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

impl<M: StateMachine> CraftyCluster<M> {
    /// Start describing a node running `machine`, identified by `node_id`. See
    /// [`CraftyClusterBuilder`](crate::CraftyClusterBuilder) for the options.
    #[must_use]
    pub fn builder(node_id: NodeId, machine: M) -> CraftyClusterBuilder<M>
    where
        M: Default,
    {
        CraftyClusterBuilder::new(node_id, machine)
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
    /// [`CraftyClusterBuilder::actor_state_store`](crate::CraftyClusterBuilder::actor_state_store),
    /// if any (actor-state-redis). Clone the `Arc` into actor `Config` when spawning
    /// stateful workers.
    #[must_use]
    pub fn actor_state_store(&self) -> Option<Arc<dyn crafty_actor::ActorStateStore>> {
        self.actor_state_store.clone()
    }

    /// Cluster-facing queue client for `stream`, routing through the leader wire
    /// service ([`CraftyClusterBuilder::job_queue`](crate::CraftyClusterBuilder::job_queue)).
    #[must_use]
    pub fn job_queue(&self, stream: &str) -> Option<Arc<dyn crafty_actor::JobQueue>> {
        self.job_queues.get(stream).cloned()
    }

    /// Enqueue many jobs in one leader transaction (tier C batch path).
    ///
    /// Batches are capped at [`crafty_actor::DEFAULT_QUEUE_BATCH_MAX`] jobs per RPC.
    ///
    /// # Errors
    /// Returns an error when the stream is unknown or enqueue fails.
    pub async fn enqueue_batch(
        &self,
        stream: &str,
        payloads: &[&[u8]],
    ) -> Result<Vec<crafty_actor::JobId>, crafty_actor::QueueError> {
        let queue = self.job_queue(stream).ok_or_else(|| {
            crafty_actor::QueueError::Backend(format!("unknown stream {stream:?}"))
        })?;
        queue.enqueue_batch(payloads).await
    }

    /// Enqueue many jobs with per-job options in one leader transaction.
    ///
    /// # Errors
    /// Returns an error when the stream is unknown or enqueue fails.
    pub async fn enqueue_batch_opts(
        &self,
        stream: &str,
        jobs: &[(Vec<u8>, crafty_actor::EnqueueOptions)],
    ) -> Result<Vec<crafty_actor::JobId>, crafty_actor::QueueError> {
        let queue = self.job_queue(stream).ok_or_else(|| {
            crafty_actor::QueueError::Backend(format!("unknown stream {stream:?}"))
        })?;
        queue.enqueue_batch_opts(jobs).await
    }

    /// Registry of queue autoscale policies replicated via Meta-Raft / group 0.
    #[must_use]
    pub fn queue_autoscale_registry(&self) -> Arc<crafty_actor::QueueAutoscaleRegistry> {
        Arc::clone(&self.queue_autoscale_registry)
    }

    /// Default saga journal: Meta-Raft metadata (multi-Raft) or group 0 (single-group),
    /// optionally mirrored to [`Self::actor_state_store`] when configured.
    ///
    /// # Panics
    /// In single-group mode when group 0 is not hosted on this node.
    #[must_use]
    pub fn saga_journal(&self) -> Arc<dyn crafty_client::SagaJournal> {
        let raft_journal = if let Some(meta) = &self.meta_handle {
            crate::saga::MetaRaftSagaJournal::new(meta.clone(), Arc::clone(&self.saga_registry))
        } else {
            crate::saga::MetaRaftSagaJournal::new(
                self.group_handle(0)
                    .expect("group 0 handle required for saga journal"),
                Arc::clone(&self.saga_registry),
            )
        };
        if let Some(store) = &self.actor_state_store {
            Arc::new(crate::saga::CompositeSagaJournal::new(
                raft_journal,
                Some(crate::saga::StoreSagaJournal::new(Arc::clone(store))),
            ))
        } else {
            Arc::new(raft_journal)
        }
    }

    /// Default 2PC client journal: Meta-Raft metadata (multi-Raft) or group 0,
    /// optionally mirrored to [`Self::actor_state_store`] when configured.
    ///
    /// # Panics
    /// In single-group mode when group 0 is not hosted on this node.
    #[must_use]
    pub fn two_phase_journal(&self) -> Arc<dyn crafty_client::TwoPhaseJournal> {
        let raft_journal = if let Some(meta) = &self.meta_handle {
            crate::two_phase::MetaRaftTwoPhaseJournal::new(
                meta.clone(),
                Arc::clone(&self.two_phase_registry),
            )
        } else {
            crate::two_phase::MetaRaftTwoPhaseJournal::new(
                self.group_handle(0)
                    .expect("group 0 handle required for 2PC journal"),
                Arc::clone(&self.two_phase_registry),
            )
        };
        if let Some(store) = &self.actor_state_store {
            Arc::new(crate::two_phase::CompositeTwoPhaseJournal::new(
                raft_journal,
                Some(crate::two_phase::StoreTwoPhaseJournal::new(Arc::clone(
                    store,
                ))),
            ))
        } else {
            Arc::new(raft_journal)
        }
    }

    /// The in-process client handle for Raft group 0 (single-group default).
    /// With multi-Raft, prefer [`group_handle`](Self::group_handle) — the
    /// bootstrap handle may be retired after rebalance.
    #[must_use]
    pub fn handle(&self) -> &NodeHandle<M> {
        &self.handle
    }

    /// Active handle for Raft group `group`, if this node currently hosts it.
    ///
    /// # Panics
    /// If the multi-Raft handle map mutex is poisoned.
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
    ///
    /// # Panics
    /// If the multi-Raft handle map mutex is poisoned.
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
    ///
    /// # Panics
    /// If the multi-Raft catalog mutex is poisoned.
    #[must_use]
    pub fn raft_groups(&self) -> u32 {
        if let Some(mr) = &self.multi_raft {
            u32::try_from(mr.catalog.lock().unwrap().len()).unwrap_or(u32::MAX)
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
    pub fn shard_routing(&self) -> crafty_core::ShardRoutingKind {
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
    /// Returns [`crafty_core::ShardExpansionError::NotMultiRaft`] on single-Raft
    /// clusters, [`crafty_core::ShardExpansionError::StableRoutingActive`] when stable
    /// routing is configured, or planner errors when `new_count` is invalid.
    pub fn expand_shard_count(
        &self,
        new_count: u32,
    ) -> Result<crafty_core::ShardCountExpansionPlan, crafty_core::ShardExpansionError> {
        let mr = self
            .multi_raft
            .as_ref()
            .ok_or(crafty_core::ShardExpansionError::NotMultiRaft)?;
        mr.sharded.expand_shard_count(new_count)
    }

    /// Grow the active virtual shard prefix (Tier 2 stable routing). Existing keyed
    /// traffic keeps the same virtual shard id.
    ///
    /// # Errors
    /// Returns [`crafty_core::StableShardActivationError::NotMultiRaft`] on single-Raft
    /// clusters, [`crafty_core::StableShardActivationError::ModulusRoutingActive`] when
    /// modulus routing is configured, or planner errors when `new_active` is invalid.
    pub fn activate_shards(
        &self,
        new_active: u32,
    ) -> Result<crafty_core::StableShardActivationPlan, crafty_core::StableShardActivationError>
    {
        let mr = self
            .multi_raft
            .as_ref()
            .ok_or(crafty_core::StableShardActivationError::NotMultiRaft)?;
        mr.sharded.activate_shards(new_active)
    }

    /// Switch keyed routing from Tier 1 modulus to Tier 2 stable virtual.
    ///
    /// Keys **remap** — drain keyed clients before calling.
    ///
    /// # Errors
    /// Returns [`crafty_core::ShardRoutingSwitchError::NotMultiRaft`] on single-Raft
    /// clusters or [`crafty_core::ShardRoutingSwitchError::AlreadyStable`] when stable
    /// routing is already active.
    pub fn switch_to_stable_shards(
        &self,
    ) -> Result<crafty_core::ShardRoutingSwitchPlan, crafty_core::ShardRoutingSwitchError> {
        let mr = self
            .multi_raft
            .as_ref()
            .ok_or(crafty_core::ShardRoutingSwitchError::NotMultiRaft)?;
        mr.sharded.switch_to_stable_routing()
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

    /// Monotonic catalog generation (starts at 1; bumps on each committed expansion).
    #[must_use]
    pub fn catalog_version(&self) -> u32 {
        self.catalog_version.load(Ordering::SeqCst)
    }

    /// Record one saga lifecycle event on this node's metrics registry.
    pub fn record_saga_event(&self, event: crafty_client::SagaEvent) {
        crate::saga::record_saga_event(&self.metrics, self.node_id.0, event);
    }

    /// Metrics hook for [`crafty_client::RunSagaOpts::on_event`].
    #[must_use]
    pub fn saga_metrics_callback(&self) -> Arc<dyn Fn(crafty_client::SagaEvent) + Send + Sync> {
        crate::saga::saga_metrics_callback(self.metrics.clone(), self.node_id.0)
    }

    /// Record one 2PC lifecycle event on this node's metrics registry.
    pub fn record_two_phase_event(&self, event: crafty_client::TwoPhaseEvent) {
        crate::two_phase::record_two_phase_event(&self.metrics, self.node_id.0, event);
    }

    /// Metrics hook for [`crafty_client::RunTwoPhaseOpts::on_event`].
    #[must_use]
    pub fn two_phase_metrics_callback(
        &self,
    ) -> Arc<dyn Fn(crafty_client::TwoPhaseEvent) + Send + Sync> {
        crate::two_phase::two_phase_metrics_callback(self.metrics.clone(), self.node_id.0)
    }

    fn group_for_key(&self, key: &[u8]) -> Option<u32> {
        use crafty_core::{RaftGroupId, StableShardRouter, place_shard};
        let router = StableShardRouter::new(self.shard_count);
        let shard = router.shard_for(key)?;
        let groups: Vec<RaftGroupId> = (0..self.raft_groups).map(RaftGroupId).collect();
        place_shard(shard, &groups).map(|g| g.0)
    }

    /// Run a cross-shard 2PC transaction with journal + metrics wired.
    ///
    /// # Errors
    /// Same as [`crafty_client::propose_cross_shard_2pc`].
    pub async fn run_keyed_2pc<C: crafty_client::TwoPhaseClient>(
        &self,
        client: &C,
        plan: &crafty_core::TwoPhasePlan,
    ) -> Result<Vec<Vec<u8>>, crafty_client::TwoPhaseError> {
        let journal = self.two_phase_journal();
        let on_event = self.two_phase_metrics_callback();
        crafty_client::propose_cross_shard_2pc_with_opts(
            client,
            plan,
            |key| self.group_for_key(key),
            crafty_client::RunTwoPhaseOpts {
                journal: Some(journal.as_ref()),
                on_event: Some(on_event.as_ref()),
            },
        )
        .await
    }

    /// Resume a cross-shard 2PC from its durable client journal with metrics wired.
    ///
    /// # Errors
    /// Same as [`crafty_client::resume_cross_shard_2pc`].
    pub async fn resume_cross_shard_2pc<C: crafty_client::TwoPhaseClient>(
        &self,
        client: &C,
        plan: &crafty_core::TwoPhasePlan,
    ) -> Result<Vec<Vec<u8>>, crafty_client::TwoPhaseError> {
        let journal = self.two_phase_journal();
        let on_event = self.two_phase_metrics_callback();
        crafty_client::resume_cross_shard_2pc(
            client,
            plan,
            |key| self.group_for_key(key),
            crafty_client::ResumeTwoPhaseOpts {
                journal: Some(journal.as_ref()),
                probe: true,
                on_event: Some(on_event.as_ref()),
            },
        )
        .await
    }

    /// Run a cross-shard saga with catalog version pinned and metrics wired.
    ///
    /// # Errors
    /// Same as [`crafty_client::run_saga`].
    pub async fn run_keyed_saga<C: crafty_client::KeyedClient>(
        &self,
        client: &C,
        plan: &crafty_client::SagaPlan,
        journal: &dyn crafty_client::SagaJournal,
    ) -> Result<crafty_client::SagaOutcome, crafty_client::SagaError> {
        let on_event = self.saga_metrics_callback();
        crafty_client::run_saga(
            client,
            plan,
            crafty_client::RunSagaOpts {
                journal: Some(journal),
                catalog_version: Some(self.catalog_version()),
                catalog_version_live: Some(Arc::clone(&self.catalog_version)),
                on_event: Some(on_event.as_ref()),
            },
        )
        .await
    }

    /// Resume a cross-shard saga from its durable journal with metrics wired.
    ///
    /// # Errors
    /// Same as [`crafty_client::resume_saga`].
    pub async fn resume_keyed_saga<C: crafty_client::KeyedClient>(
        &self,
        client: &C,
        plan: &crafty_client::SagaPlan,
        journal: &dyn crafty_client::SagaJournal,
    ) -> Result<crafty_client::SagaOutcome, crafty_client::SagaError> {
        let on_event = self.saga_metrics_callback();
        crafty_client::resume_saga(
            client,
            plan,
            crafty_client::ResumeSagaOpts {
                journal,
                catalog_version_live: Some(Arc::clone(&self.catalog_version)),
                on_event: Some(on_event.as_ref()),
            },
        )
        .await
    }

    /// Enable opt-in per-message tracing (observability §7, H6) for the local actor
    /// group `name`. While active, every message that group handles emits a
    /// [`CraftyEvent::MessageHandled`] (with handling latency) onto
    /// [`events`](Self::events). Tracing auto-expires after `opts.duration`, so
    /// it never runs unbounded. Off by default — steady-state message flow does
    /// not emit per-message events, only the sampled rate/latency metrics.
    pub fn trace(&self, name: &str, opts: &TraceOpts) {
        self.telemetry.enable_trace(name, opts);
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
        if let Some(meta) = &self.meta_handle {
            return meta
                .status()
                .await
                .is_some_and(|s| matches!(s.role, crafty_core::Role::Leader));
        }
        let Some(handle) = self.group_handle(0) else {
            return false;
        };
        handle
            .status()
            .await
            .is_some_and(|s| matches!(s.role, crafty_core::Role::Leader))
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
    ) -> Result<LeaveResponse, crafty_net::TransportError> {
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
                        let _ = self.events.emit(CraftyEvent::NodeLeft {
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
                    LeaveResponse::Redirect { .. } => Err(LeaveError::Timeout),
                }
            }
            LeaveResponse::Rejected { reason } => Err(LeaveError::Rejected(reason)),
            LeaveResponse::Redirect { .. } => Err(LeaveError::Timeout),
        }
    }

    /// Grow the multi-Raft group catalog without restarting nodes (Tier 2).
    ///
    /// # Errors
    /// Returns [`AddRaftGroupsError`] when multi-Raft is disabled, `count` is
    /// zero, the runtime has stopped, no leader is elected, the catalog add is
    /// rejected, or the wire request fails.
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
            if matches!(status.role, crafty_core::Role::Leader) {
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
                CatalogAddResponse::Accepted { new_groups, .. } => {
                    self.catalog_version.fetch_add(1, Ordering::SeqCst);
                    return Ok(new_groups);
                }
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
        let request = CatalogAddRequest {
            protocol_version: PROTOCOL_VERSION,
            add_groups: count,
        };
        let response = if let Some(meta) = &self.meta_handle {
            meta.catalog_add(request).await
        } else if let Some(handle) = self.group_handle(0) {
            handle.catalog_add(request).await
        } else {
            return Err(CatalogAddLocalError::NoMetaRaftHandle);
        }
        .map_err(|_| CatalogAddLocalError::Stopped)?;
        match response {
            CatalogAddResponse::Accepted { new_groups, .. } => {
                self.catalog_version.fetch_add(1, Ordering::SeqCst);
                Ok(new_groups)
            }
            CatalogAddResponse::Redirect { leader } => {
                Err(CatalogAddLocalError::NotLeader { leader })
            }
            CatalogAddResponse::Rejected { reason } => Err(CatalogAddLocalError::Rejected(reason)),
        }
    }

    /// Hot-reload handle when the node was started with [`CraftyClusterBuilder::start_quic_pem`]
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
            if matches!(status.role, crafty_core::Role::Leader) {
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
    /// [`DirectoryPolicy::ReadYourWrites`](crafty_actor::DirectoryPolicy::ReadYourWrites) is enabled.
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
    ///
    /// # Errors
    /// Propagates [`crafty_actor::StopError`] when the group is unknown.
    pub fn set_group_drain_timeout(
        &self,
        name: &str,
        timeout: Option<Duration>,
    ) -> Result<(), crafty_actor::StopError> {
        self.registry.set_group_drain_timeout(name, timeout)
    }

    /// Gracefully stop a local actor group using the cluster drain default.
    ///
    /// # Errors
    /// Propagates [`crafty_actor::StopError`] when the group is unknown or drain fails.
    pub async fn stop_group_graceful(
        &self,
        name: &str,
    ) -> Result<crafty_actor::DrainOutcome, crafty_actor::StopError> {
        self.registry.stop_graceful(name, self.drain_timeout).await
    }

    /// Stop the node: shut the runtime down and abort all background tasks.
    ///
    /// # Panics
    /// If a multi-Raft handle map or task-list mutex is poisoned.
    pub fn shutdown(&self) {
        if let Some(meta) = &self.meta_handle {
            meta.shutdown();
        }
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
    ///
    /// # Panics
    /// If the background task-list mutex is poisoned.
    pub async fn shutdown_and_wait(&self) {
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
        if let Some(meta) = &self.meta_handle {
            meta.shutdown_and_wait().await;
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

impl<M: StateMachine> Drop for CraftyCluster<M> {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// How long [`scale_cluster`](CraftyCluster::scale_cluster) keeps re-resolving
/// the leader while a forwarded scale is transiently refused because leadership
/// is still settling. Comfortably exceeds the facts-refresh period so a scale
/// issued right after an election succeeds rather than failing spuriously.
const SCALE_FORWARD_TIMEOUT: Duration = Duration::from_secs(5);
/// Delay between forward retries within [`SCALE_FORWARD_TIMEOUT`].
const SCALE_FORWARD_RETRY: Duration = Duration::from_millis(25);
const CATALOG_ADD_TIMEOUT: Duration = Duration::from_secs(5);
const CATALOG_ADD_RETRY: Duration = Duration::from_millis(25);
/// Total budget for [`CraftyCluster::leave`] peer retries.
const LEAVE_TIMEOUT: Duration = Duration::from_secs(30);
/// Delay between leave attempts within [`LEAVE_TIMEOUT`].
const LEAVE_RETRY: Duration = Duration::from_millis(50);

/// Why [`CraftyCluster::leave`] failed.
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
    Transport(#[from] crafty_net::TransportError),
}

/// Why a cluster-wide [`scale_cluster`](CraftyCluster::scale_cluster) failed.
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

/// Why [`CraftyCluster::add_raft_groups`] failed.
#[derive(Debug, thiserror::Error)]
pub enum AddRaftGroupsError {
    /// Multi-Raft catalog expansion is not enabled on this cluster.
    #[error("multi-raft catalog expansion is not enabled")]
    NotMultiRaft,
    /// The requested group count must be at least 1.
    #[error("add_groups must be at least 1")]
    InvalidCount,
    /// The node runtime has stopped, so catalog updates are unavailable.
    #[error("node runtime has stopped")]
    Stopped,
    /// No leader is currently elected to accept the catalog add.
    #[error("no leader is currently elected")]
    NoLeader,
    /// The catalog leader rejected the add request.
    #[error("catalog add rejected: {0:?}")]
    Rejected(CatalogRejection),
    /// A peer was unreachable or the wire framing failed.
    #[error(transparent)]
    Transport(#[from] crafty_net::TransportError),
}

impl From<CatalogAddLocalError> for AddRaftGroupsError {
    fn from(err: CatalogAddLocalError) -> Self {
        match err {
            CatalogAddLocalError::Stopped | CatalogAddLocalError::NoMetaRaftHandle => Self::Stopped,
            CatalogAddLocalError::NotLeader { .. } => Self::NoLeader,
            CatalogAddLocalError::Rejected(reason) => Self::Rejected(reason),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum CatalogAddLocalError {
    #[error("node runtime has stopped")]
    Stopped,
    #[error("meta raft group is not hosted on this node")]
    NoMetaRaftHandle,
    #[error("not leader")]
    NotLeader { leader: Option<NodeId> },
    #[error("catalog add rejected: {0:?}")]
    Rejected(CatalogRejection),
}

#[cfg(test)]
mod tests {
    use crafty_actor::NodeStatus;
    use crafty_core::Role;
    use crafty_dashboard::{EventBus, Metrics};
    use crafty_proto::{LogIndex, NodeId, Term};

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
