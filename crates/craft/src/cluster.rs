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
use craft_proto::{NodeId, ScaleRequest};
use tokio::task::JoinHandle;

use craft_actor::{
    ActorDirectory, ActorObserver, ActorRegistry, ClusterControl, ClusterMessaging,
    ClusterScaleError, ClusterState, ClusterSupervisor, DirectorySync, NOT_LEADER_REASON,
    NodeHandle, NodeStatus, UserActor,
};

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
/// to the telemetry planes (ADR 026): spawns, stops, restarts, and escalations
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

/// What changed in the committed voter set since the previous status tick.
pub(crate) struct StatusDelta {
    /// Voters present last tick but gone now (crash / leave).
    pub departed: Vec<NodeId>,
    /// Whether the committed voter set changed at all (join or leave).
    pub membership_changed: bool,
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
        if leader_changed {
            if let Some(leader) = status.leader {
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
        }

        let mut departed = Vec::new();
        let mut membership_changed = false;
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
        }
        self.prev = Some(status.clone());
        StatusDelta {
            departed,
            membership_changed,
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
    pub(crate) tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl<M: StateMachine> CraftCluster<M> {
    /// Start describing a node running `machine`, identified by `node_id`. See
    /// [`CraftClusterBuilder`](crate::CraftClusterBuilder) for the options.
    #[must_use]
    pub fn builder(node_id: NodeId, machine: M) -> crate::CraftClusterBuilder<M> {
        crate::CraftClusterBuilder::new(node_id, machine)
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

    /// The in-process client handle for Raft group 0 (single-group default).
    /// With multi-Raft, use [`group_handles`](Self::group_handles) for other
    /// groups and keyed client APIs for shard-aware routing.
    #[must_use]
    pub fn handle(&self) -> &NodeHandle<M> {
        &self.handle
    }

    /// One [`NodeHandle`] per hosted Raft group (length equals
    /// [`raft_groups`](Self::raft_groups)).
    #[must_use]
    pub fn group_handles(&self) -> &[NodeHandle<M>] {
        &self.group_handles
    }

    /// Number of independent Raft groups on this node (1 = default).
    #[must_use]
    pub fn raft_groups(&self) -> u32 {
        self.raft_groups
    }

    /// Shard count used for keyed client routing when `raft_groups > 1`.
    #[must_use]
    pub fn shard_count(&self) -> u32 {
        self.shard_count
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

    /// Enable opt-in per-message tracing (ADR 026 §7, H6) for the local actor
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

    /// Whether this node currently believes it is the Raft leader.
    pub async fn is_leader(&self) -> bool {
        self.handle
            .status()
            .await
            .is_some_and(|s| matches!(s.role, craft_core::Role::Leader))
    }

    /// Drive actor group `name` to `total` instances cluster-wide (one worker
    /// per node, ADR 014). Cluster-wide placement is the **leader's** job, so
    /// this transparently forwards to the leader when called on a follower
    /// (`/actor/scale`, ADR 018) — mirroring how client writes are forwarded
    /// (ADR 003). On the leader it plans and executes directly against the
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

    /// Stop the node: shut the runtime down and abort all background tasks.
    pub fn shutdown(&self) {
        for handle in &self.group_handles {
            handle.shutdown();
        }
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
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
