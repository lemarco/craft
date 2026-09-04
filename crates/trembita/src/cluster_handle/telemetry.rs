use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use trembita_dashboard::{EventBus, Metrics, StopReason, TraceOpts, TrembitaEvent};
use trembita_proto::NodeId;
use trembita_runtime::{ActorObserver, NodeStatus};

#[allow(clippy::cast_precision_loss)] // Prometheus gauges use f64; Raft indices fit in practice.
fn metric_u64(v: u64) -> f64 {
    v as f64
}

#[allow(clippy::cast_precision_loss)] // Prometheus gauges use f64; actor counts fit in practice.
fn metric_usize(v: usize) -> f64 {
    v as f64
}

/// Bridges the actor registry's lifecycle + per-message hooks (E14 / Track H)
/// to the telemetry planes (observability): spawns, stops, restarts, and escalations
/// emit [`TrembitaEvent`]s and bump counters, and — when opt-in tracing is enabled
/// for an actor via [`super::TrembitaCluster::trace`] — each handled message emits a
/// [`TrembitaEvent::MessageHandled`]. The registry owns no telemetry types, so the
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
            "trembita_actor_spawns_total",
            "Cumulative actor instances spawned.",
            &[("actor", name)],
            1.0,
        );
        let _ = self.events.emit(TrembitaEvent::ActorSpawned {
            id: self.id(name, instance),
        });
    }

    fn on_stopped(&self, name: &str, instance: u32) {
        self.metrics.incr(
            "trembita_actor_stops_total",
            "Cumulative actor instances stopped normally.",
            &[("actor", name)],
            1.0,
        );
        let _ = self.events.emit(TrembitaEvent::ActorStopped {
            id: self.id(name, instance),
            reason: StopReason::Normal,
        });
    }

    fn on_message_handled(&self, name: &str, instance: u32, elapsed: std::time::Duration) {
        if !self.tracing.load(Ordering::Relaxed) || !self.is_traced(name) {
            return;
        }
        let _ = self.events.emit(TrembitaEvent::MessageHandled {
            id: self.id(name, instance),
            latency_ms: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        });
    }

    fn on_restart(&self, name: &str, instance: u32, count: u32) {
        self.metrics.incr(
            "trembita_actor_restarts_total",
            "Cumulative supervised actor restarts.",
            &[("actor", name)],
            1.0,
        );
        let _ = self.events.emit(TrembitaEvent::ActorRestarted {
            id: self.id(name, instance),
            count,
        });
    }

    fn on_escalated(&self, name: &str, instance: u32) {
        self.metrics.incr(
            "trembita_actor_escalations_total",
            "Supervised actors that exhausted their restart budget and stopped.",
            &[("actor", name)],
            1.0,
        );
        let _ = self.events.emit(TrembitaEvent::ActorStopped {
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
    #[allow(clippy::too_many_lines)]
    pub(crate) fn record(&mut self, status: &NodeStatus) -> StatusDelta {
        let node = self.node_label.as_str();
        self.metrics.set(
            "trembita_raft_term",
            "Current Raft term.",
            &[("node", node)],
            metric_u64(status.term.0),
        );
        self.metrics.set(
            "trembita_raft_commit_index",
            "Highest committed log index.",
            &[("node", node)],
            metric_u64(status.commit_index.0),
        );
        self.metrics.set(
            "trembita_raft_last_applied",
            "Highest applied log index.",
            &[("node", node)],
            metric_u64(status.last_applied.0),
        );
        self.metrics.set(
            "trembita_raft_live_nodes",
            "Committed voter count.",
            &[("node", node)],
            metric_usize(status.voters.len()),
        );
        self.metrics.set(
            "trembita_raft_is_leader",
            "1 when this node currently believes it is the Raft leader.",
            &[("node", node)],
            f64::from(u8::from(matches!(status.role, trembita_core::Role::Leader))),
        );

        let leader_changed = self
            .prev
            .as_ref()
            .is_some_and(|p| p.leader != status.leader || p.term != status.term);
        if leader_changed && let Some(leader) = status.leader {
            self.metrics.incr(
                "trembita_raft_leader_changes_total",
                "Observed leadership changes.",
                &[("node", node)],
                1.0,
            );
            let _ = self.events.emit(TrembitaEvent::LeaderChanged {
                term: status.term.0,
                leader: leader.0,
            });
        }

        let commit_advanced = self
            .prev
            .as_ref()
            .is_some_and(|p| p.commit_index != status.commit_index);
        if commit_advanced {
            let _ = self.events.emit(TrembitaEvent::RaftCommitted {
                commit_index: status.commit_index.0,
                term: status.term.0,
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
                    "trembita_cluster_node_joins_total",
                    "Nodes observed joining the committed voter set.",
                    &[("node", node)],
                    1.0,
                );
                let _ = self
                    .events
                    .emit(TrembitaEvent::NodeJoined { node_id: joined.0 });
            }
            for left in prev_v.difference(&new_v) {
                membership_changed = true;
                departed.push(*left);
                self.metrics.incr(
                    "trembita_cluster_node_leaves_total",
                    "Nodes observed leaving the committed voter set.",
                    &[("node", node)],
                    1.0,
                );
                let _ = self.events.emit(TrembitaEvent::NodeLeft {
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
