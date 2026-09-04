use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Observes actor lifecycle transitions (E14 / observability Track H).
pub trait ActorObserver: Send + Sync {
    /// A fresh instance of `name` started (plain, supervised, or restored).
    fn on_spawned(&self, _name: &str, _instance: u32) {}
    /// An instance stopped for a non-escalation reason (explicit stop, drain,
    /// scale-in, or the source side of a migration).
    fn on_stopped(&self, _name: &str, _instance: u32) {}
    /// An instance finished handling one message in `elapsed`. Hot path —
    /// implementations should fast-path when no work is required.
    fn on_message_handled(&self, _name: &str, _instance: u32, _elapsed: Duration) {}
    /// A supervised instance rebuilt fresh state after a handler failure.
    /// `count` is the group's cumulative restart tally after this restart.
    fn on_restart(&self, name: &str, instance: u32, count: u32);
    /// A supervised instance exhausted its restart budget (or could not rebuild)
    /// and escalated: the instance stopped and deregistered itself.
    fn on_escalated(&self, name: &str, instance: u32);
}

/// A slot an [`ActorObserver`] can be installed into after construction (the
/// registry outlives the telemetry wiring in the builder). Read once per
/// instance task at launch.
pub(super) type ObserverHook = Arc<Mutex<Option<Arc<dyn ActorObserver>>>>;
pub(super) type ComputeTokenHook = Arc<Mutex<Option<Arc<crate::ComputeTokenPool>>>>;

pub(super) fn mailbox_depth_u64(depth: i64) -> u64 {
    depth.max(0).cast_unsigned()
}

/// Point-in-time introspection for one locally hosted actor instance (Observer /
/// dashboard). Remote instances are not included (directory entries on other
/// nodes report zero).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalActorIntrospection {
    /// Registered group name.
    pub name: String,
    /// Instance index within the group.
    pub instance: u32,
    /// Currently-queued messages for this instance (instantaneous).
    pub mailbox_depth: i64,
    /// Wall-time since this instance task was launched on this node.
    pub uptime_secs: u64,
}

/// A point-in-time snapshot of one actor group's runtime counters, for metrics
/// sampling (observability §2). Cumulative counters (`messages`, `handle_nanos`) are
/// monotonic; the sampler derives rates/latency by differencing successive
/// reads. `mailbox_depth` is instantaneous (queued-but-unhandled messages).
#[derive(Debug, Clone)]
pub struct ActorGroupStats {
    /// The group's registered name.
    pub name: String,
    /// Live instance count.
    pub instances: usize,
    /// Cumulative messages handled across the group's instances.
    pub messages: u64,
    /// Cumulative wall-time spent in `handle`, in nanoseconds.
    pub handle_nanos: u64,
    /// Currently-queued (enqueued but not yet handled) messages.
    pub mailbox_depth: i64,
}
