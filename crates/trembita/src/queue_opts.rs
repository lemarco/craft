//! Job queue registration options for [`TrembitaAppBuilder`](super::app::TrembitaAppBuilder).

use std::time::Duration;

use trembita_jobs::{AutoShardPolicy, DEFAULT_QUEUE_PREFETCH};

/// Physical layout for a registered job stream (B-32 product coordination scale).
#[derive(Debug, Clone, Default)]
pub enum QueueRegistrationScale {
    /// One redb stream (`queue-{name}.redb`).
    #[default]
    Standard,
    /// Federated `{name}~0` … `{name}~{n-1}` ([job-queue](../../docs/decisions/job-queue.md)).
    Sharded(usize),
    /// Start at `{name}~0`; leader may add shards under load.
    AutoShard(AutoShardPolicy),
}

/// One durable job stream for [`.queue`](crate::AppManifest::queue).
#[derive(Debug, Clone)]
pub struct QueueOpts {
    /// Stream name (`queue-{name}.redb` under `data_dir`).
    pub name: String,
    /// Lease timeout for workers holding jobs from this stream.
    pub lease: Duration,
    /// Leader prefetch depth (`0` = disable). Default: [`DEFAULT_QUEUE_PREFETCH`].
    pub prefetch: usize,
    /// Default delivery-attempt ceiling for jobs that do not set their own (`0` = unlimited).
    pub default_max_attempts: u32,
    /// Single stream vs sharded / auto-shard coordination queue.
    pub scale: QueueRegistrationScale,
}

impl QueueOpts {
    /// Register a queue with framework default prefetch.
    #[must_use]
    pub fn new(name: impl Into<String>, lease: Duration) -> Self {
        Self {
            name: name.into(),
            lease,
            prefetch: DEFAULT_QUEUE_PREFETCH,
            default_max_attempts: 0,
            scale: QueueRegistrationScale::Standard,
        }
    }

    /// Spread replication across `shard_count` physical streams (`{name}~0`, …).
    #[must_use]
    pub fn sharded(mut self, shard_count: usize) -> Self {
        self.scale = QueueRegistrationScale::Sharded(shard_count.max(1));
        self
    }

    /// Adaptive shard expansion ([`AutoShardPolicy::default`] when policy omitted).
    #[must_use]
    pub fn auto_shard(mut self) -> Self {
        self.scale = QueueRegistrationScale::AutoShard(AutoShardPolicy::default());
        self
    }

    /// Adaptive shard expansion with explicit thresholds.
    #[must_use]
    pub fn auto_shard_policy(mut self, policy: AutoShardPolicy) -> Self {
        self.scale = QueueRegistrationScale::AutoShard(policy);
        self
    }

    /// Attempt ceiling for enqueues that leave `max_attempts` unset (`0` = unlimited).
    ///
    /// A job that sets its own ceiling always wins; this only fills in the gap for
    /// HTTP enqueues, cron ticks, and plain [`enqueue`](trembita_jobs::JobQueue::enqueue).
    #[must_use]
    pub fn default_max_attempts(mut self, max: u32) -> Self {
        self.default_max_attempts = max;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// B-32 — product queue registration scale helpers.
    #[test]
    fn b32_queue_registration_scale_scenarios_table() {
        let lease = Duration::from_secs(30);
        struct Row {
            label: &'static str,
            opts: QueueOpts,
            check: fn(&QueueRegistrationScale) -> bool,
        }
        let rows = [
            Row {
                label: "standard",
                opts: QueueOpts::new("work", lease),
                check: |s| matches!(s, QueueRegistrationScale::Standard),
            },
            Row {
                label: "sharded clamps zero to one",
                opts: QueueOpts::new("work", lease).sharded(0),
                check: |s| matches!(s, QueueRegistrationScale::Sharded(1)),
            },
            Row {
                label: "auto shard default policy",
                opts: QueueOpts::new("work", lease).auto_shard(),
                check: |s| matches!(s, QueueRegistrationScale::AutoShard(_)),
            },
        ];
        for row in rows {
            assert!((row.check)(&row.opts.scale), "{}", row.label);
        }
    }
}
