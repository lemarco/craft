//! Job queue registration options for [`TrembitaAppBuilder`](super::app::TrembitaAppBuilder).

use std::time::Duration;

use trembita_actor::DEFAULT_QUEUE_PREFETCH;

/// One durable job stream for [`.queue`](super::app::TrembitaAppBuilder::queue).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueOpts {
    /// Stream name (`queue-{name}.redb` under `data_dir`).
    pub name: String,
    /// Lease timeout for workers holding jobs from this stream.
    pub lease: Duration,
    /// Leader prefetch depth (`0` = disable). Default: [`DEFAULT_QUEUE_PREFETCH`].
    pub prefetch: usize,
    /// Default delivery-attempt ceiling for jobs that do not set their own (`0` = unlimited).
    pub default_max_attempts: u32,
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
        }
    }

    /// Attempt ceiling for enqueues that leave `max_attempts` unset (`0` = unlimited).
    ///
    /// A job that sets its own ceiling always wins; this only fills in the gap for
    /// HTTP enqueues, cron ticks, and plain [`enqueue`](trembita_actor::JobQueue::enqueue).
    #[must_use]
    pub fn default_max_attempts(mut self, max: u32) -> Self {
        self.default_max_attempts = max;
        self
    }
}
