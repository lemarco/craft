//! Job queue registration options for [`CraftyAppBuilder`](super::app::CraftyAppBuilder).

use std::time::Duration;

use crafty_actor::DEFAULT_QUEUE_PREFETCH;

/// One durable job stream for [`.queue`](super::app::CraftyAppBuilder::queue).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueOpts {
    /// Stream name (`queue-{name}.redb` under `data_dir`).
    pub name: String,
    /// Lease timeout for workers holding jobs from this stream.
    pub lease: Duration,
    /// Leader prefetch depth (`0` = disable). Default: [`DEFAULT_QUEUE_PREFETCH`].
    pub prefetch: usize,
}

impl QueueOpts {
    /// Register a queue with framework default prefetch.
    #[must_use]
    pub fn new(name: impl Into<String>, lease: Duration) -> Self {
        Self {
            name: name.into(),
            lease,
            prefetch: DEFAULT_QUEUE_PREFETCH,
        }
    }
}
