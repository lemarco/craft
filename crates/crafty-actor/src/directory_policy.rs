//! Directory visibility policy for actor routing (future-work R3 mitigation).

use std::time::Duration;

/// How [`ClusterMessaging`](crate::ClusterMessaging) treats directory lag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DirectoryPolicy {
    /// Default: pick from the current merged view; `NoTarget` if not yet visible.
    #[default]
    Eventual,
    /// Read-your-writes: retry briefly when the directory has no target yet
    /// (e.g. immediately after spawn/scale before anti-entropy converges).
    ReadYourWrites,
}

/// Retry budget when [`DirectoryPolicy::ReadYourWrites`] is active.
#[derive(Debug, Clone, Copy)]
pub struct DirectoryRetry {
    /// Total pick attempts before returning `NoTarget`.
    pub max_attempts: u32,
    /// Delay between attempts.
    pub backoff: Duration,
}

impl Default for DirectoryRetry {
    fn default() -> Self {
        Self {
            max_attempts: 8,
            backoff: Duration::from_millis(25),
        }
    }
}
