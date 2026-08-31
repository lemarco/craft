//! Cron schedule registration for [`CraftyAppBuilder`](super::app::CraftyAppBuilder).

use crafty_actor::RecurringJob;

/// One cron-driven enqueue schedule for [`.cron`](super::app::CraftyAppBuilder::cron).
///
/// Requires a matching stream from [`.queue`](super::app::CraftyAppBuilder::queue) on the same builder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronOpts {
    /// Job stream name (must match [`.queue`](super::app::CraftyAppBuilder::queue)).
    pub stream: String,
    /// Schedule definition (cron expression, payload, …).
    pub job: RecurringJob,
}

impl CronOpts {
    /// Register `job` on `stream`.
    #[must_use]
    pub fn new(stream: impl Into<String>, job: RecurringJob) -> Self {
        Self {
            stream: stream.into(),
            job,
        }
    }
}
