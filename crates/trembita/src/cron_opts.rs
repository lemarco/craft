//! Cron schedule registration for [`TrembitaAppBuilder`](super::app::TrembitaAppBuilder).

use trembita_actor::RecurringJob;

/// One cron-driven enqueue schedule for [`.cron`](super::app::TrembitaAppBuilder::cron).
///
/// Requires a matching stream from [`.queue`](super::app::TrembitaAppBuilder::queue) on the same builder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronOpts {
    /// Job stream name (must match [`.queue`](super::app::TrembitaAppBuilder::queue)).
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
