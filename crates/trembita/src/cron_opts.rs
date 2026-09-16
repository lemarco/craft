//! Cron schedule registration for [`TrembitaAppBuilder`](super::app::TrembitaAppBuilder).

use trembita_jobs::RecurringJob;

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

    /// Cron on `stream` enqueues a workflow bootstrap ([`RecurringJob::trigger_workflow`]).
    #[must_use]
    pub fn starts_workflow(
        stream: impl Into<String>,
        schedule_name: impl Into<String>,
        cron: impl Into<String>,
        saga_id: impl Into<String>,
    ) -> Self {
        Self::new(
            stream,
            RecurringJob::trigger_workflow(schedule_name, cron, saga_id),
        )
    }

    /// Cron on `stream` enqueues a pipeline bootstrap ([`RecurringJob::trigger_enqueue`]).
    #[must_use]
    pub fn starts_enqueue(
        stream: impl Into<String>,
        schedule_name: impl Into<String>,
        cron: impl Into<String>,
        follow_up_stream: impl Into<String>,
        follow_up_payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self::new(
            stream,
            RecurringJob::trigger_enqueue(schedule_name, cron, follow_up_stream, follow_up_payload),
        )
    }

    /// Calendar interval on `stream` that starts a workflow each tick.
    ///
    /// # Errors
    /// Returns [`trembita_jobs::QueueError::Codec`] when `hour`/`minute` are invalid.
    pub fn starts_workflow_every_calendar_days_at_utc(
        stream: impl Into<String>,
        schedule_name: impl Into<String>,
        every_days: u32,
        hour: u32,
        minute: u32,
        saga_id: impl Into<String>,
    ) -> Result<Self, trembita_jobs::QueueError> {
        Ok(Self::new(
            stream,
            RecurringJob::trigger_workflow_every_calendar_days_at_utc(
                schedule_name,
                every_days,
                hour,
                minute,
                saga_id,
            )?,
        ))
    }
}
