//! One declaration for **cron → queue → workflow/pipeline** (built-in consumer).
//!
//! Replaces manual `.queue("orchestration")` + `.cron([CronOpts::starts_workflow…])` +
//! a custom `#[consumer]` that calls [`dispatch_work_trigger`](crate::work_trigger::dispatch_work_trigger).

use std::time::Duration;

use trembita_jobs::DEFAULT_QUEUE_PREFETCH;

use crate::consumer::{ConsumerOpts, ConsumerSpawnFn};
use crate::cron_opts::CronOpts;
use crate::queue_opts::QueueOpts;

/// Cron schedules that enqueue [`WorkTrigger`](trembita_jobs::WorkTrigger) jobs and run them
/// on a dedicated stream (default `"orchestration"`).
///
/// ```
/// # use std::time::Duration;
/// # use trembita::{AppManifest, ScheduledWorkflowOpts, TrembitaApp, TrembitaConfigure};
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// TrembitaApp::builder()
///     .configure(TrembitaConfigure::default().with_data_dir("/tmp/app"))
///     .manifest(AppManifest::new().scheduled_workflows(
///         ScheduledWorkflowOpts::new()
///             .workflow("weekly", "0 3 * * 1", "weekly-report"),
///     ))
///     .run()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ScheduledWorkflowOpts {
    stream: String,
    lease: Duration,
    prefetch: usize,
    default_max_attempts: u32,
    instances: u32,
    batch: usize,
    idle_sleep: Duration,
    crons: Vec<CronOpts>,
    config_error: Option<String>,
}

impl ScheduledWorkflowOpts {
    /// Default stream `orchestration`, 300s lease, one consumer instance.
    #[must_use]
    pub fn new() -> Self {
        Self {
            stream: "orchestration".into(),
            lease: Duration::from_secs(300),
            prefetch: DEFAULT_QUEUE_PREFETCH,
            default_max_attempts: 0,
            instances: 1,
            batch: 1,
            idle_sleep: Duration::from_millis(100),
            crons: Vec::new(),
            config_error: None,
        }
    }

    /// Job stream for cron ticks and the built-in dispatcher (default `orchestration`).
    #[must_use]
    pub fn stream(mut self, stream: impl Into<String>) -> Self {
        self.stream = stream.into();
        self
    }

    /// Lease timeout for bootstrap jobs on this stream.
    #[must_use]
    pub fn lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// Consumer instances on each cluster node.
    #[must_use]
    pub fn instances(mut self, count: u32) -> Self {
        self.instances = count.max(1);
        self
    }

    /// Cron expression → enqueue workflow bootstrap → [`TrembitaApp::run_workflow_id`].
    #[must_use]
    pub fn workflow(
        mut self,
        schedule_name: impl Into<String>,
        cron: impl Into<String>,
        saga_id: impl Into<String>,
    ) -> Self {
        let stream = self.stream.clone();
        self.crons.push(CronOpts::starts_workflow(
            stream,
            schedule_name,
            cron,
            saga_id,
        ));
        self
    }

    /// Cron expression → enqueue follow-up job on another stream.
    #[must_use]
    pub fn pipeline(
        mut self,
        schedule_name: impl Into<String>,
        cron: impl Into<String>,
        follow_up_stream: impl Into<String>,
        follow_up_payload: impl Into<Vec<u8>>,
    ) -> Self {
        let stream = self.stream.clone();
        self.crons.push(CronOpts::starts_enqueue(
            stream,
            schedule_name,
            cron,
            follow_up_stream,
            follow_up_payload,
        ));
        self
    }

    /// Calendar interval (UTC wall clock) → workflow bootstrap.
    #[must_use]
    pub fn workflow_every_calendar_days_at_utc(
        mut self,
        schedule_name: impl Into<String>,
        every_days: u32,
        hour: u32,
        minute: u32,
        saga_id: impl Into<String>,
    ) -> Self {
        let stream = self.stream.clone();
        match CronOpts::starts_workflow_every_calendar_days_at_utc(
            stream,
            schedule_name,
            every_days,
            hour,
            minute,
            saga_id,
        ) {
            Ok(cron) => self.crons.push(cron),
            Err(e) => {
                self.config_error = Some(e.to_string());
            }
        }
        self
    }

    pub(crate) fn into_registration(self) -> ScheduledWorkflowRegistration {
        let stream = self.stream.clone();
        let mut spawners: Vec<ConsumerSpawnFn> = Vec::new();
        for instance in 0..self.instances {
            let stream = stream.clone();
            let batch = self.batch;
            let idle_sleep = self.idle_sleep;
            spawners.push(Box::new(move |app, stop| {
                app.spawn_work_trigger_consumer(
                    &stream,
                    ConsumerOpts::default()
                        .instance(instance)
                        .batch(batch)
                        .idle_sleep(idle_sleep),
                    stop,
                )
            }));
        }
        ScheduledWorkflowRegistration {
            queue: QueueOpts {
                name: self.stream.clone(),
                lease: self.lease,
                prefetch: self.prefetch,
                default_max_attempts: self.default_max_attempts,
            },
            stream: self.stream,
            crons: self.crons,
            spawners,
            config_error: self.config_error,
        }
    }
}

impl Default for ScheduledWorkflowOpts {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) struct ScheduledWorkflowRegistration {
    pub queue: QueueOpts,
    pub stream: String,
    pub crons: Vec<CronOpts>,
    pub spawners: Vec<ConsumerSpawnFn>,
    pub config_error: Option<String>,
}
