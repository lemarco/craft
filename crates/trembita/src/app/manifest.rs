//! Declarative product capability registry for [`TrembitaAppBuilder`](super::TrembitaAppBuilder).

use std::sync::Arc;

use crate::capability::CapManifest;
use crate::cron_opts::CronOpts;
use crate::job_opts::JobOpts;
use crate::queue_opts::QueueOpts;
use crate::scheduled_workflow_opts::ScheduledWorkflowOpts;
use crate::topic_opts::TopicOpts;
use crate::worker_opts::WorkerGroup;
use crate::workflow_opts::WorkflowOpts;

use super::TrembitaAppBuilder;
use super::run_hint::ManifestRunHint;

pub use super::manifest_presets::{JobsPreset, RealtimePreset, TopicsPreset};

/// Schedule source wiring ([`AppManifest::schedule_source`]).
pub struct ScheduleSourceOpts {
    /// Job stream name (must match [`.queue`](AppManifest::queue) or [`.jobs`](AppManifest::jobs)).
    pub stream: String,
    /// External schedule catalog.
    pub source: Arc<dyn trembita_jobs::ScheduleSource>,
    /// Leader poll interval.
    pub poll: trembita_jobs::SchedulePoll,
}

/// Registered jobs, topics, workers, workflows, and capabilities — apply via [`.manifest`](super::TrembitaAppBuilder::manifest).
///
/// Scaffolded apps define this in `src/manifest.rs`. Product code should not call
/// `.jobs()` / `.queue()` on [`TrembitaAppBuilder`] directly.
///
/// ```
/// # use std::time::Duration;
/// # use trembita::{AppManifest, JobOpts, TrembitaApp, TrembitaConfigure};
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let manifest = AppManifest::new()
///     .jobs([JobOpts::new("jobs").lease(Duration::from_secs(300))]);
/// TrembitaApp::builder()
///     .manifest(manifest)
///     .configure(TrembitaConfigure::default().with_data_dir("/tmp/app"))
///     .run()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Default)]
pub struct AppManifest {
    jobs: Vec<JobOpts>,
    extra_queues: Vec<QueueOpts>,
    crons: Vec<CronOpts>,
    scheduled_workflows: Option<ScheduledWorkflowOpts>,
    schedule_sources: Vec<ScheduleSourceOpts>,
    topics: Vec<TopicOpts>,
    workers: Option<WorkerGroup>,
    workflows: Vec<WorkflowOpts>,
    capabilities: Option<CapManifest>,
    consumer_groups: Vec<crate::ConsumerGroup>,
}

impl AppManifest {
    /// Empty registry — chain [`.jobs`](Self::jobs), [`.capabilities`](Self::capabilities), …
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Durable job streams (queue + optional consumer + HTTP enqueue).
    #[must_use]
    pub fn jobs(mut self, jobs: impl IntoIterator<Item = JobOpts>) -> Self {
        self.jobs.extend(jobs);
        self
    }

    /// Queue streams without [`JobOpts`] (advanced / tests).
    #[must_use]
    pub fn queue(mut self, queues: impl IntoIterator<Item = QueueOpts>) -> Self {
        self.extra_queues.extend(queues);
        self
    }

    /// Cron recurring enqueues (requires matching [`.queue`](Self::queue) or [`.jobs`](Self::jobs)).
    #[must_use]
    pub fn cron(mut self, schedules: impl IntoIterator<Item = CronOpts>) -> Self {
        self.crons.extend(schedules);
        self
    }

    /// Cron → workflow/pipeline helper (queue + cron + built-in consumer).
    #[must_use]
    pub fn scheduled_workflows(mut self, spec: ScheduledWorkflowOpts) -> Self {
        self.scheduled_workflows = Some(spec);
        self
    }

    /// External schedule catalog on the queue leader.
    #[must_use]
    pub fn schedule_source(mut self, opts: ScheduleSourceOpts) -> Self {
        self.schedule_sources.push(opts);
        self
    }

    /// Durable event topics.
    #[must_use]
    pub fn topics(mut self, topics: impl IntoIterator<Item = TopicOpts>) -> Self {
        self.topics.extend(topics);
        self
    }

    /// Job consumers not covered by [`JobOpts`](JobOpts).
    #[must_use]
    pub fn consumers(mut self, group: crate::ConsumerGroup) -> Self {
        self.consumer_groups.push(group);
        self
    }

    /// Single [`JobConsumer`](crate::JobConsumer) loop.
    #[must_use]
    pub fn consumer<C: crate::JobConsumer>(
        mut self,
        consumer: C,
        opts: crate::ConsumerOpts,
    ) -> Self {
        self.consumer_groups
            .push(crate::ConsumerGroup::new().add(consumer, opts));
        self
    }

    /// **Advanced:** legacy [`UserActor`](trembita_runtime::UserActor) worker groups ([`WorkerGroup`] /
    /// [`workers!`](crate::workers)). Product stateful logic belongs in [`.capabilities`](Self::capabilities).
    #[must_use]
    pub fn workers(mut self, group: WorkerGroup) -> Self {
        self.workers = Some(group);
        self
    }

    /// Saga workflows for HTTP `/workflows/*`.
    #[must_use]
    pub fn workflows(mut self, specs: impl IntoIterator<Item = WorkflowOpts>) -> Self {
        self.workflows.extend(specs);
        self
    }

    /// Typed capability groups ([`CapManifest`](crate::capability::CapManifest)).
    #[must_use]
    pub fn capabilities(mut self, caps: CapManifest) -> Self {
        self.capabilities = Some(caps);
        self
    }

    /// Durable job stream names from [`.jobs`](Self::jobs) (manifest order).
    #[must_use]
    pub fn job_stream_names(&self) -> Vec<&str> {
        self.jobs.iter().map(|j| j.stream_name()).collect()
    }

    /// Whether [`.workers`](Self::workers) was called.
    #[must_use]
    pub fn has_workers(&self) -> bool {
        self.workers.is_some()
    }

    fn run_hint(&self) -> ManifestRunHint {
        let mut hint = ManifestRunHint::default();
        hint.apply_manifest(self.job_stream_names(), self.has_workers());
        hint
    }

    /// Apply all registrations to `builder`.
    #[must_use]
    pub fn apply(self, builder: TrembitaAppBuilder) -> TrembitaAppBuilder {
        let hint = self.run_hint();
        let mut builder = builder;
        if let Some(caps) = self.capabilities {
            builder = builder.capabilities(caps);
        }
        if !self.extra_queues.is_empty() {
            builder = builder.queue(self.extra_queues);
        }
        if !self.jobs.is_empty() {
            builder = builder.jobs(self.jobs);
        }
        if !self.topics.is_empty() {
            builder = builder.topics(self.topics);
        }
        if !self.crons.is_empty() {
            builder = builder.cron(self.crons);
        }
        if let Some(spec) = self.scheduled_workflows {
            builder = builder.scheduled_workflows(spec);
        }
        for src in self.schedule_sources {
            builder = builder.schedule_source(src.stream, src.source, src.poll);
        }
        for group in self.consumer_groups {
            builder = builder.consumers(group);
        }
        if let Some(workers) = self.workers {
            builder = builder.workers(workers);
        }
        if !self.workflows.is_empty() {
            builder = builder.workflows(self.workflows);
        }
        builder.with_run_hint(hint)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{TrembitaApp, TrembitaConfigure};

    #[test]
    fn manifest_chains_into_builder() {
        let manifest = AppManifest::new().jobs([JobOpts::new("jobs")
            .lease(Duration::from_secs(60))
            .http_enqueue(true)]);
        let _builder = TrembitaApp::builder()
            .configure(
                TrembitaConfigure::default()
                    .with_local_gateway_apis()
                    .with_data_dir("/tmp/manifest-test"),
            )
            .manifest(manifest);
    }

    #[test]
    fn job_stream_names_follows_manifest_jobs() {
        let manifest = AppManifest::new().jobs([JobOpts::new("a"), JobOpts::new("b")]);
        assert_eq!(manifest.job_stream_names(), vec!["a", "b"]);
    }

    /// B-41 — manifest can register [`ScheduleSourceOpts`] before boot.
    #[test]
    fn b41_manifest_schedule_source_chains_into_builder() {
        use trembita_jobs::{RecurringJob, StaticScheduleSource};

        let manifest = AppManifest::new()
            .queue([QueueOpts::new("jobs", Duration::from_secs(30))])
            .schedule_source(ScheduleSourceOpts {
                stream: "jobs".into(),
                source: Arc::new(StaticScheduleSource::new(vec![RecurringJob::new(
                    "tick",
                    "0 * * * *",
                    b"x",
                )])),
                poll: trembita_jobs::SchedulePoll::secs(5),
            });
        let _builder = TrembitaApp::builder()
            .configure(
                TrembitaConfigure::default()
                    .with_data_dir("/tmp/b41-sched-manifest")
                    .with_local_gateway_apis(),
            )
            .manifest(manifest);
    }
}
