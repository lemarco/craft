//! Declarative product capability registry for [`TrembitaAppBuilder`](super::TrembitaAppBuilder).

use crate::job_opts::JobOpts;
use crate::topic_opts::TopicOpts;
use crate::worker_opts::WorkerGroup;
use crate::workflow_opts::WorkflowOpts;

use super::TrembitaAppBuilder;
use super::run_hint::ManifestRunHint;

pub use super::manifest_presets::{JobsPreset, RealtimePreset, TopicsPreset};

/// Registered jobs, topics, workers, and workflows — one place to wire product capabilities.
///
/// Scaffolded apps define this in `src/manifest.rs`; [`TrembitaAppBuilder::manifest`] applies it.
/// Scaffolded apps use `// trembita:*` marker comments in `manifest.rs` as edit guides.
///
/// ```
/// # use std::time::Duration;
/// # use trembita::{AppManifest, JobOpts, TrembitaApp, RunOpts};
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let manifest = AppManifest::new()
///     .jobs([JobOpts::new("jobs", Duration::from_secs(300)).http_enqueue(true)]);
/// TrembitaApp::builder()
///     .data_dir("/tmp/app")
///     .manifest(manifest)
///     .run(RunOpts::default())
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Default)]
pub struct AppManifest {
    jobs: Vec<JobOpts>,
    topics: Vec<TopicOpts>,
    workers: Option<WorkerGroup>,
    workflows: Vec<WorkflowOpts>,
}

impl AppManifest {
    /// Empty registry — chain [`.jobs`](Self::jobs), [`.topics`](Self::topics), …
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

    /// Durable event topics (`POST /topics/{name}/publish` on the default gateway).
    #[must_use]
    pub fn topics(mut self, topics: impl IntoIterator<Item = TopicOpts>) -> Self {
        self.topics.extend(topics);
        self
    }

    /// Stateful worker actor groups ([`WorkerGroup`] / [`workers!`](crate::workers)).
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

    /// Apply all registrations to `builder` (same effect as calling `.jobs` / `.topics` / … separately).
    #[must_use]
    pub fn apply(self, builder: TrembitaAppBuilder) -> TrembitaAppBuilder {
        let mut builder = builder;
        if !self.jobs.is_empty() {
            builder = builder.jobs(self.jobs);
        }
        if !self.topics.is_empty() {
            builder = builder.topics(self.topics);
        }
        if let Some(workers) = self.workers {
            builder = builder.workers(workers);
        }
        if !self.workflows.is_empty() {
            builder = builder.workflows(self.workflows);
        }
        builder.with_run_hint(self.run_hint())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::TrembitaApp;

    #[test]
    fn manifest_chains_into_builder() {
        let manifest = AppManifest::new().jobs([JobOpts::new("jobs")
            .lease(Duration::from_secs(60))
            .http_enqueue(true)]);
        let _builder = TrembitaApp::builder()
            .data_dir("/tmp/manifest-test")
            .manifest(manifest);
    }

    #[test]
    fn job_stream_names_follows_manifest_jobs() {
        let manifest = AppManifest::new().jobs([JobOpts::new("a"), JobOpts::new("b")]);
        assert_eq!(manifest.job_stream_names(), vec!["a", "b"]);
    }
}
