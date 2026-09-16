//! Scenario-oriented helpers for [`AppManifest`] and [`JobOpts`](crate::JobOpts).

use std::sync::Arc;

use crate::consumer::{IdempotencyOpts, JobConsumer};
use crate::job_opts::JobOpts;
use crate::topic_opts::TopicOpts;
use crate::worker_opts::{WorkerGroup, WorkerOpts, WorkerScale};
use trembita_runtime::UserActor;

use super::AppManifest;

/// Background job stream defaults (lease, retries, HTTP enqueue when `http-jobs` is enabled).
pub struct JobsPreset;

impl JobsPreset {
    /// Durable queue + handler with product defaults.
    #[must_use]
    pub fn stream<C>(name: impl Into<String>, consumer: &C) -> JobOpts
    where
        C: JobConsumer + Clone + 'static,
    {
        JobOpts::product(name, consumer)
    }

    /// Same as [`Self::stream`] with idempotency keyed by enqueue dedup.
    #[must_use]
    pub fn idempotent_stream<C>(
        name: impl Into<String>,
        consumer: &C,
        store: Arc<dyn trembita_actor_store::ActorStateStore>,
        key_prefix: impl Into<String>,
    ) -> JobOpts
    where
        C: JobConsumer + Clone + 'static,
    {
        JobOpts::product(name, consumer)
            .idempotency(IdempotencyOpts::by_dedup_key(store, key_prefix))
    }
}

/// Realtime / worker-heavy manifests.
pub struct RealtimePreset;

impl RealtimePreset {
    /// One **legacy** [`UserActor`](trembita_runtime::UserActor) group per cluster node.
    ///
    /// Product realtime apps should use [`CapGroup::per_node`](crate::capability::CapGroup::per_node)
    /// in [`CapManifest`](crate::capability::CapManifest) instead.
    #[must_use]
    pub fn worker_per_node<W>(name: impl Into<String>, config: W::Config) -> WorkerGroup
    where
        W: UserActor + 'static,
        W::Config: Clone + Send + Sync + 'static,
    {
        WorkerGroup::new().with_worker(
            WorkerOpts::<W>::new(name)
                .config(config)
                .scale(WorkerScale::PerNode),
        )
    }
}

/// Event topic registration shorthand.
pub struct TopicsPreset;

impl TopicsPreset {
    /// Durable topic with default lease/retention.
    #[must_use]
    pub fn topic(name: impl Into<String>) -> TopicOpts {
        TopicOpts::topic(name)
    }
}

impl AppManifest {
    /// Register one job stream using [`JobsPreset::stream`].
    #[must_use]
    pub fn job(self, job: JobOpts) -> Self {
        self.jobs([job])
    }

    /// Register one topic using [`TopicsPreset::topic`].
    #[must_use]
    pub fn topic(self, topic: TopicOpts) -> Self {
        self.topics([topic])
    }
}
