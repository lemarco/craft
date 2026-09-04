//! Product API builder methods (queues, topics, workload, autoscale).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use trembita_events::{
    EventOutboxDrainOpts, EventOutboxPoll, EventOutboxSource, TopicRetentionOpts,
    TopicSubscriptionDef,
};
use trembita_jobs::{
    AutoscalePolicy, BacklogFeedOpts, DEFAULT_QUEUE_PREFETCH, ExternalBacklog,
    MembershipAutoscalePolicy, RecurringJob, SchedulePoll, ScheduleSource, WorkloadOpts,
    run_queue_autoscaler, run_queue_membership_autoscaler,
};
use trembita_runtime::{ClusterControl, ClusterSupervisor, UserActor};

use crate::builder::autoscale::upsert_queue_autoscale_meta;
use crate::cluster_handle::ClusterFacts;

use super::TrembitaClusterBuilder;
use super::types::{
    BacklogFeedSpec, EventOutboxFeedSpec, JobStreamSpec, RecurringJobSpec, ScheduleSourceSpec,
    ShardedJobSpec, TopicStreamSpec,
};

impl<M: trembita_core::StateMachine + Default + 'static> TrembitaClusterBuilder<M> {
    /// Enable a durable job queue stream at `{data_dir}/queue-{name}.redb`
    /// ([job-queue](../../docs/decisions/job-queue.md)). Requires [`data_dir`](Self::data_dir).
    #[must_use]
    pub fn job_queue(mut self, name: &str, lease_timeout: Duration) -> Self {
        self.job_streams.push(JobStreamSpec {
            name: name.to_string(),
            path: None,
            lease_timeout,
            prefetch: DEFAULT_QUEUE_PREFETCH,
            default_max_attempts: 0,
        });
        self
    }

    /// Like [`job_queue`](Self::job_queue) but opens an explicit redb path (tests, custom layout).
    #[must_use]
    pub fn job_queue_at(
        mut self,
        name: &str,
        path: impl Into<PathBuf>,
        lease_timeout: Duration,
    ) -> Self {
        self.job_streams.push(JobStreamSpec {
            name: name.to_string(),
            path: Some(path.into()),
            lease_timeout,
            prefetch: DEFAULT_QUEUE_PREFETCH,
            default_max_attempts: 0,
        });
        self
    }

    /// Tune leader prefetch depth for `stream` (default [`DEFAULT_QUEUE_PREFETCH`]).
    ///
    /// Prefetch keeps recently enqueued payloads in RAM on the queue leader so
    /// [`lease`](trembita_jobs::JobQueue::lease) skips re-reading from `redb`.
    /// Set `prefetch` to `0` to disable.
    #[must_use]
    pub fn job_queue_prefetch(mut self, stream: &str, prefetch: usize) -> Self {
        for spec in &mut self.job_streams {
            if spec.name == stream {
                spec.prefetch = prefetch;
            }
        }
        self
    }

    /// Default delivery-attempt ceiling for `stream` (`0` = unlimited retries).
    ///
    /// Applies to every enqueue that leaves
    /// [`EnqueueOptions::max_attempts`](trembita_jobs::EnqueueOptions::max_attempts)
    /// unset — including HTTP `POST /jobs/{stream}` and cron schedules. An
    /// explicit per-job ceiling always wins.
    #[must_use]
    pub fn job_queue_max_attempts(mut self, stream: &str, max_attempts: u32) -> Self {
        for spec in &mut self.job_streams {
            if spec.name == stream {
                spec.default_max_attempts = max_attempts;
            }
        }
        self
    }

    /// Enable a durable event topic at `{data_dir}/topic-{name}.redb`
    /// ([event-topics](../../docs/decisions/event-topics.md)). Requires [`data_dir`](Self::data_dir).
    #[must_use]
    pub fn event_topic(mut self, name: &str, lease_timeout: Duration) -> Self {
        self.topic_streams.push(TopicStreamSpec {
            name: name.to_string(),
            path: None,
            lease_timeout,
            retention: TopicRetentionOpts::default(),
            subscriptions: Vec::new(),
        });
        self
    }

    /// Like [`event_topic`](Self::event_topic) but opens an explicit redb path.
    #[must_use]
    pub fn event_topic_at(
        mut self,
        name: &str,
        path: impl Into<PathBuf>,
        lease_timeout: Duration,
    ) -> Self {
        self.topic_streams.push(TopicStreamSpec {
            name: name.to_string(),
            path: Some(path.into()),
            lease_timeout,
            retention: TopicRetentionOpts::default(),
            subscriptions: Vec::new(),
        });
        self
    }

    /// Declare subscriptions and retention for a registered topic.
    #[must_use]
    pub fn event_topic_subscriptions(
        mut self,
        name: &str,
        subscriptions: &[TopicSubscriptionDef],
    ) -> Self {
        for spec in &mut self.topic_streams {
            if spec.name == name {
                spec.subscriptions = subscriptions.to_vec();
            }
        }
        self
    }

    /// Retention thresholds for a registered topic.
    #[must_use]
    pub fn event_topic_retention(mut self, name: &str, retention: TopicRetentionOpts) -> Self {
        for spec in &mut self.topic_streams {
            if spec.name == name {
                spec.retention = retention;
            }
        }
        self
    }

    /// Leader-fed stream backed by an [`ExternalBacklog`] ([external-backlog](../../docs/decisions/external-backlog.md)).
    ///
    /// Requires [`job_queue`](Self::job_queue) on the same `stream`. The leader claims from
    /// `backlog`, enqueues into the job queue with `dedup_key = item.key`, and calls
    /// [`ExternalBacklog::settle`] on terminal ack/dead-letter outcomes.
    #[must_use]
    pub fn job_queue_external_backlog(
        mut self,
        stream: &str,
        backlog: Arc<dyn ExternalBacklog>,
        opts: BacklogFeedOpts,
    ) -> Self {
        self.backlog_feeds.push(BacklogFeedSpec {
            stream: stream.to_string(),
            backlog,
            opts,
        });
        self
    }

    /// Per-node workload governor — compute tokens arbitrate gateway vs job handlers
    /// ([workload-governor](../../docs/decisions/workload-governor.md)).
    #[must_use]
    pub fn workload(mut self, opts: WorkloadOpts) -> Self {
        self.workload = Some(opts);
        self
    }

    /// Register a dynamic [`ScheduleSource`] for recurring jobs on `stream`
    /// ([schedule-source](../../docs/decisions/schedule-source.md)).
    ///
    /// Requires [`job_queue`](Self::job_queue) on the same stream. Pairs with
    /// [`.cron()`](crate::TrembitaAppBuilder::cron) — static and external sources
    /// are merged.
    #[must_use]
    pub fn schedule_source(
        mut self,
        stream: &str,
        source: Arc<dyn ScheduleSource>,
        poll: SchedulePoll,
    ) -> Self {
        self.schedule_sources.push(ScheduleSourceSpec {
            stream: stream.to_string(),
            source,
            poll: poll.duration(),
        });
        self
    }

    /// Register a transactional outbox drainer for `topic`
    /// ([event-outbox](../../docs/decisions/event-outbox.md)).
    ///
    /// Requires [`event_topic`](Self::event_topic) on the same name. The leader polls
    /// [`EventOutboxSource::poll`], publishes to the topic, then
    /// [`EventOutboxSource::mark_published`].
    #[must_use]
    pub fn event_outbox_source(
        mut self,
        topic: &str,
        source: Arc<dyn EventOutboxSource>,
        poll: EventOutboxPoll,
    ) -> Self {
        self.event_outbox_feeds.push(EventOutboxFeedSpec {
            topic: topic.to_string(),
            source,
            opts: EventOutboxDrainOpts::default().poll(poll.duration()),
        });
        self
    }

    /// Like [`event_outbox_source`](Self::event_outbox_source) with explicit drain tunables.
    #[must_use]
    pub fn event_outbox_source_with_opts(
        mut self,
        topic: &str,
        source: Arc<dyn EventOutboxSource>,
        opts: EventOutboxDrainOpts,
    ) -> Self {
        self.event_outbox_feeds.push(EventOutboxFeedSpec {
            topic: topic.to_string(),
            source,
            opts,
        });
        self
    }

    /// Register a cron-driven recurring job on `stream` ([`RecurringJob`]).
    ///
    /// Requires [`job_queue`](Self::job_queue) on the same stream. Schedules persist in
    /// `queue-{stream}.redb` and fire on the queue leader.
    #[must_use]
    pub fn recurring_job(mut self, stream: &str, job: RecurringJob) -> Self {
        self.recurring_jobs.push(RecurringJobSpec {
            stream: stream.to_string(),
            job,
        });
        self
    }

    /// Leader-only autoscale loop for `stream` depth → `policy.worker_group` count.
    /// Registers `A` on the control plane; pair with [`manage`](Self::manage) or
    /// [`manage_auto`](Self::manage_auto) for the same group name.
    ///
    /// # Panics
    /// If `stream` was not registered via [`job_queue`](Self::job_queue).
    #[must_use]
    pub fn job_queue_autoscale<A: UserActor>(
        mut self,
        stream: &str,
        policy: &AutoscalePolicy,
        config: A::Config,
    ) -> Self
    where
        A::Config: Clone + Send + Sync + 'static,
    {
        let stream = stream.to_string();
        let worker_group = policy.worker_group.clone();
        let policy = policy.clone();
        upsert_queue_autoscale_meta(
            &mut self.queue_autoscale_meta,
            &stream,
            Some(policy.to_wire()),
            None,
        );
        self.registrations
            .push(Box::new(|control: &ClusterControl| {
                control.register_type::<A>();
            }));
        self.job_autoscale.push(Box::new(
            move |control, state, directory, queues, registry, backlog_registry| {
                let Some(queue) = queues.get(&stream).cloned() else {
                    panic!(
                        "job_queue_autoscale stream {stream:?} was not registered via job_queue"
                    );
                };
                let backlog = backlog_registry.get(&stream);
                let policy = policy.clone();
                let config = config.clone();
                let worker_group = worker_group.clone();
                let stream = stream.clone();
                tokio::spawn(async move {
                    run_queue_autoscaler(
                        queue,
                        directory,
                        Arc::clone(&state),
                        registry,
                        stream,
                        policy,
                        backlog,
                        move |desired| {
                            let control = Arc::clone(&control);
                            let state = Arc::clone(&state);
                            let config = config.clone();
                            let worker_group = worker_group.clone();
                            async move {
                                control
                                    .scale_cluster::<A>(
                                        &worker_group,
                                        desired,
                                        config,
                                        &state.reachable_nodes(),
                                    )
                                    .await
                                    .map(|_| ())
                            }
                        },
                    )
                    .await;
                })
            },
        ));
        self
    }

    /// Federated queue over `shard_count` independent redb streams (`{name}~0` …)
    /// to spread leader replication load ([job-queue](../../docs/decisions/job-queue.md)).
    ///
    /// # Panics
    /// If `shard_count` is zero.
    #[must_use]
    pub fn job_queue_sharded(
        mut self,
        name: &str,
        shard_count: usize,
        lease_timeout: Duration,
    ) -> Self {
        assert!(
            shard_count >= 1,
            "job_queue_sharded requires shard_count >= 1"
        );
        for i in 0..shard_count {
            self.job_streams.push(JobStreamSpec {
                name: format!("{name}~{i}"),
                path: None,
                lease_timeout,
                prefetch: DEFAULT_QUEUE_PREFETCH,
                default_max_attempts: 0,
            });
        }
        self.job_sharded.push(ShardedJobSpec {
            name: name.to_string(),
            shard_count,
        });
        self
    }

    /// Leader-only loop: when queue depth per live node exceeds a threshold, call
    /// `join` to add a VPS (production scale-out beyond worker autoscale).
    ///
    /// # Panics
    /// If `stream` was not registered via [`job_queue`](Self::job_queue) or
    /// [`job_queue_sharded`](Self::job_queue_sharded).
    #[must_use]
    pub fn job_queue_membership_autoscale(
        mut self,
        stream: &str,
        policy: &MembershipAutoscalePolicy,
        join: impl Fn() -> trembita_actor_store::BoxFuture<
            'static,
            Result<(), trembita_runtime::ClusterScaleError>,
        > + Send
        + Sync
        + 'static,
    ) -> Self {
        let stream = stream.to_string();
        let policy = policy.clone();
        let join = Arc::new(join);
        upsert_queue_autoscale_meta(
            &mut self.queue_autoscale_meta,
            &stream,
            None,
            Some(policy.to_wire()),
        );
        self.job_membership_autoscale
            .push(Box::new(move |state, queues, registry, backlog_registry| {
            let Some(queue) = queues.get(&stream).cloned() else {
                panic!(
                    "job_queue_membership_autoscale stream {stream:?} was not registered via job_queue or job_queue_sharded"
                );
            };
            let backlog = backlog_registry.get(&stream);
            let policy = policy.clone();
            let join = Arc::clone(&join);
            let stream = stream.clone();
            tokio::spawn(async move {
                run_queue_membership_autoscaler(
                    queue,
                    state,
                    registry,
                    stream,
                    policy,
                    backlog,
                    move || {
                        let join = Arc::clone(&join);
                        async move { join().await }
                    },
                )
                .await;
            });
        }));
        self
    }

    /// Declare an auto-worker group: one instance of `A` on every live node,
    /// tracking membership so new nodes get a worker automatically (auto-spawn-on-join).
    #[must_use]
    pub fn manage_auto<A>(mut self, name: &str, config: A::Config) -> Self
    where
        A: UserActor,
        A::Config: Clone + Send + Sync + 'static,
    {
        let name = name.to_string();
        self.managed.push(Box::new(
            move |sup: &ClusterSupervisor<Arc<ClusterFacts>>| {
                sup.manage_auto::<A>(&name, config);
            },
        ));
        self
    }
}
