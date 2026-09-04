use std::collections::HashSet;
use std::sync::Arc;

use trembita_dashboard::MetricsSink;

use crate::NodeId;
use crate::actor_group::ActorGroupOpts;
use crate::app_opts::RunOpts;
use crate::builder::{StartError, TrembitaClusterBuilder};
use crate::configure::TrembitaConfigure;
use crate::consumer::ConsumerSpawnFn;
use crate::cron_opts::CronOpts;
use crate::env_config::{AppConfig, app_config_from_env};
use crate::gateway::spawn_gateway as spawn_gateway_task;
use crate::gateway::{GatewayConfig, GatewayOpts};
use crate::job_opts::JobOpts;
use crate::queue_opts::QueueOpts;
use crate::worker_opts::{WorkerGroup, WorkerOpts};
use crate::workflow_opts::{WorkflowOpts, WorkflowRegistration};
use trembita_runtime::{LeaderGate, LeaderLoopOpts, UserActor};

use super::runtime::TrembitaApp;
use super::types::{EmptyStateMachine, TrembitaAppGatewayApiFlags, TrembitaAppRegistrationFlags};

/// Fluent builder for [`TrembitaApp`].
pub struct TrembitaAppBuilder {
    pub(crate) inner: TrembitaClusterBuilder<EmptyStateMachine>,
    workflows: Vec<WorkflowRegistration>,
    pub(crate) registration: TrembitaAppRegistrationFlags,
    queue_streams: HashSet<String>,
    cron_streams: Vec<String>,
    schedule_streams: Vec<String>,
    event_outbox_streams: Vec<String>,
    topic_streams: HashSet<String>,
    consumer_streams: Vec<String>,
    pending_consumers: Vec<ConsumerSpawnFn>,
    pub(crate) gateway: Option<GatewayConfig>,
    pub(crate) config_errors: Vec<String>,
    pub(crate) gateway_api: TrembitaAppGatewayApiFlags,
    pub(crate) worker_autoscale_streams: Vec<String>,
}

impl TrembitaAppBuilder {
    pub(crate) fn new_default() -> Self {
        Self {
            inner: TrembitaClusterBuilder::new(NodeId(1), EmptyStateMachine),
            workflows: Vec::new(),
            registration: TrembitaAppRegistrationFlags::default(),
            queue_streams: HashSet::new(),
            cron_streams: Vec::new(),
            schedule_streams: Vec::new(),
            event_outbox_streams: Vec::new(),
            topic_streams: HashSet::new(),
            consumer_streams: Vec::new(),
            pending_consumers: Vec::new(),
            gateway: None,
            config_errors: Vec::new(),
            gateway_api: TrembitaAppGatewayApiFlags::default(),
            worker_autoscale_streams: Vec::new(),
        }
    }

    /// Merge env-only settings into `builder` when not already set in code.
    fn apply_env_config(mut self, cfg: &AppConfig) -> Self {
        self.inner = self.inner.merge_app_config(cfg);
        if let Some(stream) = cfg.job_queue_stream.clone()
            && !self.registration.jobs
        {
            self.registration.jobs = true;
            self.queue_streams.insert(stream.clone());
            self = self.queue([QueueOpts::new(stream, cfg.job_queue_lease)]);
        }
        if self.gateway.is_none()
            && let Some(addr) = cfg.gateway
        {
            let any_api = cfg.gateway_jobs_api
                || cfg.gateway_actors_api
                || cfg.gateway_workflows_api
                || cfg.gateway_introspect_api;
            let mut opts = GatewayOpts::new(addr)
                .with_jobs_api(cfg.gateway_jobs_api)
                .with_actors_api(cfg.gateway_actors_api)
                .with_workflows_api(cfg.gateway_workflows_api)
                .with_introspect_api(cfg.gateway_introspect_api)
                .drain_timeout(cfg.gateway_drain_timeout);
            if any_api {
                opts = opts.protect_product_apis(true);
                if crate::gateway::gateway_token_from_env().is_some() {
                    opts = opts.identity(crate::gateway::GatewayBearerIdentity::from_env());
                } else {
                    self.config_errors.push(
                        "gateway product APIs require GATEWAY_TOKEN or TREMBITA_GATEWAY_TOKEN"
                            .into(),
                    );
                }
            }
            if let Some((cert, key)) = cfg.gateway_tls.clone() {
                opts = opts.tls(cert, key);
            }
            self.gateway = Some(opts.into_config());
        } else if let Some(gateway) = self.gateway.as_mut() {
            if gateway.tls.is_none()
                && let Some((cert, key)) = cfg.gateway_tls.clone()
            {
                gateway.tls = Some(crate::gateway::GatewayTlsPaths { cert, key });
            }
            if cfg.env.gateway_introspect {
                gateway.introspect_api = cfg.gateway_introspect_api;
            }
        }
        self
    }

    /// Register [`crate::JobConsumer`] loops (started in [`Self::run`]).
    ///
    /// Prefer [`.consumers`](Self::consumers) when registering several workers at once.
    ///
    /// # Errors
    /// [`Self::run`] / [`Self::boot_for_test`] fail at boot when `C::STREAM` was not registered
    /// via [`.queue`](Self::queue).
    #[must_use]
    pub fn consumer<C: crate::JobConsumer>(
        mut self,
        consumer: C,
        opts: crate::ConsumerOpts,
    ) -> Self {
        self.consumer_streams.push(C::STREAM.to_string());
        self.pending_consumers.push(Box::new(move |app, stop| {
            app.spawn_consumer(consumer, opts, stop)
        }));
        self
    }

    /// Register several consumers via [`crate::ConsumerGroup`].
    ///
    /// # Errors
    /// Same stream / queue rules as [`.consumer`](Self::consumer).
    #[must_use]
    pub fn consumers(mut self, group: crate::ConsumerGroup) -> Self {
        let (streams, spawners) = group.into_parts();
        self.consumer_streams.extend(streams);
        self.pending_consumers.extend(spawners);
        self
    }

    /// Register durable job streams with handlers via [`JobOpts`] (queue + consumer + optional HTTP enqueue).
    #[must_use]
    pub fn jobs(mut self, jobs: impl IntoIterator<Item = JobOpts>) -> Self {
        for job in jobs {
            let reg = job.into_registration();
            if let Some(err) = reg.config_error {
                self.config_errors.push(err);
                continue;
            }
            self.registration.jobs = true;
            self.queue_streams.insert(reg.stream.clone());
            self.inner = self.inner.job_queue(&reg.queue.name, reg.queue.lease);
            self.inner = self
                .inner
                .job_queue_prefetch(&reg.queue.name, reg.queue.prefetch);
            self.inner = self
                .inner
                .job_queue_max_attempts(&reg.queue.name, reg.queue.default_max_attempts);
            if let Some((backlog, opts)) = reg.backlog {
                self.inner = self
                    .inner
                    .job_queue_external_backlog(&reg.stream, backlog, opts);
            }
            if !reg.spawners.is_empty() {
                self.consumer_streams.push(reg.stream);
            }
            self.pending_consumers.extend(reg.spawners);
            if reg.http_enqueue {
                self.gateway_api.jobs = true;
                if let Some(gateway) = self.gateway.as_mut() {
                    gateway.jobs_api = true;
                }
            }
        }
        self
    }

    /// Register durable job streams (requires [`Self::data_dir`]).
    #[must_use]
    pub fn queue(mut self, queues: impl IntoIterator<Item = QueueOpts>) -> Self {
        for opts in queues {
            self.registration.jobs = true;
            self.queue_streams.insert(opts.name.clone());
            self.inner = self.inner.job_queue(&opts.name, opts.lease);
            self.inner = self.inner.job_queue_prefetch(&opts.name, opts.prefetch);
            self.inner = self
                .inner
                .job_queue_max_attempts(&opts.name, opts.default_max_attempts);
        }
        self
    }

    /// Register durable event topics with named subscriptions (requires [`Self::data_dir`]).
    #[must_use]
    pub fn topics(
        mut self,
        topics: impl IntoIterator<Item = crate::topic_opts::TopicOpts>,
    ) -> Self {
        for opts in topics {
            self.topic_streams.insert(opts.name.clone());
            self.inner = self.inner.event_topic(&opts.name, opts.lease);
            self.inner = self.inner.event_topic_retention(&opts.name, opts.retention);
            if !opts.subscriptions.is_empty() {
                self.inner = self
                    .inner
                    .event_topic_subscriptions(&opts.name, &opts.subscriptions);
            }
            if let Some((source, drain_opts)) = opts.outbox {
                self.event_outbox_streams.push(opts.name.clone());
                self.inner = self
                    .inner
                    .event_outbox_source_with_opts(&opts.name, source, drain_opts);
            }
        }
        self
    }

    /// Register cron-driven recurring enqueues (requires matching [`.queue`](Self::queue) streams).
    ///
    /// Implemented as a [`StaticScheduleSource`](trembita_jobs::StaticScheduleSource) —
    /// same reconcile path as [`.schedule_source`](Self::schedule_source).
    ///
    /// # Errors
    /// [`Self::run`] / [`Self::boot_for_test`] fail at boot when a cron stream has no matching
    /// [`.queue`](Self::queue) registration.
    #[must_use]
    pub fn cron(mut self, schedules: impl IntoIterator<Item = CronOpts>) -> Self {
        for opts in schedules {
            self.cron_streams.push(opts.stream.clone());
            self.inner = self.inner.recurring_job(&opts.stream, opts.job);
        }
        self
    }

    /// Poll a [`crate::ScheduleSource`] on the queue leader and reconcile recurring jobs
    /// ([schedule-source](../../docs/decisions/schedule-source.md)).
    ///
    /// Requires a matching [`.queue`](Self::queue) stream. Pairs with [`.cron`](Self::cron).
    #[must_use]
    pub fn schedule_source(
        mut self,
        stream: impl Into<String>,
        source: Arc<dyn trembita_jobs::ScheduleSource>,
        poll: trembita_jobs::SchedulePoll,
    ) -> Self {
        let stream = stream.into();
        self.schedule_streams.push(stream.clone());
        self.inner = self.inner.schedule_source(&stream, source, poll);
        self
    }

    /// Poll an [`EventOutboxSource`](crate::EventOutboxSource) on the topic leader and publish into the topic
    /// ([event-outbox](../../docs/decisions/event-outbox.md)).
    ///
    /// Requires a matching [`.topics`](Self::topics) registration.
    #[must_use]
    pub fn event_outbox_source(
        mut self,
        topic: impl Into<String>,
        source: Arc<dyn trembita_events::EventOutboxSource>,
        poll: trembita_events::EventOutboxPoll,
    ) -> Self {
        let topic = topic.into();
        self.event_outbox_streams.push(topic.clone());
        self.inner = self.inner.event_outbox_source(&topic, source, poll);
        self
    }

    /// Register an actor group — [`ActorGroupOpts::default`] / [`ActorGroupOpts::new`] = one worker per live node;
    /// [`ActorGroupOpts::fixed`] = fixed pool size cluster-wide.
    ///
    /// Prefer [`.workers`](Self::workers) with [`WorkerOpts`] for explicit scale.
    #[must_use]
    pub fn actors<A: UserActor>(mut self, name: &str, opts: ActorGroupOpts<A::Config>) -> Self
    where
        A::Config: Clone + Send + Sync + 'static,
    {
        self.registration.actors = true;
        self.inner = match opts.total {
            Some(total) => self.inner.manage::<A>(name, total, opts.config),
            None => self.inner.manage_auto::<A>(name, opts.config),
        };
        self
    }

    /// Register one managed worker actor group via [`WorkerOpts`].
    #[must_use]
    pub fn worker<A: UserActor>(self, opts: WorkerOpts<A>) -> Self
    where
        A::Config: Clone + Send + Sync + 'static,
    {
        self.apply_worker_entry(opts.into_entry())
    }

    /// Register several worker actor groups via [`WorkerGroup`] or [`workers!`](crate::workers).
    #[must_use]
    pub fn workers(mut self, group: WorkerGroup) -> Self {
        for entry in group.into_entries() {
            self = self.apply_worker_entry(entry);
        }
        self
    }

    fn apply_worker_entry(self, entry: crate::worker_opts::WorkerEntry) -> Self {
        (entry.apply)(self)
    }

    /// Register workflow plans and runners for HTTP `/workflows/*` and [`TrembitaApp::run_workflow_id`].
    ///
    /// Use [`WorkflowOpts::new`] + [`crate::journal_workflow`] when the default keyed client is enough.
    ///
    /// # Errors
    /// [`Self::run`] / [`Self::boot_for_test`] fail at boot unless [`.gateway`](Self::gateway)
    /// enables workflows via `.with_workflows_api(true)` (or `TREMBITA_GATEWAY_WORKFLOWS=1` at boot).
    #[must_use]
    pub fn workflows(mut self, specs: impl IntoIterator<Item = WorkflowOpts>) -> Self {
        self.workflows
            .extend(specs.into_iter().map(WorkflowOpts::into_registration));
        self
    }

    /// Public HTTP gateway — custom routes and optional built-in APIs ([`GatewayOpts`]).
    #[must_use]
    pub fn gateway(mut self, opts: GatewayOpts) -> Self {
        let mut config = opts.into_config();
        if self.gateway_api.jobs {
            config.jobs_api = true;
        }
        if self.gateway_api.actors {
            config.actors_api = true;
        }
        self.gateway = Some(config);
        self
    }

    /// Per-node workload governor — compute tokens arbitrate gateway vs job handlers
    /// ([workload governor](../../docs/decisions/workload-governor.md)).
    #[must_use]
    pub fn workload(mut self, opts: trembita_jobs::WorkloadOpts) -> Self {
        self.inner = self.inner.workload(opts);
        self
    }

    /// Initial cluster membership (voting nodes) for static multi-node bootstrap.
    #[must_use]
    pub fn members(mut self, members: impl IntoIterator<Item = NodeId>) -> Self {
        self.inner = self.inner.members(members);
        self
    }

    /// Static voter bootstrap for the first `count` nodes (`NodeId(1)` … `NodeId(count)`).
    #[must_use]
    pub fn voters(mut self, count: u32) -> Self {
        self.inner = self.inner.voters(count);
        self
    }

    /// Accept dynamic cluster joins on this node (seed-side).
    #[must_use]
    pub fn allow_join(mut self, allow: bool) -> Self {
        self.inner = self.inner.allow_join(allow);
        self
    }

    /// Accept cluster leave RPC on this node.
    #[must_use]
    pub fn allow_leave(mut self, allow: bool) -> Self {
        self.inner = self.inner.allow_leave(allow);
        self
    }

    /// Join an existing cluster via a single seed (`TREMBITA_JOIN_SEEDS` equivalent).
    #[must_use]
    pub fn join(mut self, seed: NodeId, addr: std::net::SocketAddr) -> Self {
        self.inner = self.inner.join(seed, addr);
        self
    }

    /// Join via multiple seeds (deduped at boot).
    #[must_use]
    pub fn join_seeds(mut self, seeds: impl IntoIterator<Item = crate::discovery::Seed>) -> Self {
        self.inner = self.inner.join_seeds(seeds);
        self
    }

    /// PEM hot-reload poll interval when TLS paths are configured (default 60s).
    #[must_use]
    pub fn cert_watch(mut self, period: std::time::Duration) -> Self {
        self.inner = self.inner.cert_watch(period);
        self
    }

    /// Accept [`trembita_proto::JoinRole::Voter`] on `/cluster/join` (seed-side).
    /// Joiners must request voter role via [`.join_as`](Self::join_as) or
    /// `TREMBITA_JOIN_ROLE=voter`; default dynamic join is learner-only
    /// ([cluster-elasticity](../../docs/decisions/cluster-elasticity.md)).
    #[must_use]
    pub fn allow_voter_join(mut self, allow: bool) -> Self {
        self.inner = self.inner.allow_voter_join(allow);
        self
    }

    /// Role requested when this node joins via `TREMBITA_JOIN_SEEDS` (default learner).
    #[must_use]
    pub fn join_as(mut self, role: trembita_proto::JoinRole) -> Self {
        self.inner = self.inner.join_as(role);
        self
    }

    /// When `true` (default), the leader replaces a permanently unreachable voter by
    /// promoting the lowest-id caught-up learner.
    #[must_use]
    pub fn voter_replacement(mut self, enabled: bool) -> Self {
        self.inner = self.inner.voter_replacement(enabled);
        self
    }

    /// Override the logical-tick grace period before an unreachable voter is replaced.
    #[must_use]
    pub fn voter_replacement_grace_ticks(mut self, ticks: u64) -> Self {
        self.inner = self.inner.voter_replacement_grace_ticks(ticks);
        self
    }

    /// Register a leader-only periodic task ([leader-task](../../docs/decisions/leader-task.md)).
    ///
    /// The closure runs on each tick while this node holds Raft leadership.
    /// Use [`LeaderGate::first_in_term`] for one-shot work after election.
    #[must_use]
    pub fn on_leader<F, Fut>(mut self, opts: LeaderLoopOpts, f: F) -> Self
    where
        F: Fn(LeaderGate) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.inner = self.inner.on_leader(opts, f);
        self
    }

    fn validate(&self) -> Result<(), StartError> {
        if let Some(err) = self.config_errors.first() {
            return Err(StartError::Config(err.clone()));
        }
        for stream in &self.cron_streams {
            if !self.queue_streams.contains(stream) {
                return Err(StartError::Config(format!(
                    "`.cron()` stream {stream:?} has no matching `.queue()` registration"
                )));
            }
        }
        for stream in &self.schedule_streams {
            if !self.queue_streams.contains(stream) {
                return Err(StartError::Config(format!(
                    "`.schedule_source()` stream {stream:?} has no matching `.queue()` registration"
                )));
            }
        }
        for topic in &self.event_outbox_streams {
            if !self.topic_streams.contains(topic) {
                return Err(StartError::Config(format!(
                    "`.event_outbox_source()` topic {topic:?} has no matching `.topics()` registration"
                )));
            }
        }
        for stream in &self.consumer_streams {
            if !self.queue_streams.contains(stream) {
                return Err(StartError::Config(format!(
                    "`.consumer()` stream {stream:?} has no matching `.queue()` registration"
                )));
            }
        }
        for stream in &self.worker_autoscale_streams {
            if !self.queue_streams.contains(stream) {
                return Err(StartError::Config(format!(
                    "`.workers()` autoscale stream {stream:?} has no matching `.queue()` or `.jobs()` registration"
                )));
            }
        }
        if !self.workflows.is_empty() {
            match self.gateway.as_ref() {
                Some(g) if g.workflows_api => {}
                Some(_) => {
                    return Err(StartError::Config(
                        "`.workflows([…])` requires `.gateway(GatewayOpts::new(addr).with_workflows_api(true))`"
                            .into(),
                    ));
                }
                None => {
                    return Err(StartError::Config(
                        "`.workflows([…])` requires `.gateway(GatewayOpts::new(addr).with_workflows_api(true))` (or `TREMBITA_GATEWAY` + `TREMBITA_GATEWAY_WORKFLOWS=1`)"
                            .into(),
                    ));
                }
            }
        }
        Ok(())
    }

    async fn finish_start(
        app: TrembitaApp,
        gateway: Option<GatewayConfig>,
        wait_ready: Option<crate::ReadyOpts>,
    ) -> Result<Arc<TrembitaApp>, StartError> {
        let app = Arc::new(app);
        if let Some(config) = gateway {
            let addr = config.addr;
            let handle = spawn_gateway_task(Arc::clone(&app), config)
                .await
                .map_err(|e| StartError::Config(format!("gateway bind to {addr}: {e}")))?;
            app.install_gateway(handle).await;
        }
        if let Some(opts) = wait_ready {
            app.wait_until_ready(opts).await;
        }
        Ok(app)
    }

    async fn boot(self, opts: &mut RunOpts) -> Result<Arc<TrembitaApp>, StartError> {
        if let Some(net) = opts.local_net.as_ref() {
            self.validate()?;
            let workflows = self.workflows;
            let gateway = self.gateway;
            let cluster = self.inner.start_local(net).await;
            return Self::finish_start(
                TrembitaApp::assemble(cluster, workflows),
                gateway,
                opts.wait_ready.clone(),
            )
            .await;
        }
        let cfg = app_config_from_env().map_err(|e| StartError::Config(e.to_string()))?;
        let mut builder = self;
        builder = builder.apply_env_config(&cfg);
        builder.validate()?;
        let workflows = builder.workflows;
        let gateway = builder.gateway;
        let cluster = builder
            .inner
            .start_quic_cluster(
                cfg.security,
                cfg.listen,
                cfg.peers,
                cfg.pem_paths.clone(),
                cfg.cert_dir.clone(),
            )
            .await?;
        Self::finish_start(
            TrembitaApp::assemble(cluster, workflows),
            gateway,
            opts.wait_ready.clone(),
        )
        .await
    }

    /// Boot, spawn registered [`Self::consumer`] loops, block on Ctrl-C, graceful shutdown.
    ///
    /// Always starts a QUIC cluster member (seed or joiner) from `TREMBITA_*` env.
    ///
    /// # Errors
    /// Returns an error when boot, signal handling, or teardown fails.
    pub async fn run(mut self, mut opts: RunOpts) -> Result<(), Box<dyn std::error::Error>> {
        let mut pending = std::mem::take(&mut self.pending_consumers);
        let app = self.boot(&mut opts).await?;
        if !pending.is_empty() {
            let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            let handles = pending
                .drain(..)
                .map(|spawn| spawn(Arc::clone(&app), stop_rx.clone()))
                .collect();
            opts.shutdown.consumers = Some((stop_tx, handles));
        }
        app.wait_for_shutdown(opts.shutdown).await
    }

    /// Test-only boot without blocking on Ctrl-C ([`trembita_test_support::boot_local_app`]).
    #[doc(hidden)]
    pub async fn boot_for_test(self, mut opts: RunOpts) -> Result<Arc<TrembitaApp>, StartError> {
        self.boot(&mut opts).await
    }

    /// Persistent `data_dir` — enables redb job queue and actor workflow store.
    #[must_use]
    pub fn data_dir(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.inner = self.inner.data_dir(path);
        self
    }

    /// Apply runtime / cluster tuning ([`TrembitaConfigure`]).
    #[must_use]
    pub fn configure(mut self, config: TrembitaConfigure) -> Self {
        self.inner = config.apply_to(self.inner);
        self
    }

    /// Forward runtime metrics to an external [`MetricsSink`] (Prometheus scrape stays enabled).
    #[must_use]
    pub fn metrics_sink(mut self, sink: Arc<dyn MetricsSink>) -> Self {
        self.inner = self.inner.metrics_sink(sink);
        self
    }

    /// Test / framework hook — prefer [`TrembitaAppBuilder`] methods.
    #[doc(hidden)]
    #[must_use]
    pub fn inner_mut(&mut self) -> &mut TrembitaClusterBuilder<EmptyStateMachine> {
        &mut self.inner
    }
}
