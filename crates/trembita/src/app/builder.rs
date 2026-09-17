use std::collections::HashSet;
use std::sync::Arc;

use trembita_dashboard::MetricsSink;

#[cfg(feature = "http-jobs")]
use super::gateway_defaults::default_product_surfaces;
use crate::NodeId;
use crate::actor_group::ActorGroupOpts;
use crate::app_opts::RunOpts;
use crate::capability::CapDeps;
use crate::capability::CapManifest;
use crate::capability::CapRuntime;
use crate::configure::TrembitaConfigure;
use crate::consumer::ConsumerSpawnFn;
use crate::cron_opts::CronOpts;
use crate::gateway::spawn_gateway as spawn_gateway_task;
use crate::gateway::{GatewayBearerIdentity, GatewayConfig, GatewayOpts};
use crate::job_opts::JobOpts;
use crate::queue_opts::{QueueOpts, QueueRegistrationScale};
use crate::scheduled_workflow_opts::ScheduledWorkflowOpts;
use crate::worker_opts::{WorkerGroup, WorkerOpts};
use crate::workflow_opts::{WorkflowOpts, WorkflowRegistration};
use trembita_assembly::{AppConfig, app_config_from_env};
use trembita_assembly::{StartError, TrembitaClusterBuilder};
use trembita_runtime::{DirectoryPolicy, DirectoryRetry, LeaderGate, LeaderLoopOpts, UserActor};

use super::manifest::AppManifest;
use super::run_hint::ManifestRunHint;
use super::runtime::TrembitaApp;
use super::scale_plan::ProductScalePlan;
use super::types::{
    EmptyStateMachine, GatewayProductApiExclusions, TrembitaAppGatewayApiFlags,
    TrembitaAppRegistrationFlags,
};

/// Fluent builder for [`TrembitaApp`].
///
/// Cluster membership, join seeds, and seed/joiner policy come from [`Self::from_env`] /
/// [`Self::from_config`] (`TREMBITA_*`).
pub struct TrembitaAppBuilder {
    pub(crate) inner: TrembitaClusterBuilder<EmptyStateMachine>,
    workflows: Vec<WorkflowRegistration>,
    pub(crate) registration: TrembitaAppRegistrationFlags,
    pub(crate) queue_streams: HashSet<String>,
    cron_streams: Vec<String>,
    schedule_streams: Vec<String>,
    event_outbox_streams: Vec<String>,
    topic_streams: HashSet<String>,
    pub(crate) consumer_streams: Vec<String>,
    pub(crate) pending_consumers: Vec<ConsumerSpawnFn>,
    pub(crate) gateway: Option<GatewayConfig>,
    pub(crate) config_errors: Vec<String>,
    pub(crate) gateway_api: TrembitaAppGatewayApiFlags,
    pub(crate) worker_autoscale_streams: Vec<String>,
    #[cfg(feature = "http-jobs")]
    gateway_extra_routes: Option<
        Arc<
            dyn Fn(crate::gateway::TrembitaGatewayState) -> trembita_http::RouteTable + Send + Sync,
        >,
    >,
    /// Operational routes on the unified listener ([`super::gateway::DefaultGatewayApis::ops`]).
    #[cfg(feature = "http-jobs")]
    gateway_include_ops: bool,
    /// Disable registration-driven product APIs on the default gateway.
    #[cfg(feature = "http-jobs")]
    gateway_exclude_apis: GatewayProductApiExclusions,
    /// Config from [`Self::from_config`] — avoids re-parsing env in [`Self::boot`].
    boot_config: Option<AppConfig>,
    /// Derived from [`.manifest`](Self::manifest) / [`.jobs`](Self::jobs) / [`.workers`](Self::workers).
    run_hint: ManifestRunHint,
    pub(crate) cap_runtime: CapRuntime,
    cap_deps: Option<CapDeps>,
    pub(crate) scale_plan: ProductScalePlan,
    /// B-37 preset applied to standard manifest queues ([`Self::configure`]).
    coordination_growth_preset:
        Option<trembita_assembly::coordination_profile::CoordinationGrowthPreset>,
    /// B-41 — [`TrembitaConfigure::with_durable_mailbox`].
    durable_mailbox: bool,
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
            #[cfg(feature = "http-jobs")]
            gateway_extra_routes: None,
            #[cfg(feature = "http-jobs")]
            gateway_include_ops: false,
            #[cfg(feature = "http-jobs")]
            gateway_exclude_apis: GatewayProductApiExclusions {
                jobs: true,
                schedules: true,
                actors: true,
                workflows: true,
                topics: true,
            },
            boot_config: None,
            run_hint: ManifestRunHint::default(),
            cap_runtime: CapRuntime::empty(),
            cap_deps: None,
            scale_plan: ProductScalePlan::default(),
            coordination_growth_preset: None,
            durable_mailbox: false,
        }
    }

    /// Domain service ports available from [`OpCtx::deps`](crate::OpCtx::deps) in capability handlers.
    #[must_use]
    pub fn cap_deps(mut self, deps: CapDeps) -> Self {
        self.cap_deps = Some(deps);
        self
    }

    /// Register capability groups — use [`AppManifest::capabilities`] + [`.manifest`](Self::manifest).
    #[must_use]
    pub(crate) fn capabilities(self, caps: CapManifest) -> Self {
        let (mut builder, runtime) = caps.apply(self);
        builder.cap_runtime = runtime;
        // R3: product capability delivery uses directory RYW (spawn, scale, rebalance).
        builder.inner = builder
            .inner
            .directory_policy(DirectoryPolicy::ReadYourWrites)
            .directory_retry(DirectoryRetry::default());
        builder
    }

    /// Directory visibility for cross-node capability / actor delivery (R3 in `future-work-and-risks`).
    ///
    /// [`AppManifest`](super::manifest::AppManifest) capabilities enable
    /// [`DirectoryPolicy::ReadYourWrites`] by default (brief retry on `NoTarget` after
    /// spawn, scale, or Raft group rebalance). Default retry: **8** attempts × **25 ms**
    /// ([`DirectoryRetry::default`](trembita_runtime::DirectoryRetry::default)); assembly
    /// boosts to [`DirectoryRetry::after_rebalance`](trembita_runtime::DirectoryRetry::after_rebalance)
    /// for **3 s** after multi-Raft group adopt/retire. Override for lowest-latency advanced paths.
    #[must_use]
    pub fn directory_policy(mut self, policy: DirectoryPolicy) -> Self {
        self.inner = self.inner.directory_policy(policy);
        self
    }

    /// Retry budget when [`directory_policy`](Self::directory_policy) is
    /// [`DirectoryPolicy::ReadYourWrites`] (default **8 × 25 ms** on capability apps).
    #[must_use]
    pub fn directory_retry(mut self, retry: DirectoryRetry) -> Self {
        self.inner = self.inner.directory_retry(retry);
        self
    }

    #[must_use]
    pub(crate) fn with_run_hint(mut self, hint: ManifestRunHint) -> Self {
        self.run_hint = hint;
        self
    }

    fn resolve_run_opts(&self) -> Result<RunOpts, StartError> {
        let base = if let Some(cfg) = &self.boot_config {
            RunOpts::from_config(cfg)
        } else {
            RunOpts::default()
        };
        Ok(base.with_run_hint(&self.run_hint))
    }

    /// Re-enable `/actors/*` on the default gateway ([`WorkerOpts::http_cast`](crate::WorkerOpts::http_cast)).
    #[cfg(feature = "http-jobs")]
    pub(crate) fn enable_actors_gateway_api(&mut self) {
        self.gateway_api.actors = true;
        self.gateway_exclude_apis.actors = false;
    }

    #[cfg(feature = "http-jobs")]
    fn default_gateway_apis(&self) -> super::gateway::DefaultGatewayApis {
        let jobs = self.gateway_api.jobs && !self.gateway_exclude_apis.jobs;
        super::gateway::DefaultGatewayApis {
            ops: self.gateway_include_ops,
            jobs,
            schedules: jobs && !self.gateway_exclude_apis.schedules,
            actors: self.gateway_api.actors && !self.gateway_exclude_apis.actors,
            workflows: !self.workflows.is_empty() && !self.gateway_exclude_apis.workflows,
            topics: !self.topic_streams.is_empty() && !self.gateway_exclude_apis.topics,
        }
    }

    /// Merge extra HTTP routes into the env/default gateway ([`TrembitaApp::default_surfaces`] + your tables).
    ///
    /// Scaffold: `http/product.rs` → `.gateway_routes(|state| http::product::route_table(&state))`.
    #[cfg(feature = "http-jobs")]
    #[must_use]
    pub fn gateway_routes<F>(mut self, routes: F) -> Self
    where
        F: Fn(crate::gateway::TrembitaGatewayState) -> trembita_http::RouteTable
            + Send
            + Sync
            + 'static,
    {
        self.gateway_extra_routes = Some(Arc::new(routes));
        self
    }

    fn apply_growth_to_queue_opts(&self, mut opts: QueueOpts) -> QueueOpts {
        if let Some(preset) = self.coordination_growth_preset {
            opts = opts.with_coordination_growth_preset(preset);
        }
        opts
    }

    fn mount_queue_stream(
        inner: TrembitaClusterBuilder<EmptyStateMachine>,
        opts: &QueueOpts,
    ) -> TrembitaClusterBuilder<EmptyStateMachine> {
        let inner = match &opts.scale {
            QueueRegistrationScale::Standard => inner.job_queue(&opts.name, opts.lease),
            QueueRegistrationScale::Sharded(count) => {
                inner.job_queue_sharded(&opts.name, *count, opts.lease)
            }
            QueueRegistrationScale::AutoShard(policy) => {
                inner.job_queue_auto_shard(&opts.name, opts.lease, policy.clone())
            }
        };
        inner
            .job_queue_prefetch(&opts.name, opts.prefetch)
            .job_queue_max_attempts(&opts.name, opts.default_max_attempts)
    }

    fn apply_env_coordination(
        inner: TrembitaClusterBuilder<EmptyStateMachine>,
        cfg: &AppConfig,
    ) -> TrembitaClusterBuilder<EmptyStateMachine> {
        let mut inner = inner;
        if cfg.coordination_raft_groups > 1 {
            let n = usize::try_from(cfg.coordination_raft_groups).unwrap_or(1);
            inner = inner.raft_machines((0..n).map(|_| EmptyStateMachine));
            if let Some(count) = cfg.coordination_shard_count {
                inner = inner.shard_count(count);
            }
        }
        inner
    }

    /// Merge env-only settings into `builder` when not already set in code.
    fn apply_env_config(mut self, cfg: &AppConfig) -> Self {
        self.inner = Self::apply_env_coordination(self.inner.merge_app_config(cfg), cfg);
        self.scale_plan
            .set_coordination(cfg.coordination_raft_groups, cfg.coordination_shard_count);
        if let Some(stream) = cfg.job_queue_stream.clone()
            && !self.registration.jobs
        {
            self.registration.jobs = true;
            self.queue_streams.insert(stream.clone());
            let mut opts = QueueOpts::new(stream, cfg.job_queue_lease);
            if cfg.job_queue_auto_shard {
                let policy = cfg
                    .coordination_growth_profile
                    .map(|p| p.spec().auto_shard_policy)
                    .unwrap_or_default();
                opts = opts.auto_shard_policy(policy);
            } else if let Some(shards) = cfg.job_queue_shards {
                opts = opts.sharded(shards);
            }
            self = self.queue([opts]);
        }
        if let Some(preset) = cfg.coordination_growth_profile {
            self.coordination_growth_preset = Some(preset);
        }
        if let Some(gateway) = self.gateway.as_mut()
            && gateway.tls.is_none()
            && let Some((cert, key)) = cfg.http_tls.clone()
        {
            gateway.tls = Some(crate::gateway::GatewayTlsPaths { cert, key });
        }
        if !cfg.join_seeds.is_empty() {
            self.run_hint.join_pool_wait = true;
        }
        self
    }

    /// Builder from a parsed [`AppConfig`] (scaffold `config.rs`, tests, embedders).
    ///
    /// Same cluster/gateway/job-queue merge as [`Self::from_env`], without reading the environment again in [`.run`](Self::run).
    #[must_use]
    pub fn from_config(cfg: AppConfig) -> Self {
        let mut builder = Self::new_default().apply_env_config(&cfg);
        #[cfg(feature = "http-jobs")]
        if cfg.http.is_some() {
            builder.apply_env_listen_gateway_defaults();
        }
        builder.boot_config = Some(cfg);
        builder
    }

    /// Ops + registration-driven product HTTP when `TREMBITA_LISTEN` is set ([`Self::from_env`]); `/actors/*` stays off.
    #[cfg(feature = "http-jobs")]
    fn apply_env_listen_gateway_defaults(&mut self) {
        self.gateway_include_ops = true;
        self.gateway_exclude_apis.jobs = false;
        self.gateway_exclude_apis.schedules = false;
        self.gateway_exclude_apis.workflows = false;
        self.gateway_exclude_apis.topics = false;
    }

    /// Env-first builder: cluster join/listen/data_dir/job queue from `TREMBITA_*` (see [`trembita_assembly::env_config::app_config_from_env`]).
    ///
    /// When `TREMBITA_LISTEN` / [`AppConfig::http`](trembita_assembly::env_config::AppConfig) is set, ops (`/health`, `/metrics`, …) and
    /// registration-driven product APIs mount automatically; `/actors/*` stays off until
    /// [`.with_actors_api`](Self::with_actors_api) + [`WorkerOpts::http_cast`](crate::WorkerOpts::http_cast).
    ///
    /// Prefer [`Self::from_config`] when `main` already parsed env once. Register domain wiring (`.jobs`, `.manifest`, …), then [`.run`](Self::run)([`RunOpts::for_manifest`](crate::app_opts::RunOpts::for_manifest)).
    ///
    /// # Errors
    /// Invalid or missing required environment variables.
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self::from_config(app_config_from_env()?))
    }

    fn ensure_product_gateway(mut self, cfg: Option<&AppConfig>) -> Self {
        #[cfg(feature = "http-jobs")]
        {
            let http = cfg
                .and_then(|c| c.http)
                .or_else(|| self.gateway.as_ref().map(|g| g.addr));
            let Some(http) = http else {
                return self;
            };
            if self.gateway.is_none() {
                self.gateway = Some(self.default_gateway_config(http, cfg));
                return self;
            }
            let apis = self.default_gateway_apis();
            let extra = self.gateway_extra_routes.clone();
            let Some(gateway) = self.gateway.as_mut() else {
                return self;
            };
            let ws = gateway.websocket_routes.clone();
            if let Some(user_surfaces) = gateway.surfaces.take() {
                gateway.surfaces = Some(super::gateway_defaults::composite_product_surfaces(
                    apis,
                    extra,
                    ws,
                    user_surfaces,
                ));
            } else {
                gateway.surfaces = Some(default_product_surfaces(apis, extra, ws));
            }
            if gateway.identity.is_none() {
                gateway.identity = GatewayOpts::new(gateway.addr)
                    .identity(GatewayBearerIdentity::from_env())
                    .into_config()
                    .identity;
            }
        }
        let _ = cfg;
        self
    }

    #[cfg(feature = "http-jobs")]
    fn default_gateway_config(
        &self,
        addr: std::net::SocketAddr,
        cfg: Option<&AppConfig>,
    ) -> GatewayConfig {
        use crate::gateway::DEFAULT_GATEWAY_DRAIN_TIMEOUT;
        let drain = cfg
            .map(|c| c.http_drain_timeout)
            .unwrap_or(DEFAULT_GATEWAY_DRAIN_TIMEOUT);
        let mut opts = GatewayOpts::new(addr)
            .drain_timeout(drain)
            .identity(GatewayBearerIdentity::from_env())
            .surfaces(default_product_surfaces(
                self.default_gateway_apis(),
                self.gateway_extra_routes.clone(),
                None,
            ));
        if let Some((cert, key)) = cfg.and_then(|c| c.http_tls.clone()) {
            opts = opts.tls(cert, key);
        }
        opts.into_config()
    }

    /// Register [`crate::JobConsumer`] loops (started in [`Self::run`]).
    ///
    /// Prefer [`.consumers`](Self::consumers) when registering several workers at once.
    ///
    /// # Errors
    /// [`Self::run`] / [`Self::boot_for_test`] fail at boot when `C::STREAM` was not registered
    /// via [`.queue`](Self::queue).
    #[must_use]
    #[expect(dead_code)] // direct builder chaining; [`AppManifest`] uses [`Self::consumers`]
    pub(crate) fn consumer<C: crate::JobConsumer>(
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
    pub(crate) fn consumers(mut self, group: crate::ConsumerGroup) -> Self {
        let (streams, spawners) = group.into_parts();
        self.consumer_streams.extend(streams);
        self.pending_consumers.extend(spawners);
        self
    }

    /// Apply a declarative [`AppManifest`] (jobs, topics, workers, workflows).
    #[must_use]
    pub fn manifest(self, manifest: AppManifest) -> Self {
        manifest.apply(self)
    }

    /// Register durable job streams with handlers via [`JobOpts`] (queue + consumer + optional HTTP enqueue).
    #[must_use]
    pub(crate) fn jobs(mut self, jobs: impl IntoIterator<Item = JobOpts>) -> Self {
        for job in jobs {
            let stream = job.stream_name().to_string();
            let reg = job.into_registration();
            if let Some(err) = reg.config_error {
                self.config_errors.push(err);
                continue;
            }
            self.registration.jobs = true;
            self.queue_streams.insert(reg.stream.clone());
            let queue = self.apply_growth_to_queue_opts(reg.queue);
            self.inner = Self::mount_queue_stream(self.inner, &queue);
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
            }
            self.run_hint.record_job_stream(&stream);
        }
        self
    }

    /// Register durable job streams (requires [`Self::data_dir`]).
    #[must_use]
    pub(crate) fn queue(mut self, queues: impl IntoIterator<Item = QueueOpts>) -> Self {
        for opts in queues {
            self.registration.jobs = true;
            self.queue_streams.insert(opts.name.clone());
            let opts = self.apply_growth_to_queue_opts(opts);
            self.scale_plan.record_job_queue(&opts.name, &opts.scale);
            self.inner = Self::mount_queue_stream(self.inner, &opts);
        }
        self
    }

    /// Register durable event topics with named subscriptions (requires [`Self::data_dir`]).
    #[must_use]
    pub(crate) fn topics(
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
    pub(crate) fn cron(mut self, schedules: impl IntoIterator<Item = CronOpts>) -> Self {
        for opts in schedules {
            self.cron_streams.push(opts.stream.clone());
            self.inner = self.inner.recurring_job(&opts.stream, opts.job);
        }
        self
    }

    /// **Cron → workflow/pipeline** in one call: queue stream, schedules, and a built-in consumer.
    ///
    /// Equivalent to `.queue(…)` + `.cron([CronOpts::starts_workflow…])` + automatic
    /// [`dispatch_work_trigger`](crate::work_trigger::dispatch_work_trigger). Still requires
    /// [`.workflows`](Self::workflows) when schedules call saga ids.
    ///
    /// ```
    /// # use trembita::{AppManifest, ScheduledWorkflowOpts, TrembitaApp, TrembitaConfigure};
    /// TrembitaApp::builder()
    ///     .manifest(AppManifest::new().scheduled_workflows(
    ///         ScheduledWorkflowOpts::new().workflow("weekly", "0 3 * * 1", "weekly-report"),
    ///     ))
    ///     .configure(TrembitaConfigure::default().with_data_dir("/tmp/x"));
    /// ```
    #[must_use]
    pub(crate) fn scheduled_workflows(mut self, spec: ScheduledWorkflowOpts) -> Self {
        let reg = spec.into_registration();
        if let Some(err) = reg.config_error {
            self.config_errors.push(err);
            return self;
        }
        if reg.crons.is_empty() {
            self.config_errors.push(
                "`.scheduled_workflows()`: add at least one `.workflow()` or `.pipeline()`".into(),
            );
            return self;
        }
        self = self.queue([reg.queue]);
        self = self.cron(reg.crons);
        if !reg.spawners.is_empty() {
            self.consumer_streams.push(reg.stream);
            self.pending_consumers.extend(reg.spawners);
        }
        self
    }

    /// Poll a [`crate::ScheduleSource`] on the queue leader and reconcile recurring jobs
    /// ([schedule-source](../../docs/decisions/schedule-source.md)).
    ///
    /// Requires a matching [`.queue`](Self::queue) stream. Pairs with [`.cron`](Self::cron).
    #[must_use]
    pub(crate) fn schedule_source(
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
    #[expect(dead_code)] // prefer [`Self::topics`] outbox wiring; retained for explicit builder use
    pub(crate) fn event_outbox_source(
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
    #[expect(dead_code)] // direct builder chaining; [`AppManifest::workers`] uses [`Self::workers`]
    pub(crate) fn actors<A: UserActor>(
        mut self,
        name: &str,
        opts: ActorGroupOpts<A::Config>,
    ) -> Self
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
    #[expect(dead_code)] // direct builder chaining; [`AppManifest::workers`] uses [`Self::workers`]
    pub(crate) fn worker<A: UserActor>(self, opts: WorkerOpts<A>) -> Self
    where
        A::Config: Clone + Send + Sync + 'static,
    {
        self.apply_worker_entry(opts.into_entry())
    }

    /// Register several worker actor groups via [`WorkerGroup`] or [`workers!`](crate::workers).
    #[must_use]
    pub(crate) fn workers(mut self, group: WorkerGroup) -> Self {
        for entry in group.into_entries() {
            self = self.apply_worker_entry(entry);
        }
        self.run_hint.has_workers = true;
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
    /// [`Self::run`] / [`Self::boot_for_test`] fail at boot unless a product HTTP listener
    /// is available (`TREMBITA_LISTEN` or [`.gateway`](Self::gateway)) with workflow routes.
    #[must_use]
    pub(crate) fn workflows(mut self, specs: impl IntoIterator<Item = WorkflowOpts>) -> Self {
        self.workflows
            .extend(specs.into_iter().map(WorkflowOpts::into_registration));
        self
    }

    /// Public HTTP listener — declare routes in [`.surfaces`](GatewayOpts::surfaces).
    #[must_use]
    pub fn gateway(mut self, opts: GatewayOpts) -> Self {
        self.gateway = Some(opts.into_config());
        self
    }

    /// Per-node workload governor — compute tokens arbitrate gateway vs job handlers
    /// ([workload governor](../../docs/decisions/workload-governor.md)).
    #[must_use]
    pub fn workload(mut self, opts: trembita_jobs::WorkloadOpts) -> Self {
        self.inner = self.inner.workload(opts);
        self
    }

    /// Enable durable mailbox spool (B-41) — same as [`.configure`](Self::configure)([`TrembitaConfigure::with_durable_mailbox`](crate::TrembitaConfigure::with_durable_mailbox)(true)).
    #[must_use]
    pub fn with_durable_mailbox(mut self, enabled: bool) -> Self {
        self.durable_mailbox = enabled;
        self.inner = self.inner.durable_mailbox(enabled);
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

    fn validate(&self, product_http: Option<std::net::SocketAddr>) -> Result<(), StartError> {
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
        if !self.workflows.is_empty() && self.gateway.is_none() && product_http.is_none() {
            return Err(StartError::Config(
                "`.workflows([…])` requires a product HTTP listener (`TREMBITA_LISTEN` / `.gateway(...)`) with workflow routes"
                    .into(),
            ));
        }
        Ok(())
    }

    async fn finish_start(
        app: TrembitaApp,
        gateway: Option<GatewayConfig>,
        wait_ready: Option<crate::ReadyOpts>,
    ) -> Result<Arc<TrembitaApp>, StartError> {
        let app = Arc::new(app);
        app.cap_runtime().attach_app(Arc::downgrade(&app));
        if let Some(config) = gateway {
            let addr = config.addr;
            let handle = spawn_gateway_task(Arc::clone(&app), config)
                .await
                .map_err(|e| StartError::Config(format!("gateway bind to {addr}: {e}")))?;
            app.install_gateway(handle).await;
        }
        if let Some(opts) = wait_ready
            && app.wait_until_ready(opts).await
        {
            app.scale_plan().emit_boot_log();
        }
        Ok(app)
    }

    async fn boot(self, opts: &mut RunOpts) -> Result<Arc<TrembitaApp>, StartError> {
        if let Some(net) = opts.local_net.as_ref() {
            let builder = self.ensure_product_gateway(None);
            let product_http = builder.gateway.as_ref().map(|g| g.addr);
            builder.validate(product_http)?;
            let workflows = builder.workflows;
            let gateway = builder.gateway;
            let cap_runtime = builder.cap_runtime;
            let cap_deps = builder.cap_deps.unwrap_or_default();
            let scale_plan = builder.scale_plan;
            let coordination_growth_preset = builder.coordination_growth_preset;
            let cluster = builder.inner.start_local(net).await;
            return Self::finish_start(
                TrembitaApp::assemble(
                    cluster,
                    workflows,
                    cap_runtime,
                    cap_deps,
                    scale_plan,
                    coordination_growth_preset,
                ),
                gateway,
                opts.wait_ready.clone(),
            )
            .await;
        }
        let mut builder = self;
        let cfg = match builder.boot_config.take() {
            Some(cfg) => cfg,
            None => {
                let cfg = app_config_from_env().map_err(|e| StartError::Config(e.to_string()))?;
                builder = builder.apply_env_config(&cfg);
                cfg
            }
        };
        builder = builder.ensure_product_gateway(Some(&cfg));
        if builder.durable_mailbox && cfg.data_dir.is_none() {
            return Err(StartError::Config(
                "durable mailbox requires `TREMBITA_DATA_DIR` or TrembitaConfigure::with_data_dir"
                    .into(),
            ));
        }
        builder.validate(cfg.http)?;
        let workflows = builder.workflows;
        let gateway = builder.gateway;
        let cap_runtime = builder.cap_runtime;
        let cap_deps = builder.cap_deps.unwrap_or_default();
        let scale_plan = builder.scale_plan;
        let coordination_growth_preset = builder.coordination_growth_preset;
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
            TrembitaApp::assemble(
                cluster,
                workflows,
                cap_runtime,
                cap_deps,
                scale_plan,
                coordination_growth_preset,
            ),
            gateway,
            opts.wait_ready.clone(),
        )
        .await
    }

    /// Boot using [`RunOpts`] derived from [`Self::from_config`] / registration ([`.manifest`](Self::manifest), [`AppManifest::jobs`](crate::AppManifest::jobs)).
    ///
    /// # Errors
    /// Same as [`Self::run_with`].
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let opts = self.resolve_run_opts()?;
        self.run_with(opts).await
    }

    /// Boot, spawn registered job consumer loops, block on shutdown signal, graceful shutdown.
    ///
    /// Always starts a QUIC cluster member (seed or joiner) from `TREMBITA_*` env.
    ///
    /// # Errors
    /// Returns an error when boot, signal handling, or teardown fails.
    pub async fn run_with(mut self, mut opts: RunOpts) -> Result<(), Box<dyn std::error::Error>> {
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
        app.wait_for_shutdown(opts.shutdown, opts.shutdown_signal)
            .await
    }

    /// Test-only boot without blocking on Ctrl-C ([`trembita_test_facade::boot_local_app`]).
    #[doc(hidden)]
    pub async fn boot_for_test(self, mut opts: RunOpts) -> Result<Arc<TrembitaApp>, StartError> {
        self.boot(&mut opts).await
    }

    /// Like [`Self::boot_for_test`] but also spawns registered queue consumers (capability bridges, …).
    #[doc(hidden)]
    pub async fn boot_for_test_with_consumers(
        mut self,
        mut opts: RunOpts,
    ) -> Result<crate::TestBoot, StartError> {
        let pending = std::mem::take(&mut self.pending_consumers);
        let app = self.boot(&mut opts).await?;
        let consumers = if pending.is_empty() {
            None
        } else {
            let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
            let handles = pending
                .into_iter()
                .map(|spawn| spawn(Arc::clone(&app), stop_rx.clone()))
                .collect();
            Some((stop_tx, handles))
        };
        Ok(crate::TestBoot { app, consumers })
    }

    /// Apply boot tuning ([`TrembitaConfigure`]): `data_dir`, Raft ticks, gateway opt-in flags.
    #[must_use]
    pub fn configure(mut self, config: TrembitaConfigure) -> Self {
        #[cfg(feature = "http-jobs")]
        {
            self.gateway_include_ops = !config.without_ops;
            self.gateway_exclude_apis = GatewayProductApiExclusions {
                jobs: config.without_jobs_api,
                schedules: config.without_schedules_api,
                actors: config.without_actors_api,
                workflows: config.without_workflows_api,
                topics: config.without_topics_api,
            };
        }
        self.coordination_growth_preset = config.coordination_growth_preset;
        self.durable_mailbox = config.durable_mailbox;
        self.scale_plan.set_coordination(
            config.coordination_raft_groups,
            config.coordination_shard_count,
        );
        self.inner = config.apply_to(self.inner);
        self
    }

    /// Omit ops HTTP on the default gateway (`/health`, `/ready`, `/metrics`, …).
    #[cfg(feature = "http-jobs")]
    #[must_use]
    pub fn without_ops(mut self) -> Self {
        self.gateway_include_ops = false;
        self
    }

    /// Omit `POST/GET /jobs/*` on the default gateway (registration may still enqueue via cluster APIs).
    #[cfg(feature = "http-jobs")]
    #[must_use]
    pub fn without_jobs_api(mut self) -> Self {
        self.gateway_exclude_apis.jobs = true;
        self
    }

    /// Omit job schedule HTTP on the default gateway.
    #[cfg(feature = "http-jobs")]
    #[must_use]
    pub fn without_schedules_api(mut self) -> Self {
        self.gateway_exclude_apis.schedules = true;
        self
    }

    /// Omit `/actors/*` on the default gateway (default for product apps; idempotent).
    ///
    /// Legacy HTTP cast/ask requires [`.workers(…)`](crate::AppManifest::workers) with
    /// [`WorkerOpts::http_cast(true)`](crate::WorkerOpts::http_cast) and
    /// [`.with_actors_api()`](Self::with_actors_api).
    #[cfg(feature = "http-jobs")]
    #[must_use]
    pub fn without_actors_api(mut self) -> Self {
        self.gateway_exclude_apis.actors = true;
        self
    }

    /// Allow `/actors/*` when worker groups opt in via [`WorkerOpts::http_cast`](crate::WorkerOpts::http_cast).
    #[cfg(feature = "http-jobs")]
    #[must_use]
    pub fn with_actors_api(mut self) -> Self {
        self.gateway_exclude_apis.actors = false;
        self
    }

    /// Omit `POST /workflows/*` on the default gateway.
    #[cfg(feature = "http-jobs")]
    #[must_use]
    pub fn without_workflows_api(mut self) -> Self {
        self.gateway_exclude_apis.workflows = true;
        self
    }

    /// Omit topic publish/metrics HTTP on the default gateway.
    #[cfg(feature = "http-jobs")]
    #[must_use]
    pub fn without_topics_api(mut self) -> Self {
        self.gateway_exclude_apis.topics = true;
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

#[cfg(all(test, feature = "http-jobs"))]
mod env_listen_gateway_tests {
    use super::TrembitaAppBuilder;

    #[test]
    fn new_default_keeps_gateway_opt_out_defaults() {
        let builder = TrembitaAppBuilder::new_default();
        assert!(!builder.gateway_include_ops);
        assert!(builder.gateway_exclude_apis.jobs);
        assert!(builder.gateway_exclude_apis.actors);
    }

    #[test]
    fn env_listen_defaults_enable_ops_and_product_apis() {
        let mut builder = TrembitaAppBuilder::new_default();
        builder.apply_env_listen_gateway_defaults();
        assert!(builder.gateway_include_ops);
        assert!(!builder.gateway_exclude_apis.jobs);
        assert!(!builder.gateway_exclude_apis.schedules);
        assert!(!builder.gateway_exclude_apis.workflows);
        assert!(!builder.gateway_exclude_apis.topics);
        assert!(builder.gateway_exclude_apis.actors);
    }
}
