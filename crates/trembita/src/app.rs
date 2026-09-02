//! [`TrembitaApp`] — product-facing entry point over [`TrembitaCluster`](crate::cluster_handle::TrembitaCluster)
//! ([product-scenarios](../../../docs/decisions/product-scenarios.md)).

use std::collections::HashSet;
use std::convert::Infallible;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use trembita_actor::ClientError;
use trembita_actor::NodeHandle;
use trembita_actor::{
    ActorRegistry, ActorSession, CastError, ClusterAskError, ClusterControl, ClusterRef,
    ClusterSupervisor, EnqueueOptions, JobId, JobQueue, JobStatus, LeaseId, UserActor, WorkerId,
};
use trembita_client::{SagaError, SagaOutcome, SagaPlan};
use trembita_core::StateMachine;
use trembita_proto::LogIndex;

use crate::NodeId;
use crate::actor_group::ActorGroupOpts;
use crate::app_opts::RunOpts;
use crate::builder::{StartError, TrembitaClusterBuilder};
use crate::cluster_handle::{ClusterFacts, TrembitaCluster};
use crate::configure::TrembitaConfigure;
use crate::consumer::ConsumerSpawnFn;
use crate::cron_opts::CronOpts;
use crate::env_config::{AppConfig, app_config_from_env};
use crate::gateway::spawn_gateway as spawn_gateway_task;
use crate::gateway::{GatewayConfig, GatewayHandle, GatewayOpts};
use crate::job_opts::JobOpts;
use crate::queue_opts::QueueOpts;
use crate::worker_opts::{WorkerGroup, WorkerOpts};
use crate::workflow::WorkflowBuilder;
use crate::workflow_opts::{WorkflowOpts, WorkflowRegistration, resolve_workflow};
use trembita_dashboard::MetricsSink;

/// Run a workflow using the default keyed client and Meta-Raft journal.
///
/// Pass as the runner to [`.workflows`](TrembitaAppBuilder::workflows) when no custom client is needed.
///
/// # Errors
/// Same as [`TrembitaApp::run_workflow`].
pub async fn journal_workflow(
    app: Arc<TrembitaApp>,
    plan: SagaPlan,
) -> Result<SagaOutcome, SagaError> {
    let client = app.keyed_client();
    app.run_workflow(client.as_ref(), &plan).await
}

/// Options for [`TrembitaApp::shutdown_graceful`] and [`TrembitaAppBuilder::run`].
#[derive(Debug)]
pub struct ShutdownOpts {
    /// Call [`TrembitaCluster::leave`](crate::cluster::TrembitaCluster::leave) when the node is in a multi-node cluster.
    pub graceful_leave: bool,
    /// Drain local actor groups before stopping the runtime.
    pub drain_actors: bool,
    /// Stop job consumers: send on the watch sender, then await these handles.
    pub consumers: Option<(tokio::sync::watch::Sender<bool>, Vec<JoinHandle<()>>)>,
    /// Drain the product HTTP gateway (WebSocket / long-lived HTTP) before shutdown.
    pub drain_gateway: bool,
    /// Max wait for job queue consumer tasks after the stop signal.
    pub consumer_drain_timeout: Duration,
}

impl Default for ShutdownOpts {
    fn default() -> Self {
        Self {
            graceful_leave: true,
            drain_actors: true,
            consumers: None,
            drain_gateway: true,
            consumer_drain_timeout: crate::gateway::DEFAULT_CONSUMER_DRAIN_TIMEOUT,
        }
    }
}

/// A worker instance registered in the cluster directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerInfo {
    /// Hosting cluster node.
    pub node: u64,
    /// Worker actor instance id on that node.
    pub instance: u32,
}

/// Minimal state machine for actor-only / queue-only applications.
#[derive(Debug, Default, Clone, Copy)]
pub struct EmptyStateMachine;

impl StateMachine for EmptyStateMachine {
    type Command = ();
    type Query = ();
    type Response = ();
    type Error = Infallible;

    fn apply(&mut self, _index: LogIndex, _command: &()) -> Result<(), Self::Error> {
        Ok(())
    }

    fn query(&self, _query: &()) -> Result<(), Self::Error> {
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<u8>, Self::Error> {
        Ok(Vec::new())
    }

    fn restore(&mut self, _snapshot: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Job/actor registration toggles on [`TrembitaAppBuilder`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TrembitaAppRegistrationFlags {
    pub(crate) jobs: bool,
    pub(crate) actors: bool,
}

/// Gateway built-in API toggles on [`TrembitaAppBuilder`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TrembitaAppGatewayApiFlags {
    pub(crate) jobs: bool,
    pub(crate) actors: bool,
}

/// Fluent builder for [`TrembitaApp`].
pub struct TrembitaAppBuilder {
    pub(crate) inner: TrembitaClusterBuilder<EmptyStateMachine>,
    workflows: Vec<WorkflowRegistration>,
    pub(crate) registration: TrembitaAppRegistrationFlags,
    queue_streams: HashSet<String>,
    cron_streams: Vec<String>,
    schedule_streams: Vec<String>,
    consumer_streams: Vec<String>,
    pending_consumers: Vec<ConsumerSpawnFn>,
    pub(crate) gateway: Option<GatewayConfig>,
    pub(crate) config_errors: Vec<String>,
    pub(crate) gateway_api: TrembitaAppGatewayApiFlags,
    pub(crate) worker_autoscale_streams: Vec<String>,
}

impl TrembitaAppBuilder {
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
            let mut opts = GatewayOpts::new(addr)
                .with_jobs_api(cfg.gateway_jobs_api)
                .with_actors_api(cfg.gateway_actors_api)
                .with_workflows_api(cfg.gateway_workflows_api)
                .drain_timeout(cfg.gateway_drain_timeout);
            if let Some((cert, key)) = cfg.gateway_tls.clone() {
                opts = opts.tls(cert, key);
            }
            self.gateway = Some(opts.into_config());
        } else if let Some(gateway) = self.gateway.as_mut()
            && gateway.tls.is_none()
            && let Some((cert, key)) = cfg.gateway_tls.clone()
        {
            gateway.tls = Some(crate::gateway::GatewayTlsPaths { cert, key });
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
            self.inner = self.inner.event_topic(&opts.name, opts.lease);
            self.inner = self.inner.event_topic_retention(&opts.name, opts.retention);
            if !opts.subscriptions.is_empty() {
                self.inner = self
                    .inner
                    .event_topic_subscriptions(&opts.name, &opts.subscriptions);
            }
        }
        self
    }

    /// Register cron-driven recurring enqueues (requires matching [`.queue`](Self::queue) streams).
    ///
    /// Implemented as a [`StaticScheduleSource`](trembita_actor::StaticScheduleSource) —
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
        source: Arc<dyn trembita_actor::ScheduleSource>,
        poll: trembita_actor::SchedulePoll,
    ) -> Self {
        let stream = stream.into();
        self.schedule_streams.push(stream.clone());
        self.inner = self.inner.schedule_source(&stream, source, poll);
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
    /// Use [`WorkflowOpts::new`] + [`journal_workflow`] when the default keyed client is enough.
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
    pub fn workload(mut self, opts: trembita_actor::WorkloadOpts) -> Self {
        self.inner = self.inner.workload(opts);
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
            *app.gateway.lock().await = Some(handle);
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
                Self::assemble_app(cluster, workflows),
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
            Self::assemble_app(cluster, workflows),
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

    fn assemble_app(
        cluster: TrembitaCluster<EmptyStateMachine>,
        workflows: Vec<WorkflowRegistration>,
    ) -> TrembitaApp {
        TrembitaApp {
            cluster,
            workflows,
            workflow_lock: Arc::new(Mutex::new(())),
            gateway: tokio::sync::Mutex::new(None),
        }
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

/// Running product app handle ([`EmptyStateMachine`] by default).
pub struct TrembitaApp {
    cluster: TrembitaCluster<EmptyStateMachine>,
    workflows: Vec<WorkflowRegistration>,
    workflow_lock: Arc<Mutex<()>>,
    gateway: tokio::sync::Mutex<Option<GatewayHandle>>,
}

impl TrembitaApp {
    /// Begin configuring an app. Always runs as a QUIC cluster member (seed or joiner) via `TREMBITA_*` env in [`.run`](TrembitaAppBuilder::run).
    #[must_use]
    pub fn builder() -> TrembitaAppBuilder {
        TrembitaAppBuilder {
            inner: TrembitaClusterBuilder::new(NodeId(1), EmptyStateMachine),
            workflows: Vec::new(),
            registration: TrembitaAppRegistrationFlags::default(),
            queue_streams: HashSet::new(),
            cron_streams: Vec::new(),
            schedule_streams: Vec::new(),
            consumer_streams: Vec::new(),
            pending_consumers: Vec::new(),
            gateway: None,
            config_errors: Vec::new(),
            gateway_api: TrembitaAppGatewayApiFlags::default(),
            worker_autoscale_streams: Vec::new(),
        }
    }

    /// Block until Ctrl-C / SIGINT, then [`Self::shutdown_graceful`].
    async fn wait_for_shutdown(
        self: Arc<Self>,
        opts: ShutdownOpts,
    ) -> Result<(), Box<dyn std::error::Error>> {
        tokio::signal::ctrl_c().await?;
        self.shutdown_graceful(opts).await;
        Ok(())
    }

    /// This node's cluster id.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.cluster.node_id()
    }

    /// Actor spawn / migrate control plane.
    #[must_use]
    pub fn control(&self) -> &Arc<ClusterControl> {
        self.cluster.control()
    }

    /// Local actor registry (handles for registered worker types).
    #[must_use]
    pub fn registry(&self) -> &ActorRegistry {
        self.cluster.registry()
    }

    /// Leader-only worker placement reconciler.
    #[must_use]
    pub fn supervisor(&self) -> &Arc<ClusterSupervisor<Arc<ClusterFacts>>> {
        self.cluster.supervisor()
    }

    /// Registered job stream handle (`None` when unknown or not mounted yet).
    #[must_use]
    pub fn job_queue(&self, stream: &str) -> Option<Arc<dyn JobQueue>> {
        self.cluster.job_queue(stream)
    }

    /// Look up a registered durable topic by name.
    #[must_use]
    pub fn event_topic(&self, name: &str) -> Option<Arc<dyn trembita_actor::EventTopic>> {
        self.cluster.event_topic(name)
    }

    /// Publish one event to a registered topic ([event-topics](../../docs/decisions/event-topics.md)).
    ///
    /// # Errors
    /// Returns an error when the topic is unknown or publish fails.
    pub async fn publish(
        &self,
        topic: &str,
        payload: &[u8],
    ) -> Result<trembita_actor::EventId, trembita_actor::TopicError> {
        let t = self.event_topic(topic).ok_or_else(|| {
            trembita_actor::TopicError::NotFound(format!("unknown topic {topic:?}"))
        })?;
        t.publish(payload).await
    }

    /// Whether this node is the Raft leader on the default group.
    pub async fn is_leader(&self) -> bool {
        self.cluster.is_leader().await
    }

    /// Stop background tasks (does not drain actors — use [`Self::shutdown_graceful`] in production).
    pub fn shutdown(&self) {
        self.cluster.shutdown();
    }

    /// Low-level cluster handle — tests and custom state machines only.
    #[doc(hidden)]
    #[must_use]
    pub fn cluster(&self) -> &TrembitaCluster<EmptyStateMachine> {
        &self.cluster
    }

    /// Consume the inner cluster handle — tests and custom state machines only.
    #[doc(hidden)]
    #[must_use]
    pub fn into_cluster(self) -> TrembitaCluster<EmptyStateMachine> {
        self.cluster
    }

    /// In-process Raft handle (propose / query).
    #[must_use]
    pub fn handle(&self) -> &NodeHandle<EmptyStateMachine> {
        self.cluster.handle()
    }

    /// Propose a command on the default Raft group (actor-only apps use [`EmptyStateMachine`]).
    ///
    /// # Errors
    /// Returns [`ClientError`] when the proposal fails or the node is not leader.
    pub async fn propose(&self, command: ()) -> Result<(), ClientError> {
        self.handle().propose(command).await
    }

    /// Workflow store when [`TrembitaClusterBuilder::data_dir`](crate::cluster::TrembitaClusterBuilder::data_dir) / auto durable store is enabled.
    #[must_use]
    pub fn actor_state_store(&self) -> Option<Arc<dyn trembita_actor::ActorStateStore>> {
        self.cluster.actor_state_store()
    }

    /// Enqueue on a registered job stream.
    ///
    /// # Errors
    /// Returns an error when the stream is unknown or enqueue fails.
    pub async fn enqueue(
        &self,
        stream: &str,
        payload: &[u8],
    ) -> Result<JobId, trembita_actor::QueueError> {
        let queue = self.cluster.job_queue(stream).ok_or_else(|| {
            trembita_actor::QueueError::Backend(format!("unknown stream {stream:?}"))
        })?;
        queue.enqueue(payload).await
    }

    /// Enqueue with options (priority, dedup, delay).
    ///
    /// # Errors
    /// Returns an error when the stream is unknown or enqueue fails.
    pub async fn enqueue_opts(
        &self,
        stream: &str,
        payload: &[u8],
        options: EnqueueOptions,
    ) -> Result<JobId, trembita_actor::QueueError> {
        let queue = self.cluster.job_queue(stream).ok_or_else(|| {
            trembita_actor::QueueError::Backend(format!("unknown stream {stream:?}"))
        })?;
        queue.enqueue_opts(payload, options).await
    }

    /// Enqueue a workflow step with a saga-scoped dedup key ([`WorkflowBuilder::step_dedup_key`]).
    ///
    /// # Errors
    /// Returns an error when the stream is unknown or enqueue fails.
    pub async fn enqueue_workflow_step(
        &self,
        saga_id: &str,
        step_id: &str,
        stream: &str,
        payload: &[u8],
    ) -> Result<JobId, trembita_actor::QueueError> {
        self.enqueue_opts(
            stream,
            payload,
            EnqueueOptions::dedup_key(WorkflowBuilder::step_dedup_key(saga_id, step_id)),
        )
        .await
    }

    /// Enqueue many jobs in one leader transaction (batch path).
    ///
    /// Batches are capped at [`crate::cluster::DEFAULT_QUEUE_BATCH_MAX`] jobs per RPC.
    ///
    /// # Errors
    /// Returns an error when the stream is unknown or enqueue fails.
    pub async fn enqueue_batch(
        &self,
        stream: &str,
        payloads: &[&[u8]],
    ) -> Result<Vec<JobId>, trembita_actor::QueueError> {
        self.cluster.enqueue_batch(stream, payloads).await
    }

    /// Enqueue many jobs with per-job options in one leader transaction.
    ///
    /// # Errors
    /// Returns an error when the stream is unknown or enqueue fails.
    pub async fn enqueue_batch_opts(
        &self,
        stream: &str,
        jobs: &[(Vec<u8>, EnqueueOptions)],
    ) -> Result<Vec<JobId>, trembita_actor::QueueError> {
        self.cluster.enqueue_batch_opts(stream, jobs).await
    }

    /// Acknowledge many leased jobs in one leader transaction (batch path).
    ///
    /// # Errors
    /// Returns an error when the stream is unknown or ack fails.
    pub async fn ack_batch(
        &self,
        stream: &str,
        worker: WorkerId,
        lease_ids: &[LeaseId],
    ) -> Result<(), trembita_actor::QueueError> {
        self.cluster.ack_batch(stream, worker, lease_ids).await
    }

    /// Lookup job metadata by id (`None` when acked or unknown).
    ///
    /// # Errors
    /// Returns an error when the stream is unknown or lookup fails.
    pub async fn job_status(
        &self,
        stream: &str,
        job_id: JobId,
    ) -> Result<Option<JobStatus>, trembita_actor::QueueError> {
        let queue = self.cluster.job_queue(stream).ok_or_else(|| {
            trembita_actor::QueueError::Backend(format!("unknown stream {stream:?}"))
        })?;
        queue.job_status(job_id).await
    }

    /// Move a dead-letter job back to the pending queue for retry.
    ///
    /// # Errors
    /// Returns an error when the stream is unknown, the job is not in dead-letter, or requeue fails.
    pub async fn requeue_dead_letter(
        &self,
        stream: &str,
        job_id: JobId,
    ) -> Result<(), trembita_actor::QueueError> {
        self.cluster.requeue_dead_letter(stream, job_id).await
    }

    /// List jobs in a stream with optional filters (admin inspection).
    ///
    /// # Errors
    /// Returns an error when the stream is unknown or listing fails.
    pub async fn list_jobs(
        &self,
        stream: &str,
        filter: trembita_actor::JobListFilter,
    ) -> Result<trembita_actor::JobListPage, trembita_actor::QueueError> {
        self.cluster.list_jobs(stream, filter).await
    }

    /// Requeue many dead-letter jobs; partial success is allowed.
    ///
    /// # Errors
    /// Returns an error when the stream is unknown or the whole batch request fails.
    pub async fn requeue_dead_letter_batch(
        &self,
        stream: &str,
        job_ids: &[JobId],
    ) -> Result<trembita_actor::BatchRequeueResult, trembita_actor::QueueError> {
        self.cluster
            .requeue_dead_letter_batch(stream, job_ids)
            .await
    }

    /// Worker group names known cluster-wide (from the actor directory).
    #[must_use]
    pub fn worker_groups(&self) -> Vec<String> {
        self.cluster.directory().groups()
    }

    /// Instances registered for a worker group.
    #[must_use]
    pub fn workers(&self, group: &str) -> Vec<WorkerInfo> {
        self.cluster
            .directory()
            .lookup(group)
            .into_iter()
            .map(|reg| WorkerInfo {
                node: reg.id.node.0,
                instance: reg.id.instance,
            })
            .collect()
    }

    /// Round-robin cast to any instance in `group`.
    ///
    /// # Errors
    /// Returns [`CastError::NoTarget`] when the group has no live workers.
    pub async fn cast(&self, group: &str, payload: Vec<u8>) -> Result<(), CastError> {
        self.cluster.messaging().cast(group, payload).await
    }

    /// Round-robin ask (request/reply) to any instance in `group`.
    ///
    /// # Errors
    /// Returns [`ClusterAskError`] when the group has no workers, delivery fails, or the handler
    /// does not reply within the ask deadline.
    pub async fn ask(&self, group: &str, payload: Vec<u8>) -> Result<Vec<u8>, ClusterAskError> {
        self.cluster.messaging().ask(group, payload).await
    }

    /// Cast to a sticky session opened via [`Self::session`].
    ///
    /// # Errors
    /// Returns [`CastError`] when the session target is gone or delivery fails.
    pub async fn cast_session(
        &self,
        session: &ActorSession,
        payload: Vec<u8>,
    ) -> Result<(), CastError> {
        self.cluster
            .messaging()
            .cast_session(session, payload)
            .await
    }

    /// Ask through a sticky session opened via [`Self::session`].
    ///
    /// # Errors
    /// Returns [`ClusterAskError`] when the session target is gone, delivery fails, or the
    /// handler does not reply within the ask deadline.
    pub async fn ask_session(
        &self,
        session: &ActorSession,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, ClusterAskError> {
        self.cluster.messaging().ask_session(session, payload).await
    }

    /// Open a sticky session to a keyed worker pool (product helper).
    pub fn session<K: Hash>(
        &self,
        group: &str,
        key: &K,
        ttl: Option<Duration>,
    ) -> Option<ActorSession> {
        self.session_keyed(group, key, ttl)
    }

    /// Like [`Self::session`] but accepts a string routing key (WebSocket users, tenants).
    pub fn session_str(
        &self,
        group: &str,
        key: &str,
        ttl: Option<Duration>,
    ) -> Option<ActorSession> {
        self.cluster_ref(group).session_str(key, ttl)
    }

    /// Open a sticky session to a keyed worker pool.
    pub fn session_keyed<K: Hash>(
        &self,
        group: &str,
        key: &K,
        ttl: Option<Duration>,
    ) -> Option<ActorSession> {
        self.cluster_ref(group).session_keyed(key, ttl)
    }

    /// Cluster-wide view of a worker group name.
    #[must_use]
    pub fn cluster_ref(&self, group: &str) -> ClusterRef {
        self.cluster.directory().cluster(group)
    }

    /// Poll until Raft elected a leader and optional job streams are mounted.
    ///
    /// Returns `true` when ready. On timeout, returns `false` (does not panic).
    pub async fn wait_until_ready(&self, opts: crate::ReadyOpts) -> bool {
        self.cluster.wait_until_ready(opts).await
    }

    /// Keyed client for cross-shard sagas / workflows.
    #[must_use]
    pub fn keyed_client(&self) -> Arc<trembita_client::RemoteClient> {
        self.cluster.keyed_client()
    }

    /// Gracefully stop consumers, drain gateway + actors, optionally leave the cluster, then shutdown.
    pub async fn shutdown_graceful(&self, opts: ShutdownOpts) {
        if let Some((stop_tx, handles)) = opts.consumers {
            let _ = stop_tx.send(true);
            let drain = async {
                for handle in handles {
                    let _ = handle.await;
                }
            };
            if opts.consumer_drain_timeout.is_zero() {
                drain.await;
            } else {
                let _ = tokio::time::timeout(opts.consumer_drain_timeout, drain).await;
            }
        }
        if opts.drain_gateway
            && let Some(handle) = self.gateway.lock().await.take()
        {
            handle.drain().await;
        }
        if opts.drain_actors {
            for name in self.cluster.registry().names() {
                let _ = self.cluster.stop_group_graceful(&name).await;
            }
        }
        if opts.graceful_leave && self.cluster.members().len() > 1 {
            let _ = self.cluster.leave().await;
        }
        self.cluster.shutdown();
    }

    /// Shutdown opts derived from standard `TREMBITA_*` env (graceful leave flag).
    #[must_use]
    pub fn shutdown_opts_from_env() -> ShutdownOpts {
        ShutdownOpts {
            graceful_leave: crate::env_config::app_config_from_env()
                .map_or(true, |c| c.graceful_leave),
            drain_actors: true,
            consumers: None,
            drain_gateway: true,
            consumer_drain_timeout: crate::gateway::DEFAULT_CONSUMER_DRAIN_TIMEOUT,
        }
    }

    async fn run_workflow_plan(
        self: &Arc<Self>,
        saga_id: &str,
        plan: SagaPlan,
    ) -> Result<SagaOutcome, SagaError> {
        let _guard = self.workflow_lock.lock().await;
        let spec = resolve_workflow(&self.workflows, saga_id)?;
        (spec.runner)(Arc::clone(self), plan).await
    }

    /// Run a workflow by saga id (requires `.workflows([…])` on the builder).
    ///
    /// # Errors
    /// Returns [`SagaError`] when no workflow was configured or execution fails.
    pub async fn run_workflow_id(
        self: &Arc<Self>,
        saga_id: &str,
    ) -> Result<SagaOutcome, SagaError> {
        let spec = resolve_workflow(&self.workflows, saga_id)?;
        let plan = (spec.plan)(saga_id);
        self.run_workflow_plan(saga_id, plan).await
    }

    /// Resume a workflow by saga id (requires `.workflows([…])` on the builder).
    ///
    /// # Errors
    /// Returns [`SagaError`] when no workflow was configured or resume fails.
    pub async fn resume_workflow_id(
        self: &Arc<Self>,
        saga_id: &str,
    ) -> Result<SagaOutcome, SagaError> {
        let spec = resolve_workflow(&self.workflows, saga_id)?;
        let plan = (spec.plan)(saga_id);
        let _guard = self.workflow_lock.lock().await;
        let client = self.keyed_client();
        self.resume_workflow(client.as_ref(), &plan).await
    }

    /// HTTP workflow trigger API. Requires `http-jobs` feature and `.workflows(plan, runner)`.
    #[cfg(feature = "http-jobs")]
    pub fn workflows_api(app: Arc<Self>) -> trembita_http::WorkflowsApi {
        let run_app = Arc::clone(&app);
        let resume_app = app;
        trembita_http::WorkflowsApi::new(
            Arc::new(move |saga_id| {
                let app = Arc::clone(&run_app);
                Box::pin(async move {
                    let outcome = app
                        .run_workflow_id(&saga_id)
                        .await
                        .map_err(|e| trembita_http::WorkflowsApiError::Failed(e.to_string()))?;
                    Ok(workflow_accepted(&saga_id, &outcome))
                })
            }),
            Arc::new(move |saga_id| {
                let app = Arc::clone(&resume_app);
                Box::pin(async move {
                    let outcome = app
                        .resume_workflow_id(&saga_id)
                        .await
                        .map_err(|e| trembita_http::WorkflowsApiError::Failed(e.to_string()))?;
                    Ok(workflow_accepted(&saga_id, &outcome))
                })
            }),
        )
    }

    /// Spawn the product HTTP / WebSocket gateway on a background task.
    ///
    /// Requires an [`Arc`] handle so routes can call into the app.
    ///
    /// # Errors
    /// Returns [`std::io::Error`] when the listen socket cannot be bound.
    pub async fn spawn_gateway(
        app: Arc<Self>,
        config: GatewayConfig,
    ) -> std::io::Result<GatewayHandle> {
        crate::gateway::spawn_gateway(app, config).await
    }

    /// HTTP job enqueue API (`POST /jobs/{stream}` → `202`). Requires `http-jobs` feature.
    ///
    /// Pass an [`Arc`] handle so the Axum service can enqueue from any task:
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use trembita::TrembitaApp;
    /// # async fn demo(app: Arc<TrembitaApp>) {
    /// let _api = TrembitaApp::jobs_api(app);
    /// # }
    /// ```
    #[cfg(feature = "http-jobs")]
    pub fn jobs_api(app: Arc<Self>) -> trembita_http::JobsApi {
        let enqueue_app = Arc::clone(&app);
        let batch_app = Arc::clone(&app);
        let ack_app = Arc::clone(&app);
        let status_app = Arc::clone(&app);
        let list_app = Arc::clone(&app);
        let requeue_app = Arc::clone(&app);
        let requeue_batch_app = app;
        trembita_http::JobsApi::new(
            Arc::new(move |stream, payload, opts| {
                let app = Arc::clone(&enqueue_app);
                Box::pin(async move {
                    if opts == EnqueueOptions::default() {
                        app.enqueue(&stream, &payload).await
                    } else {
                        app.enqueue_opts(&stream, &payload, opts).await
                    }
                })
            }),
            Arc::new(move |stream, jobs| {
                let app = Arc::clone(&batch_app);
                Box::pin(async move { app.enqueue_batch_opts(&stream, &jobs).await })
            }),
            Arc::new(move |stream, worker, lease_ids| {
                let app = Arc::clone(&ack_app);
                Box::pin(async move { app.ack_batch(&stream, worker, &lease_ids).await })
            }),
            Arc::new(move |stream, job_id| {
                let app = Arc::clone(&status_app);
                Box::pin(async move { app.job_status(&stream, JobId(job_id)).await })
            }),
            Arc::new(move |stream, filter| {
                let app = Arc::clone(&list_app);
                Box::pin(async move { app.list_jobs(&stream, filter).await })
            }),
            Arc::new(move |stream, job_id| {
                let app = Arc::clone(&requeue_app);
                Box::pin(async move { app.requeue_dead_letter(&stream, JobId(job_id)).await })
            }),
            Arc::new(move |stream, job_ids| {
                let app = Arc::clone(&requeue_batch_app);
                Box::pin(async move {
                    app.requeue_dead_letter_batch(
                        &stream,
                        &job_ids.iter().copied().map(JobId).collect::<Vec<_>>(),
                    )
                    .await
                })
            }),
        )
    }

    /// HTTP actor cast / ask API. Requires `http-jobs` feature.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use trembita::TrembitaApp;
    /// # async fn demo(app: Arc<TrembitaApp>) {
    /// let _api = TrembitaApp::actors_api(app);
    /// # }
    /// ```
    #[cfg(feature = "http-jobs")]
    pub fn actors_api(app: Arc<Self>) -> trembita_http::ActorsApi {
        let ask_app = Arc::clone(&app);
        let cast_app = app;
        trembita_http::ActorsApi::new(
            Arc::new(move |group, payload| {
                let app = Arc::clone(&ask_app);
                Box::pin(async move { app.ask(&group, payload).await })
            }),
            Arc::new(move |group, payload| {
                let app = Arc::clone(&cast_app);
                Box::pin(async move { app.cast(&group, payload).await })
            }),
        )
    }

    /// Run a cross-shard workflow using the node's default saga journal.
    ///
    /// # Errors
    /// Same as [`TrembitaCluster::run_keyed_saga`](crate::cluster::TrembitaCluster::run_keyed_saga).
    pub async fn run_workflow<C: trembita_client::KeyedClient>(
        &self,
        client: &C,
        plan: &trembita_client::SagaPlan,
    ) -> Result<trembita_client::SagaOutcome, trembita_client::SagaError> {
        let journal = self.cluster.saga_journal();
        self.cluster
            .run_keyed_saga(client, plan, journal.as_ref())
            .await
    }

    /// Resume a workflow from the durable journal after crash or partial progress.
    ///
    /// # Errors
    /// Same as [`TrembitaCluster::resume_keyed_saga`](crate::cluster::TrembitaCluster::resume_keyed_saga).
    pub async fn resume_workflow<C: trembita_client::KeyedClient>(
        &self,
        client: &C,
        plan: &trembita_client::SagaPlan,
    ) -> Result<trembita_client::SagaOutcome, trembita_client::SagaError> {
        let journal = self.cluster.saga_journal();
        self.cluster
            .resume_keyed_saga(client, plan, journal.as_ref())
            .await
    }
}

#[cfg(feature = "http-jobs")]
fn workflow_accepted(saga_id: &str, outcome: &SagaOutcome) -> trembita_http::WorkflowAccepted {
    let label = match outcome {
        SagaOutcome::Completed(_) => "completed",
        SagaOutcome::Compensated { .. } => "compensated",
    };
    trembita_http::WorkflowAccepted {
        saga_id: saga_id.to_string(),
        outcome: label.to_string(),
    }
}
