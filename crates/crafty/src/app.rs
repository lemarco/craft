//! [`CraftyApp`] — product-facing entry point over [`CraftyCluster`](super::cluster::CraftyCluster)
//! ([product-scenarios](../../../docs/decisions/product-scenarios.md)).

use std::convert::Infallible;
use std::future::Future;
use std::hash::Hash;
#[cfg(feature = "http-jobs")]
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crafty_actor::ClientError;
use crafty_actor::NodeHandle;
use crafty_actor::{
    ActorSession, CastError, ClusterAskError, ClusterRef, EnqueueOptions, JobId, JobStatus,
    LeaseId, UserActor, WorkerId,
};
use crafty_client::{SagaError, SagaOutcome, SagaPlan};
use crafty_core::StateMachine;
use crafty_proto::LogIndex;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::NodeId;
use crate::actor_group::ActorGroupOpts;
use crate::app_opts::RunOpts;
use crate::builder::{CraftyClusterBuilder, StartError};
use crate::cluster::CraftyCluster;
use crate::configure::CraftyConfigure;
use crate::cron_opts::CronOpts;
use crate::env_config::{AppConfig, app_config_from_env};
#[cfg(feature = "http-jobs")]
use crate::gateway::spawn_gateway as spawn_gateway_task;
#[cfg(feature = "http-jobs")]
use crate::gateway::{GatewayConfig, GatewayOpts};
use crate::queue_opts::QueueOpts;

type ConsumerSpawnFn =
    Box<dyn FnOnce(Arc<CraftyApp>, tokio::sync::watch::Receiver<bool>) -> JoinHandle<()> + Send>;

/// Builds [`SagaPlan`] values from HTTP / CLI saga ids.
pub type WorkflowPlanFn = Arc<dyn Fn(&str) -> SagaPlan + Send + Sync>;

type WorkflowRunnerFn = Arc<
    dyn Fn(
            Arc<CraftyApp>,
            SagaPlan,
        ) -> Pin<Box<dyn Future<Output = Result<SagaOutcome, SagaError>> + Send>>
        + Send
        + Sync,
>;

/// Run a workflow using the default keyed client and Meta-Raft journal.
///
/// Pass as the runner to [`.workflows`](CraftyAppBuilder::workflows) when no custom client is needed.
///
/// # Errors
/// Same as [`CraftyApp::run_workflow`].
pub async fn journal_workflow(
    app: Arc<CraftyApp>,
    plan: SagaPlan,
) -> Result<SagaOutcome, SagaError> {
    let client = app.keyed_client();
    app.run_workflow(client.as_ref(), &plan).await
}

/// Options for [`CraftyApp::shutdown_graceful`] and [`CraftyAppBuilder::run`].
#[derive(Debug, Default)]
pub struct ShutdownOpts {
    /// Call [`CraftyCluster::leave`] when the node is in a multi-node cluster.
    pub graceful_leave: bool,
    /// Drain local actor groups before stopping the runtime.
    pub drain_actors: bool,
    /// Stop tier-C consumers: send on the watch sender, then await these handles.
    pub consumers: Option<(tokio::sync::watch::Sender<bool>, Vec<JoinHandle<()>>)>,
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

/// Fluent builder for [`CraftyApp`].
pub struct CraftyAppBuilder {
    inner: CraftyClusterBuilder<EmptyStateMachine>,
    plan_builder: Option<WorkflowPlanFn>,
    workflow_runner: Option<WorkflowRunnerFn>,
    reg_jobs: bool,
    reg_actors: bool,
    pending_consumers: Vec<ConsumerSpawnFn>,
    #[cfg(feature = "http-jobs")]
    gateway: Option<GatewayConfig>,
}

impl CraftyAppBuilder {
    /// Merge env-only settings into `builder` when not already set in code.
    fn apply_env_config(mut self, cfg: &AppConfig) -> Self {
        self.inner = self.inner.merge_app_config(cfg);
        if let Some(stream) = cfg.job_queue_stream.clone()
            && !self.reg_jobs
        {
            self.reg_jobs = true;
            self = self.queue([QueueOpts::new(stream, cfg.job_queue_lease)]);
        }
        #[cfg(feature = "http-jobs")]
        if self.gateway.is_none()
            && let Some(addr) = cfg.gateway
        {
            self.gateway = Some(GatewayConfig {
                addr,
                jobs_api: cfg.gateway_jobs_api,
                actors_api: cfg.gateway_actors_api,
                workflows_api: cfg.gateway_workflows_api,
                routes: None,
            });
        }
        self
    }

    /// Register a tier-C [`JobConsumer`] loop (started in [`Self::run`]).
    #[must_use]
    pub fn consumer<C: crate::JobConsumer>(
        mut self,
        consumer: C,
        opts: crate::ConsumerOpts,
    ) -> Self {
        self.pending_consumers.push(Box::new(move |app, stop| {
            app.spawn_consumer(consumer, opts, stop)
        }));
        self
    }

    /// Register durable job streams (requires [`Self::data_dir`]).
    #[must_use]
    pub fn queue(mut self, queues: impl IntoIterator<Item = QueueOpts>) -> Self {
        for opts in queues {
            self.reg_jobs = true;
            self.inner = self.inner.job_queue(&opts.name, opts.lease);
            self.inner = self.inner.job_queue_prefetch(&opts.name, opts.prefetch);
        }
        self
    }

    /// Register cron-driven recurring enqueues (requires matching [`.queue`](Self::queue) streams).
    #[must_use]
    pub fn cron(mut self, schedules: impl IntoIterator<Item = CronOpts>) -> Self {
        for opts in schedules {
            self.inner = self.inner.recurring_job(&opts.stream, opts.job);
        }
        self
    }

    /// Register an actor group — [`ActorGroupOpts::default`] / [`ActorGroupOpts::new`] = one worker per live node;
    /// [`ActorGroupOpts::fixed`] = fixed pool size cluster-wide.
    #[must_use]
    pub fn actors<A: UserActor>(mut self, name: &str, opts: ActorGroupOpts<A::Config>) -> Self
    where
        A::Config: Clone + Send + Sync + 'static,
    {
        self.reg_actors = true;
        self.inner = match opts.total {
            Some(total) => self.inner.manage::<A>(name, total, opts.config),
            None => self.inner.manage_auto::<A>(name, opts.config),
        };
        self
    }

    /// Register workflow plans and runner for HTTP `/workflows/*` and [`CraftyApp::run_workflow_id`].
    ///
    /// Use [`journal_workflow`] as the runner when the default keyed client + Meta-Raft journal is enough.
    #[must_use]
    pub fn workflows<F, R, Fut>(mut self, plan: F, runner: R) -> Self
    where
        F: Fn(&str) -> SagaPlan + Send + Sync + 'static,
        R: Fn(Arc<CraftyApp>, SagaPlan) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<SagaOutcome, SagaError>> + Send + 'static,
    {
        self.plan_builder = Some(Arc::new(plan));
        self.workflow_runner = Some(Arc::new(move |app, p| Box::pin(runner(app, p))));
        self
    }

    /// Public HTTP gateway — custom routes and optional built-in APIs ([`GatewayOpts`]).
    #[cfg(feature = "http-jobs")]
    #[must_use]
    pub fn gateway(mut self, addr: SocketAddr, opts: GatewayOpts) -> Self {
        self.gateway = Some(opts.into_config(addr));
        self
    }

    async fn finish_start(
        app: CraftyApp,
        #[cfg(feature = "http-jobs")] gateway: Option<GatewayConfig>,
        wait_ready: Option<crate::ReadyOpts>,
    ) -> Result<Arc<CraftyApp>, StartError> {
        let app = Arc::new(app);
        #[cfg(feature = "http-jobs")]
        if let Some(config) = gateway {
            let addr = config.addr;
            spawn_gateway_task(Arc::clone(&app), config)
                .await
                .map_err(|e| StartError::Config(format!("gateway bind to {addr}: {e}")))?;
        }
        if let Some(opts) = wait_ready {
            app.wait_until_ready(opts).await;
        }
        Ok(app)
    }

    async fn boot(self, opts: &mut RunOpts) -> Result<Arc<CraftyApp>, StartError> {
        if let Some(net) = opts.local_net.as_ref() {
            let plan_builder = self.plan_builder;
            let workflow_runner = self.workflow_runner;
            #[cfg(feature = "http-jobs")]
            let gateway = self.gateway;
            let cluster = self.inner.start_local(net).await;
            return Self::finish_start(
                Self::assemble_app(cluster, plan_builder, workflow_runner),
                #[cfg(feature = "http-jobs")]
                gateway,
                opts.wait_ready.clone(),
            )
            .await;
        }
        let cfg = app_config_from_env().map_err(|e| StartError::Config(e.to_string()))?;
        let mut builder = self;
        builder = builder.apply_env_config(&cfg);
        let plan_builder = builder.plan_builder;
        let workflow_runner = builder.workflow_runner;
        #[cfg(feature = "http-jobs")]
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
            Self::assemble_app(cluster, plan_builder, workflow_runner),
            #[cfg(feature = "http-jobs")]
            gateway,
            opts.wait_ready.clone(),
        )
        .await
    }

    /// Boot, spawn registered [`Self::consumer`] loops, block on Ctrl-C, graceful shutdown.
    ///
    /// Always starts a QUIC cluster member (seed or joiner) from `CRAFTY_*` env.
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

    /// Test-only boot without blocking on Ctrl-C ([`crafty_test_support::boot_local_app`]).
    #[doc(hidden)]
    pub async fn boot_for_test(self, mut opts: RunOpts) -> Result<Arc<CraftyApp>, StartError> {
        self.boot(&mut opts).await
    }

    fn assemble_app(
        cluster: CraftyCluster<EmptyStateMachine>,
        plan_builder: Option<WorkflowPlanFn>,
        workflow_runner: Option<WorkflowRunnerFn>,
    ) -> CraftyApp {
        CraftyApp {
            cluster,
            plan_builder,
            workflow_runner,
            workflow_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Persistent `data_dir` — enables redb job queue and actor workflow store.
    #[must_use]
    pub fn data_dir(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.inner = self.inner.data_dir(path);
        self
    }

    /// Apply runtime / cluster tuning ([`CraftyConfigure`]).
    #[must_use]
    pub fn configure(mut self, config: CraftyConfigure) -> Self {
        self.inner = config.apply_to(self.inner);
        self
    }

    /// Access the underlying cluster builder for advanced options.
    #[must_use]
    pub fn inner_mut(&mut self) -> &mut CraftyClusterBuilder<EmptyStateMachine> {
        &mut self.inner
    }
}

/// Running product app handle ([`EmptyStateMachine`] by default).
pub struct CraftyApp {
    cluster: CraftyCluster<EmptyStateMachine>,
    plan_builder: Option<WorkflowPlanFn>,
    workflow_runner: Option<WorkflowRunnerFn>,
    workflow_lock: Arc<Mutex<()>>,
}

impl CraftyApp {
    /// Begin configuring an app. Always runs as a QUIC cluster member (seed or joiner) via `CRAFTY_*` env in [`.run`](CraftyAppBuilder::run).
    #[must_use]
    pub fn builder() -> CraftyAppBuilder {
        CraftyAppBuilder {
            inner: CraftyClusterBuilder::new(NodeId(1), EmptyStateMachine),
            plan_builder: None,
            workflow_runner: None,
            reg_jobs: false,
            reg_actors: false,
            pending_consumers: Vec::new(),
            #[cfg(feature = "http-jobs")]
            gateway: None,
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

    /// Underlying cluster handle for advanced APIs.
    #[must_use]
    pub fn cluster(&self) -> &CraftyCluster<EmptyStateMachine> {
        &self.cluster
    }

    /// Consume and return the inner [`CraftyCluster`].
    #[must_use]
    pub fn into_cluster(self) -> CraftyCluster<EmptyStateMachine> {
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

    /// Workflow store when [`CraftyClusterBuilder::data_dir`] / auto durable store is enabled.
    #[must_use]
    pub fn actor_state_store(&self) -> Option<Arc<dyn crafty_actor::ActorStateStore>> {
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
    ) -> Result<JobId, crafty_actor::QueueError> {
        let queue = self.cluster.job_queue(stream).ok_or_else(|| {
            crafty_actor::QueueError::Backend(format!("unknown stream {stream:?}"))
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
    ) -> Result<JobId, crafty_actor::QueueError> {
        let queue = self.cluster.job_queue(stream).ok_or_else(|| {
            crafty_actor::QueueError::Backend(format!("unknown stream {stream:?}"))
        })?;
        queue.enqueue_opts(payload, options).await
    }

    /// Enqueue many jobs in one leader transaction (tier C batch path).
    ///
    /// Batches are capped at [`crate::DEFAULT_QUEUE_BATCH_MAX`] jobs per RPC.
    ///
    /// # Errors
    /// Returns an error when the stream is unknown or enqueue fails.
    pub async fn enqueue_batch(
        &self,
        stream: &str,
        payloads: &[&[u8]],
    ) -> Result<Vec<JobId>, crafty_actor::QueueError> {
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
    ) -> Result<Vec<JobId>, crafty_actor::QueueError> {
        self.cluster.enqueue_batch_opts(stream, jobs).await
    }

    /// Acknowledge many leased jobs in one leader transaction (tier C batch path).
    ///
    /// # Errors
    /// Returns an error when the stream is unknown or ack fails.
    pub async fn ack_batch(
        &self,
        stream: &str,
        worker: WorkerId,
        lease_ids: &[LeaseId],
    ) -> Result<(), crafty_actor::QueueError> {
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
    ) -> Result<Option<JobStatus>, crafty_actor::QueueError> {
        let queue = self.cluster.job_queue(stream).ok_or_else(|| {
            crafty_actor::QueueError::Backend(format!("unknown stream {stream:?}"))
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
    ) -> Result<(), crafty_actor::QueueError> {
        self.cluster.requeue_dead_letter(stream, job_id).await
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
    pub fn keyed_client(&self) -> Arc<crafty_client::RemoteClient> {
        self.cluster.keyed_client()
    }

    /// Gracefully stop consumers, drain actors, optionally leave the cluster, then shutdown.
    pub async fn shutdown_graceful(&self, opts: ShutdownOpts) {
        if let Some((stop_tx, handles)) = opts.consumers {
            let _ = stop_tx.send(true);
            for handle in handles {
                let _ = handle.await;
            }
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

    /// Shutdown opts derived from standard `CRAFTY_*` env (graceful leave flag).
    #[must_use]
    pub fn shutdown_opts_from_env() -> ShutdownOpts {
        ShutdownOpts {
            graceful_leave: crate::env_config::app_config_from_env()
                .map_or(true, |c| c.graceful_leave),
            drain_actors: true,
            consumers: None,
        }
    }

    async fn run_workflow_plan(self: &Arc<Self>, plan: SagaPlan) -> Result<SagaOutcome, SagaError> {
        let _guard = self.workflow_lock.lock().await;
        let runner = self.workflow_runner.as_ref().ok_or(SagaError::Journal(
            crafty_client::SagaJournalError::Backend(
                "workflows require `.workflows(plan, runner)`".into(),
            ),
        ))?;
        runner(Arc::clone(self), plan).await
    }

    /// Run a workflow by saga id (requires `.workflows(plan, runner)` on the builder).
    ///
    /// # Errors
    /// Returns [`SagaError`] when no plan builder was configured or execution fails.
    pub async fn run_workflow_id(
        self: &Arc<Self>,
        saga_id: &str,
    ) -> Result<SagaOutcome, SagaError> {
        let plan_builder = self.plan_builder.as_ref().ok_or(SagaError::Journal(
            crafty_client::SagaJournalError::Backend(
                "workflows require `.workflows(plan, runner)`".into(),
            ),
        ))?;
        let plan = plan_builder(saga_id);
        self.run_workflow_plan(plan).await
    }

    /// Resume a workflow by saga id (requires `.workflows(plan, runner)` on the builder).
    ///
    /// # Errors
    /// Returns [`SagaError`] when no plan builder was configured or resume fails.
    pub async fn resume_workflow_id(
        self: &Arc<Self>,
        saga_id: &str,
    ) -> Result<SagaOutcome, SagaError> {
        let plan_builder = self.plan_builder.as_ref().ok_or(SagaError::Journal(
            crafty_client::SagaJournalError::Backend(
                "workflows require `.workflows(plan, runner)`".into(),
            ),
        ))?;
        let plan = plan_builder(saga_id);
        let _guard = self.workflow_lock.lock().await;
        let client = self.keyed_client();
        self.resume_workflow(client.as_ref(), &plan).await
    }

    /// HTTP workflow trigger API. Requires `http-jobs` feature and `.workflows(plan, runner)`.
    #[cfg(feature = "http-jobs")]
    pub fn workflows_api(app: Arc<Self>) -> crafty_http::WorkflowsApi {
        let run_app = Arc::clone(&app);
        let resume_app = app;
        crafty_http::WorkflowsApi::new(
            Arc::new(move |saga_id| {
                let app = Arc::clone(&run_app);
                Box::pin(async move {
                    let outcome = app
                        .run_workflow_id(&saga_id)
                        .await
                        .map_err(|e| crafty_http::WorkflowsApiError::Failed(e.to_string()))?;
                    Ok(workflow_accepted(&saga_id, &outcome))
                })
            }),
            Arc::new(move |saga_id| {
                let app = Arc::clone(&resume_app);
                Box::pin(async move {
                    let outcome = app
                        .resume_workflow_id(&saga_id)
                        .await
                        .map_err(|e| crafty_http::WorkflowsApiError::Failed(e.to_string()))?;
                    Ok(workflow_accepted(&saga_id, &outcome))
                })
            }),
        )
    }

    /// Spawn the product HTTP / WebSocket gateway on a background task.
    ///
    /// Requires `http-jobs` feature and an [`Arc`] handle so routes can call into the app.
    ///
    /// # Errors
    /// Returns [`std::io::Error`] when the listen socket cannot be bound.
    #[cfg(feature = "http-jobs")]
    pub async fn spawn_gateway(app: Arc<Self>, config: GatewayConfig) -> std::io::Result<()> {
        crate::gateway::spawn_gateway(app, config).await
    }

    /// HTTP job enqueue API (`POST /jobs/{stream}` → `202`). Requires `http-jobs` feature.
    ///
    /// Pass an [`Arc`] handle so the Axum service can enqueue from any task:
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use crafty::CraftyApp;
    /// # async fn demo(app: Arc<CraftyApp>) {
    /// let _api = CraftyApp::jobs_api(app);
    /// # }
    /// ```
    #[cfg(feature = "http-jobs")]
    pub fn jobs_api(app: Arc<Self>) -> crafty_http::JobsApi {
        let enqueue_app = Arc::clone(&app);
        let batch_app = Arc::clone(&app);
        let ack_app = Arc::clone(&app);
        let status_app = Arc::clone(&app);
        let requeue_app = app;
        crafty_http::JobsApi::new(
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
            Arc::new(move |stream, job_id| {
                let app = Arc::clone(&requeue_app);
                Box::pin(async move { app.requeue_dead_letter(&stream, JobId(job_id)).await })
            }),
        )
    }

    /// HTTP actor cast / ask API. Requires `http-jobs` feature.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use crafty::CraftyApp;
    /// # async fn demo(app: Arc<CraftyApp>) {
    /// let _api = CraftyApp::actors_api(app);
    /// # }
    /// ```
    #[cfg(feature = "http-jobs")]
    pub fn actors_api(app: Arc<Self>) -> crafty_http::ActorsApi {
        let ask_app = Arc::clone(&app);
        let cast_app = app;
        crafty_http::ActorsApi::new(
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
    /// Same as [`CraftyCluster::run_keyed_saga`](crate::CraftyCluster::run_keyed_saga).
    pub async fn run_workflow<C: crafty_client::KeyedClient>(
        &self,
        client: &C,
        plan: &crafty_client::SagaPlan,
    ) -> Result<crafty_client::SagaOutcome, crafty_client::SagaError> {
        let journal = self.cluster.saga_journal();
        self.cluster
            .run_keyed_saga(client, plan, journal.as_ref())
            .await
    }

    /// Resume a workflow from the durable journal after crash or partial progress.
    ///
    /// # Errors
    /// Same as [`CraftyCluster::resume_keyed_saga`](crate::CraftyCluster::resume_keyed_saga).
    pub async fn resume_workflow<C: crafty_client::KeyedClient>(
        &self,
        client: &C,
        plan: &crafty_client::SagaPlan,
    ) -> Result<crafty_client::SagaOutcome, crafty_client::SagaError> {
        let journal = self.cluster.saga_journal();
        self.cluster
            .resume_keyed_saga(client, plan, journal.as_ref())
            .await
    }
}

#[cfg(feature = "http-jobs")]
fn workflow_accepted(saga_id: &str, outcome: &SagaOutcome) -> crafty_http::WorkflowAccepted {
    let label = match outcome {
        SagaOutcome::Completed(_) => "completed",
        SagaOutcome::Compensated { .. } => "compensated",
    };
    crafty_http::WorkflowAccepted {
        saga_id: saga_id.to_string(),
        outcome: label.to_string(),
    }
}
