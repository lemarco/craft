use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use trembita_client::{SagaError, SagaOutcome, SagaPlan};
use trembita_jobs::{EnqueueOptions, JobId, JobQueue, JobStatus, LeaseId, WorkerId};
use trembita_runtime::{
    ActorRegistry, ActorSession, CastError, ClientError, ClusterAskError, ClusterControl,
    ClusterRef, ClusterSupervisor, NodeHandle,
};

use crate::NodeId;
use crate::cluster_handle::{ClusterFacts, TrembitaCluster};
use crate::gateway::{GatewayConfig, GatewayHandle};
use crate::workflow::WorkflowBuilder;
use crate::workflow_opts::{WorkflowRegistration, resolve_workflow};

use super::builder::TrembitaAppBuilder;
use super::shutdown::ShutdownOpts;
use super::types::{EmptyStateMachine, WorkerInfo};

/// Running product app handle ([`EmptyStateMachine`] by default).
pub struct TrembitaApp {
    cluster: TrembitaCluster<EmptyStateMachine>,
    workflows: Vec<WorkflowRegistration>,
    workflow_lock: Arc<Mutex<()>>,
    gateway: tokio::sync::Mutex<Option<GatewayHandle>>,
}

impl TrembitaApp {
    pub(crate) fn assemble(
        cluster: TrembitaCluster<EmptyStateMachine>,
        workflows: Vec<WorkflowRegistration>,
    ) -> Self {
        Self {
            cluster,
            workflows,
            workflow_lock: Arc::new(Mutex::new(())),
            gateway: tokio::sync::Mutex::new(None),
        }
    }

    /// Begin configuring an app. Always runs as a QUIC cluster member (seed or joiner) via `TREMBITA_*` env in [`.run`](TrembitaAppBuilder::run).
    #[must_use]
    pub fn builder() -> TrembitaAppBuilder {
        TrembitaAppBuilder::new_default()
    }

    pub(crate) async fn install_gateway(&self, handle: GatewayHandle) {
        *self.gateway.lock().await = Some(handle);
    }

    /// Block until Ctrl-C / SIGINT, then [`Self::shutdown_graceful`].
    pub(crate) async fn wait_for_shutdown(
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
    pub fn event_topic(&self, name: &str) -> Option<Arc<dyn trembita_events::EventTopic>> {
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
    ) -> Result<trembita_events::EventId, trembita_events::TopicError> {
        let t = self.event_topic(topic).ok_or_else(|| {
            trembita_events::TopicError::NotFound(format!("unknown topic {topic:?}"))
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
    pub fn actor_state_store(&self) -> Option<Arc<dyn trembita_actor_store::ActorStateStore>> {
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
    ) -> Result<JobId, trembita_jobs::QueueError> {
        let queue = self.cluster.job_queue(stream).ok_or_else(|| {
            trembita_jobs::QueueError::Backend(format!("unknown stream {stream:?}"))
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
    ) -> Result<JobId, trembita_jobs::QueueError> {
        let queue = self.cluster.job_queue(stream).ok_or_else(|| {
            trembita_jobs::QueueError::Backend(format!("unknown stream {stream:?}"))
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
    ) -> Result<JobId, trembita_jobs::QueueError> {
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
    ) -> Result<Vec<JobId>, trembita_jobs::QueueError> {
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
    ) -> Result<Vec<JobId>, trembita_jobs::QueueError> {
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
    ) -> Result<(), trembita_jobs::QueueError> {
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
    ) -> Result<Option<JobStatus>, trembita_jobs::QueueError> {
        let queue = self.cluster.job_queue(stream).ok_or_else(|| {
            trembita_jobs::QueueError::Backend(format!("unknown stream {stream:?}"))
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
    ) -> Result<(), trembita_jobs::QueueError> {
        self.cluster.requeue_dead_letter(stream, job_id).await
    }

    /// List jobs in a stream with optional filters (admin inspection).
    ///
    /// # Errors
    /// Returns an error when the stream is unknown or listing fails.
    pub async fn list_jobs(
        &self,
        stream: &str,
        filter: trembita_jobs::JobListFilter,
    ) -> Result<trembita_jobs::JobListPage, trembita_jobs::QueueError> {
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
    ) -> Result<trembita_jobs::BatchRequeueResult, trembita_jobs::QueueError> {
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
    /// Returns [`GatewaySpawnError`] when config is invalid or the listen socket cannot be bound.
    pub async fn spawn_gateway(
        app: Arc<Self>,
        config: GatewayConfig,
    ) -> Result<GatewayHandle, crate::gateway::GatewaySpawnError> {
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

    /// Live [`Observer`](trembita_dashboard::Observer) for admin and gateway introspection routes.
    #[must_use]
    pub fn introspect_observer(&self) -> Arc<dyn trembita_dashboard::Observer> {
        self.cluster.introspect_observer()
    }

    /// HTTP introspection API (`GET /introspect/*`). Requires `http-jobs` feature.
    ///
    /// Pair with [`TrembitaApp::jobs_api`] when the operator UI also lists or requeues jobs.
    #[cfg(feature = "http-jobs")]
    #[must_use]
    pub fn introspect_api(&self) -> trembita_http::IntrospectApi {
        trembita_http::IntrospectApi::new(self.introspect_observer())
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
