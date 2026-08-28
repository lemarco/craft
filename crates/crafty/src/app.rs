//! [`CraftyApp`] — product-facing entry point over [`CraftyCluster`](super::cluster::CraftyCluster)
//! ([product-scenarios](../../../docs/decisions/product-scenarios.md)).

use std::convert::Infallible;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use crafty_actor::ClientError;
use crafty_actor::NodeHandle;
use crafty_actor::{ActorSession, ClusterRef, EnqueueOptions, JobId, UserActor};
use crafty_core::StateMachine;
use crafty_net::LocalNetwork;
use crafty_proto::LogIndex;

use crate::NodeId;
use crate::builder::{CraftyClusterBuilder, StartError};
use crate::cluster::CraftyCluster;
use crate::env_config::{AppConfig, app_config_from_env};
use crate::security::Security;

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
}

impl CraftyAppBuilder {
    /// Start from environment variables (`CRAFTY_NODE_ID`, `CRAFTY_DATA_DIR`, …).
    ///
    /// # Errors
    /// Returns an error when required environment variables are invalid.
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let cfg = app_config_from_env()?;
        Ok(Self::from_config(&cfg))
    }

    /// Apply a parsed [`AppConfig`].
    #[must_use]
    pub fn from_config(cfg: &AppConfig) -> Self {
        let mut inner = CraftyClusterBuilder::new(cfg.node_id, EmptyStateMachine);
        if !cfg.members.is_empty() {
            inner = inner.members(cfg.members.clone());
        }
        if let Some(dir) = cfg.data_dir.clone() {
            inner = inner.data_dir(dir);
        }
        if let Some(stream) = cfg.job_queue_stream.clone() {
            inner = inner.job_queue(&stream, cfg.job_queue_lease);
        }
        inner = inner
            .allow_join(cfg.allow_join)
            .allow_leave(cfg.allow_leave)
            .drain_timeout(cfg.drain_timeout);
        if !cfg.join_seeds.is_empty() {
            inner = inner.join_seeds(cfg.join_seeds.clone());
        }
        Self { inner }
    }

    /// Explicit node id (overrides env when chaining before `from_env`).
    #[must_use]
    pub fn node_id(mut self, node_id: NodeId) -> Self {
        self.inner = CraftyClusterBuilder::new(node_id, EmptyStateMachine);
        self
    }

    /// Persistent `data_dir` — enables redb job queue and actor workflow store.
    #[must_use]
    pub fn data_dir(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.inner = self.inner.data_dir(path);
        self
    }

    /// Register a durable job stream (requires [`Self::data_dir`]).
    #[must_use]
    pub fn job_stream(mut self, name: &str, lease_timeout: Duration) -> Self {
        self.inner = self.inner.job_queue(name, lease_timeout);
        self
    }

    /// Register a managed auto-worker group (one worker per live node).
    #[must_use]
    pub fn manage_auto<A: UserActor>(mut self, name: &str, config: A::Config) -> Self
    where
        A::Config: Clone + Send + Sync + 'static,
    {
        self.inner = self.inner.manage_auto::<A>(name, config);
        self
    }

    /// Register a fixed-size managed worker pool across the cluster.
    #[must_use]
    pub fn manage<A: UserActor>(mut self, name: &str, total: usize, config: A::Config) -> Self
    where
        A::Config: Clone + Send + Sync + 'static,
    {
        self.inner = self.inner.manage::<A>(name, total, config);
        self
    }

    /// Cluster voters / bootstrap members.
    #[must_use]
    pub fn members(mut self, members: impl IntoIterator<Item = NodeId>) -> Self {
        self.inner = self.inner.members(members);
        self
    }

    /// Join seeds for dynamic cluster growth.
    #[must_use]
    pub fn join_seeds(mut self, seeds: impl IntoIterator<Item = crate::discovery::Seed>) -> Self {
        self.inner = self.inner.join_seeds(seeds);
        self
    }

    /// Access the underlying cluster builder for advanced options.
    #[must_use]
    pub fn inner_mut(&mut self) -> &mut CraftyClusterBuilder<EmptyStateMachine> {
        &mut self.inner
    }

    /// Wall-clock duration of one logical Raft tick.
    #[must_use]
    pub fn tick_period(mut self, period: Duration) -> Self {
        self.inner = self.inner.tick_period(period);
        self
    }

    /// Leader supervisor reconcile interval.
    #[must_use]
    pub fn reconcile_period(mut self, period: Duration) -> Self {
        self.inner = self.inner.reconcile_period(period);
        self
    }

    /// Actor directory publish interval.
    #[must_use]
    pub fn directory_publish_period(mut self, period: Duration) -> Self {
        self.inner = self.inner.directory_publish_period(period);
        self
    }

    /// Start over the in-memory [`LocalNetwork`] (tests / local dev).
    pub async fn start_local(self, net: &LocalNetwork) -> CraftyApp {
        let cluster = self.inner.start_local(net).await;
        CraftyApp { cluster }
    }

    /// Start over QUIC with mTLS [`Security`].
    ///
    /// # Errors
    /// Returns [`StartError`] when bind or join fails.
    pub async fn start_quic(
        self,
        security: Security,
        listen: std::net::SocketAddr,
        peers: crafty_net::PeerDirectory,
    ) -> Result<CraftyApp, StartError> {
        let cluster = self.inner.start_quic(security, listen, peers).await?;
        Ok(CraftyApp { cluster })
    }
}

/// Running product app handle ([`EmptyStateMachine`] by default).
pub struct CraftyApp {
    cluster: CraftyCluster<EmptyStateMachine>,
}

impl CraftyApp {
    /// Begin configuring an app for `node_id`.
    #[must_use]
    pub fn builder(node_id: NodeId) -> CraftyAppBuilder {
        CraftyAppBuilder {
            inner: CraftyClusterBuilder::new(node_id, EmptyStateMachine),
        }
    }

    /// Start over QUIC using a parsed [`AppConfig`].
    ///
    /// # Errors
    /// Returns [`StartError`] when bind or join fails.
    pub async fn start_from_config(cfg: AppConfig) -> Result<Self, StartError> {
        let builder = CraftyAppBuilder::from_config(&cfg);
        let listen = cfg.listen;
        let peers = cfg.peers;
        let security = cfg.security;
        builder.start_quic(security, listen, peers).await
    }

    /// Parse `CRAFTY_*` environment variables and start over QUIC.
    ///
    /// # Errors
    /// Returns an error when env parsing or cluster start fails.
    pub async fn start_from_env() -> Result<Self, StartError> {
        let cfg = app_config_from_env().map_err(|e| StartError::Config(e.to_string()))?;
        Self::start_from_config(cfg).await
    }

    /// Parse `CRAFTY_*` environment variables and return a pre-wired builder.
    ///
    /// # Errors
    /// Returns an error when environment configuration is invalid.
    pub fn from_env() -> Result<CraftyAppBuilder, Box<dyn std::error::Error>> {
        CraftyAppBuilder::from_env()
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

    /// Block until Ctrl-C / SIGINT, then shut down the node.
    ///
    /// # Errors
    /// Returns an error when the signal handler fails to install.
    pub async fn run_until_shutdown(self) -> Result<(), Box<dyn std::error::Error>> {
        tokio::signal::ctrl_c().await?;
        self.cluster.shutdown();
        Ok(())
    }

    /// HTTP job enqueue API (`POST /jobs/{stream}` → `202`). Requires `http-jobs` feature.
    ///
    /// Pass an [`Arc`] handle so the Axum service can enqueue from any task:
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # async fn demo(app: Arc<crafty::CraftyApp>) {
    /// let api = CraftyApp::jobs_api(app);
    /// let router = api.router().with_state(Arc::new(api.into_state()));
    /// # let _ = router;
    /// # }
    /// ```
    #[cfg(feature = "http-jobs")]
    pub fn jobs_api(app: Arc<Self>) -> crafty_http::JobsApi {
        crafty_http::JobsApi::new(Arc::new(move |stream, payload, opts| {
            let app = Arc::clone(&app);
            Box::pin(async move {
                if opts == EnqueueOptions::default() {
                    app.enqueue(&stream, &payload).await
                } else {
                    app.enqueue_opts(&stream, &payload, opts).await
                }
            })
        }))
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
