//! HTTP product helpers for trembita ([background-jobs](../../docs/scenarios/background-jobs.md)).
//!
//! # Jobs API
//!
//! [`JobsApi`] exposes:
//!
//! - `POST /jobs/{stream}` → `202 Accepted` + `{ "job_id": … }` (optional `run_at_ms` / `delay_ms` query or JSON for one-shot scheduling)
//! - `POST /jobs/{stream}/batch` → `202 Accepted` + `{ "job_ids": […] }`
//! - `POST /jobs/{stream}/ack-batch` → `200 OK` + `{ "acked": N }`
//! - `GET /jobs/{stream}` → list jobs with optional filters
//! - `POST /jobs/{stream}/requeue-batch` → `200 OK` + `{ "requeued": […], "failures": […] }`
//! - `POST /jobs/{stream}/{id}/requeue` → `200 OK` + `{ "job_id": … }`
//! - `GET /jobs/{stream}/{id}` → job metadata when the queue supports lookup
//!
//! # Schedules API
//!
//! [`SchedulesApi`] exposes (with [`JobsApi`] on the same gateway when `http_enqueue` is enabled):
//!
//! - `GET /jobs/{stream}/schedules` → list recurring cron schedules
//! - `PUT /jobs/{stream}/schedules/{name}` → upsert schedule (JSON body)
//! - `DELETE /jobs/{stream}/schedules/{name}` → remove schedule
//!
//! # Topics API
//!
//! [`TopicsApi`] exposes:
//!
//! - `POST /topics/{name}/publish` → `202 Accepted` + `{ "event_id": … }`
//! - `GET /topics/{name}` → topic depth / subscription lag snapshot
//!
//! # Gateway surfaces
//!
//! [`Gateway`] and [`Surface`] declare host-based product HTTP in 0.4.0 — see
//! [gateway-routing-v2](../../docs/decisions/gateway-routing-v2.md).
//!
//! # Static sites
//!
//! [`StaticSite`] serves SPAs from embedded bytes, a filesystem path, or S3-compatible
//! storage — see [`StaticSource`].

mod actor_routes;
mod actor_types;
mod cookie_config;
mod gateway;
mod host;
mod introspect_routes;
mod introspect_types;
mod ops_routes;
mod routes;
mod routing;
mod schedule_routes;
mod static_site;
mod topic_routes;
mod topic_types;
mod types;
mod upgrade_routes;
mod upgrade_types;
mod workflow_routes;
mod workflow_types;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use http::{HeaderMap, Method, Uri};
use trembita_jobs::{
    BatchRequeueResult, EnqueueOptions, JobId, JobListFilter, JobListPage, JobStatus, LeaseId,
    QueueError, RecurringJob, WorkerId,
};
use trembita_runtime::{CastError, ClusterAskError};

pub use actor_types::{ActorsApiError, AskAccepted};
pub use cookie_config::CookieConfig;
pub use gateway::{
    CorsPolicy, Gateway, GatewayBuildError, GatewayDispatch, GatewayService, Surface,
    UpgradeStream, accept_websocket, is_websocket_upgrade, routing_to_http_response,
};
pub use host::{is_local_dev_host, normalize_host};
pub use introspect_types::IntrospectApiError;
pub use ops_routes::{OpsApi, OpsApiState, ops_route_table};
pub use routes::parse_enqueue_body;
pub use routing::{
    ArcHandler, AuthMode, DispatchGates, Handler, HttpError, IdentityAuthFn, PathParams,
    PathPattern, PathSegment, RequestCtx, Response, ResponseBody, RouteDescriptor, RouteEntry,
    RouteTable, RouteTableDiff, SessionGate,
};
pub use static_site::{
    EmbeddedAssets, EmbeddedFile, Precompressed, StaticSite, StaticSiteEnvError, StaticSiteError,
    StaticSource, embedded_from_dir,
};
#[cfg(feature = "static-s3")]
pub use static_site::{ObjectStoreConfig, S3Delivery};
pub use topic_types::{
    PublishAccepted, TopicMetricsResponse, TopicSubscriptionMetricsResponse, TopicsApiError,
};
pub use trembita_dashboard::{
    ActorView, ClusterView, NodeSummary, NodeView, Observer, QueueStreamView, QueuesView,
    RaftGroupSummary, RaftGroupsView, Readiness, SagaRecordView,
};
pub use types::{
    AckBatchAccepted, AckBatchBody, EnqueueAccepted, EnqueueBatchAccepted, EnqueueBatchBody,
    EnqueueBatchJobBody, EnqueueJsonBody, JobListResponse, JobStatusResponse, JobsApiError,
    LeasedByResponse, RequeueAccepted, RequeueBatchAccepted, RequeueBatchBody,
    RequeueFailureResponse, ScheduleJson, ScheduleListResponse, SchedulesApiError,
};
pub use upgrade_routes::{UpgradeApi, UpgradeApiState, route_table as upgrade_route_table};
pub use upgrade_types::{SetDesiredBody, UpgradeApiError, UpgradeStatusResponse};

/// Async enqueue hook used by [`JobsApi`].
pub type EnqueueFn = Arc<
    dyn Fn(
            String,
            Vec<u8>,
            EnqueueOptions,
        ) -> Pin<Box<dyn Future<Output = Result<JobId, QueueError>> + Send>>
        + Send
        + Sync,
>;

/// Async batch enqueue hook used by [`JobsApi`].
pub type EnqueueBatchFn = Arc<
    dyn Fn(
            String,
            Vec<(Vec<u8>, EnqueueOptions)>,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<JobId>, QueueError>> + Send>>
        + Send
        + Sync,
>;

/// Async batch ack hook used by [`JobsApi`].
pub type AckBatchFn = Arc<
    dyn Fn(
            String,
            WorkerId,
            Vec<LeaseId>,
        ) -> Pin<Box<dyn Future<Output = Result<(), QueueError>> + Send>>
        + Send
        + Sync,
>;

/// Async job lookup hook used by [`JobsApi`].
pub type JobStatusFn = Arc<
    dyn Fn(
            String,
            u64,
        ) -> Pin<Box<dyn Future<Output = Result<Option<JobStatus>, QueueError>> + Send>>
        + Send
        + Sync,
>;

/// Async job list hook used by [`JobsApi`].
pub type ListJobsFn = Arc<
    dyn Fn(
            String,
            JobListFilter,
        ) -> Pin<Box<dyn Future<Output = Result<JobListPage, QueueError>> + Send>>
        + Send
        + Sync,
>;

/// Async batch dead-letter requeue hook used by [`JobsApi`].
pub type RequeueDeadLetterBatchFn = Arc<
    dyn Fn(
            String,
            Vec<u64>,
        ) -> Pin<Box<dyn Future<Output = Result<BatchRequeueResult, QueueError>> + Send>>
        + Send
        + Sync,
>;

/// Async dead-letter requeue hook used by [`JobsApi`].
pub type RequeueDeadLetterFn = Arc<
    dyn Fn(String, u64) -> Pin<Box<dyn Future<Output = Result<(), QueueError>> + Send>>
        + Send
        + Sync,
>;

/// Async list-schedules hook used by [`SchedulesApi`].
pub type ListSchedulesFn = Arc<
    dyn Fn(
            String,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<Vec<trembita_proto::RecurringScheduleWire>, QueueError>>
                    + Send,
            >,
        > + Send
        + Sync,
>;

/// Async upsert-schedule hook used by [`SchedulesApi`].
pub type UpsertScheduleFn = Arc<
    dyn Fn(String, RecurringJob) -> Pin<Box<dyn Future<Output = Result<(), QueueError>> + Send>>
        + Send
        + Sync,
>;

/// Async remove-schedule hook used by [`SchedulesApi`].
pub type RemoveScheduleFn = Arc<
    dyn Fn(String, String) -> Pin<Box<dyn Future<Output = Result<(), QueueError>> + Send>>
        + Send
        + Sync,
>;

/// Optional async auth hook for product gateway routes.
pub type AuthFn = Arc<
    dyn Fn(Method, Uri, HeaderMap) -> Pin<Box<dyn Future<Output = Result<(), JobsApiError>> + Send>>
        + Send
        + Sync,
>;

/// Convert legacy [`AuthFn`] (handler-level product API auth) to [`IdentityAuthFn`].
#[must_use]
pub fn auth_fn_to_identity(auth: AuthFn) -> routing::IdentityAuthFn {
    Arc::new(move |method, uri, headers| {
        let auth = Arc::clone(&auth);
        Box::pin(async move {
            auth(method, uri, headers)
                .await
                .map_err(|e| routing::HttpError::Unauthorized(e.to_string()))
        })
    })
}

/// Shared state for job routes.
pub struct JobsApiState {
    pub(crate) enqueue: EnqueueFn,
    pub(crate) enqueue_batch: EnqueueBatchFn,
    pub(crate) ack_batch: AckBatchFn,
    pub(crate) job_status: JobStatusFn,
    pub(crate) list_jobs: ListJobsFn,
    pub(crate) requeue_dead_letter: RequeueDeadLetterFn,
    pub(crate) requeue_dead_letter_batch: RequeueDeadLetterBatchFn,
}

/// HTTP job enqueue + lookup API.
#[derive(Clone)]
pub struct JobsApi {
    enqueue: EnqueueFn,
    enqueue_batch: EnqueueBatchFn,
    ack_batch: AckBatchFn,
    job_status: JobStatusFn,
    list_jobs: ListJobsFn,
    requeue_dead_letter: RequeueDeadLetterFn,
    requeue_dead_letter_batch: RequeueDeadLetterBatchFn,
}

impl JobsApi {
    /// Build from custom enqueue, batch, and job-status closures.
    #[must_use]
    pub fn new(
        enqueue: EnqueueFn,
        enqueue_batch: EnqueueBatchFn,
        ack_batch: AckBatchFn,
        job_status: JobStatusFn,
        list_jobs: ListJobsFn,
        requeue_dead_letter: RequeueDeadLetterFn,
        requeue_dead_letter_batch: RequeueDeadLetterBatchFn,
    ) -> Self {
        Self {
            enqueue,
            enqueue_batch,
            ack_batch,
            job_status,
            list_jobs,
            requeue_dead_letter,
            requeue_dead_letter_batch,
        }
    }

    /// Route table for job enqueue and lookup.
    #[must_use]
    pub fn route_table(&self) -> RouteTable {
        self.route_table_with_auth(None)
    }

    /// Route table with optional gateway auth hook.
    ///
    /// When `auth` is `Some`, routes use [`AuthMode::Identity`]; the hook itself is
    /// wired on the gateway via [`auth_fn_to_identity`], not stored in handler state.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn route_table_with_auth(&self, auth: Option<AuthFn>) -> RouteTable {
        let table = routes::route_table(Arc::new(self.clone().into_state()));
        if auth.is_some() {
            table.with_auth_mode(routing::AuthMode::Identity)
        } else {
            table
        }
    }

    /// State handle for route tables.
    #[must_use]
    pub fn into_state(self) -> JobsApiState {
        JobsApiState {
            enqueue: self.enqueue,
            enqueue_batch: self.enqueue_batch,
            ack_batch: self.ack_batch,
            job_status: self.job_status,
            list_jobs: self.list_jobs,
            requeue_dead_letter: self.requeue_dead_letter,
            requeue_dead_letter_batch: self.requeue_dead_letter_batch,
        }
    }

    /// Alias of [`Self::into_state`]. Auth is applied via [`Self::route_table_with_auth`].
    #[must_use]
    pub fn into_state_with_auth(self, _auth: Option<AuthFn>) -> JobsApiState {
        self.into_state()
    }
}

/// HTTP recurring schedule admin (`GET/PUT/DELETE /jobs/{stream}/schedules/...`).
#[derive(Clone)]
pub struct SchedulesApi {
    list: ListSchedulesFn,
    upsert: UpsertScheduleFn,
    remove: RemoveScheduleFn,
}

impl SchedulesApi {
    /// Build from custom schedule admin closures.
    #[must_use]
    pub fn new(list: ListSchedulesFn, upsert: UpsertScheduleFn, remove: RemoveScheduleFn) -> Self {
        Self {
            list,
            upsert,
            remove,
        }
    }

    /// Route table for schedule admin routes.
    #[must_use]
    pub fn route_table(&self) -> RouteTable {
        self.route_table_with_auth(None)
    }

    /// Route table with optional gateway auth hook.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn route_table_with_auth(&self, auth: Option<AuthFn>) -> RouteTable {
        let table = schedule_routes::route_table(Arc::new(schedule_routes::SchedulesApiState {
            list: Arc::clone(&self.list),
            upsert: Arc::clone(&self.upsert),
            remove: Arc::clone(&self.remove),
        }));
        if auth.is_some() {
            table.with_auth_mode(routing::AuthMode::Identity)
        } else {
            table
        }
    }
}

/// Async ask hook used by [`ActorsApi`].
pub type AskFn = Arc<
    dyn Fn(
            String,
            Vec<u8>,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, ClusterAskError>> + Send>>
        + Send
        + Sync,
>;

/// Async cast hook used by [`ActorsApi`].
pub type CastFn = Arc<
    dyn Fn(String, Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), CastError>> + Send>>
        + Send
        + Sync,
>;

/// Shared state for actor routes.
pub struct ActorsApiState {
    pub(crate) ask: AskFn,
    pub(crate) cast: CastFn,
}

/// HTTP actor cast / ask API.
#[derive(Clone)]
pub struct ActorsApi {
    ask: AskFn,
    cast: CastFn,
}

impl ActorsApi {
    /// Build from custom ask and cast closures.
    #[must_use]
    pub fn new(ask: AskFn, cast: CastFn) -> Self {
        Self { ask, cast }
    }

    /// Route table for actor cast and ask.
    #[must_use]
    pub fn route_table(&self) -> RouteTable {
        self.route_table_with_auth(None)
    }

    /// Route table with optional gateway auth hook.
    ///
    /// When `auth` is `Some`, routes use [`AuthMode::Identity`]; the hook itself is
    /// wired on the gateway via [`auth_fn_to_identity`], not stored in handler state.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn route_table_with_auth(&self, auth: Option<AuthFn>) -> RouteTable {
        let table = actor_routes::route_table(Arc::new(self.clone().into_state()));
        if auth.is_some() {
            table.with_auth_mode(routing::AuthMode::Identity)
        } else {
            table
        }
    }

    /// State handle for route tables.
    #[must_use]
    pub fn into_state(self) -> ActorsApiState {
        ActorsApiState {
            ask: self.ask,
            cast: self.cast,
        }
    }

    /// Alias of [`Self::into_state`]. Auth is applied via [`Self::route_table_with_auth`].
    #[must_use]
    pub fn into_state_with_auth(self, _auth: Option<AuthFn>) -> ActorsApiState {
        self.into_state()
    }
}

pub use workflow_types::{SagaBody, WorkflowAccepted, WorkflowsApiError};

/// Async publish hook used by [`TopicsApi`].
pub type PublishTopicFn = Arc<
    dyn Fn(String, Vec<u8>) -> Pin<Box<dyn Future<Output = Result<u64, String>> + Send>>
        + Send
        + Sync,
>;

/// Async metrics hook used by [`TopicsApi`].
pub type TopicMetricsFn = Arc<
    dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<TopicMetricsResponse, String>> + Send>>
        + Send
        + Sync,
>;

/// Shared state for topic routes.
pub struct TopicsApiState {
    pub(crate) publish: PublishTopicFn,
    pub(crate) metrics: TopicMetricsFn,
}

/// HTTP event topic publish + metrics API.
#[derive(Clone)]
pub struct TopicsApi {
    publish: PublishTopicFn,
    metrics: TopicMetricsFn,
}

impl TopicsApi {
    /// Build from custom publish and metrics closures.
    #[must_use]
    pub fn new(publish: PublishTopicFn, metrics: TopicMetricsFn) -> Self {
        Self { publish, metrics }
    }

    /// Route table for topic publish and metrics.
    #[must_use]
    pub fn route_table(&self) -> RouteTable {
        self.route_table_with_auth(None)
    }

    /// Route table with optional gateway auth hook.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn route_table_with_auth(&self, auth: Option<AuthFn>) -> RouteTable {
        let table = topic_routes::route_table(Arc::new(self.clone().into_state()));
        if auth.is_some() {
            table.with_auth_mode(routing::AuthMode::Identity)
        } else {
            table
        }
    }

    /// State handle for route tables.
    #[must_use]
    pub fn into_state(self) -> TopicsApiState {
        TopicsApiState {
            publish: self.publish,
            metrics: self.metrics,
        }
    }

    /// Alias of [`Self::into_state`]. Auth is applied via [`Self::route_table_with_auth`].
    #[must_use]
    pub fn into_state_with_auth(self, _auth: Option<AuthFn>) -> TopicsApiState {
        self.into_state()
    }
}

/// Async run hook used by [`WorkflowsApi`].
pub type RunWorkflowFn = Arc<
    dyn Fn(
            String,
        )
            -> Pin<Box<dyn Future<Output = Result<WorkflowAccepted, WorkflowsApiError>> + Send>>
        + Send
        + Sync,
>;

/// Async resume hook used by [`WorkflowsApi`].
pub type ResumeWorkflowFn = Arc<
    dyn Fn(
            String,
        )
            -> Pin<Box<dyn Future<Output = Result<WorkflowAccepted, WorkflowsApiError>> + Send>>
        + Send
        + Sync,
>;

/// Shared state for workflow routes.
pub struct WorkflowsApiState {
    pub(crate) run: RunWorkflowFn,
    pub(crate) resume: ResumeWorkflowFn,
}

/// HTTP keyed-saga trigger API.
#[derive(Clone)]
pub struct WorkflowsApi {
    run: RunWorkflowFn,
    resume: ResumeWorkflowFn,
}

impl WorkflowsApi {
    /// Build from custom run and resume closures.
    #[must_use]
    pub fn new(run: RunWorkflowFn, resume: ResumeWorkflowFn) -> Self {
        Self { run, resume }
    }

    /// Route table for workflow run/resume.
    #[must_use]
    pub fn route_table(&self) -> RouteTable {
        self.route_table_with_auth(None)
    }

    /// Route table with optional gateway auth hook.
    ///
    /// When `auth` is `Some`, routes use [`AuthMode::Identity`]; the hook itself is
    /// wired on the gateway via [`auth_fn_to_identity`], not stored in handler state.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn route_table_with_auth(&self, auth: Option<AuthFn>) -> RouteTable {
        let table = workflow_routes::route_table(Arc::new(self.clone().into_state()));
        if auth.is_some() {
            table.with_auth_mode(routing::AuthMode::Identity)
        } else {
            table
        }
    }

    /// State handle for route tables.
    #[must_use]
    pub fn into_state(self) -> WorkflowsApiState {
        WorkflowsApiState {
            run: self.run,
            resume: self.resume,
        }
    }

    /// Alias of [`Self::into_state`]. Auth is applied via [`Self::route_table_with_auth`].
    #[must_use]
    pub fn into_state_with_auth(self, _auth: Option<AuthFn>) -> WorkflowsApiState {
        self.into_state()
    }
}

/// Bind and serve workflow routes on `addr` (background task).
///
/// # Errors
/// Returns [`std::io::Error`] when the listen socket cannot be bound.
pub async fn spawn_workflows_server(
    api: WorkflowsApi,
    addr: std::net::SocketAddr,
) -> std::io::Result<()> {
    let routes = api.route_table();
    let gateway = Gateway::new(false).dev_fallback(routes);
    let service = gateway
        .build_service()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e.to_string()))?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("trembita: workflows API listening on http://{addr}");
    tokio::spawn(async move {
        use hyper::server::conn::http1;
        use hyper_util::rt::TokioIo;
        use hyper_util::service::TowerToHyperService;
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let service = service.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let hyper_service = TowerToHyperService::new(service);
                let _ = http1::Builder::new()
                    .serve_connection(io, hyper_service)
                    .with_upgrades()
                    .await;
            });
        }
    });
    Ok(())
}

/// Shared state for introspection routes.
pub struct IntrospectApiState {
    pub(crate) observer: Arc<dyn Observer>,
}

/// HTTP cluster introspection API (read-only Observer snapshots).
#[derive(Clone)]
pub struct IntrospectApi {
    observer: Arc<dyn Observer>,
}

impl IntrospectApi {
    /// Build from an [`Observer`] implementation.
    #[must_use]
    pub fn new(observer: Arc<dyn Observer>) -> Self {
        Self { observer }
    }

    /// Route table for introspection snapshots.
    #[must_use]
    pub fn route_table(&self) -> RouteTable {
        self.route_table_with_auth(None)
    }

    /// Route table with optional gateway auth hook.
    ///
    /// When `auth` is `Some`, routes use [`AuthMode::Identity`]; the hook itself is
    /// wired on the gateway via [`auth_fn_to_identity`], not stored in handler state.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn route_table_with_auth(&self, auth: Option<AuthFn>) -> RouteTable {
        let table = introspect_routes::route_table(Arc::new(self.clone().into_state()));
        if auth.is_some() {
            table.with_auth_mode(routing::AuthMode::Identity)
        } else {
            table
        }
    }

    /// State handle for route tables.
    #[must_use]
    pub fn into_state(self) -> IntrospectApiState {
        IntrospectApiState {
            observer: self.observer,
        }
    }

    /// Alias of [`Self::into_state`]. Auth is applied via [`Self::route_table_with_auth`].
    #[must_use]
    pub fn into_state_with_auth(self, _auth: Option<AuthFn>) -> IntrospectApiState {
        self.into_state()
    }
}
