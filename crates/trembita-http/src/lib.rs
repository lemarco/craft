//! HTTP product helpers for trembita ([background-jobs](../../docs/scenarios/background-jobs.md)).
//!
//! # Jobs API
//!
//! [`JobsApi`] exposes:
//!
//! - `POST /jobs/{stream}` → `202 Accepted` + `{ "job_id": … }`
//! - `POST /jobs/{stream}/batch` → `202 Accepted` + `{ "job_ids": […] }`
//! - `POST /jobs/{stream}/ack-batch` → `200 OK` + `{ "acked": N }`
//! - `GET /jobs/{stream}` → list jobs with optional filters (`state`, `min_attempts`, `dedup`, `limit`, `after`)
//! - `POST /jobs/{stream}/requeue-batch` → `200 OK` + `{ "requeued": […], "failures": […] }`
//! - `POST /jobs/{stream}/{id}/requeue` → `200 OK` + `{ "job_id": … }` (dead-letter retry)
//! - `GET /jobs/{stream}/{id}` → job metadata when the queue supports lookup
//!
//! # Virtual hosts
//!
//! [`HostRouter`] dispatches by HTTP `Host` on a single listen port (strict by default;
//! opt-in [`HostRouter::local_dev_fallback`] for loopback only).
//!
//! Wire it to [`TrembitaApp::jobs_api`](https://docs.rs/trembita/latest/trembita/struct.TrembitaApp.html#method.jobs_api)
//! or custom enqueue / lookup closures.
//!
//! # Actors API
//!
//! [`ActorsApi`] exposes:
//!
//! - `POST /actors/{group}/ask` → `200 OK` + `{ "reply_b64": … }` (or raw bytes with `Accept: application/octet-stream`)
//! - `POST /actors/{group}/cast` → `202 Accepted`
//!
//! Wire it to [`TrembitaApp::actors_api`](https://docs.rs/trembita/latest/trembita/struct.TrembitaApp.html#method.actors_api).
//!
//! # Introspect API
//!
//! [`IntrospectApi`] exposes read-only cluster snapshots (same JSON as the admin port):
//!
//! - `GET /introspect/cluster`, `/actors`, `/queues`, `/sagas`, `/raft-groups`
//! - `GET /introspect/actors/{id}`, `/introspect/node/{id}`
//!
//! Wire it to [`TrembitaApp::introspect_api`](https://docs.rs/trembita/latest/trembita/struct.TrembitaApp.html#method.introspect_api)
//! or any [`Observer`](trembita_dashboard::Observer) implementation.
//!
//! # Workflows API
//!
//! [`WorkflowsApi`] exposes:
//!
//! - `GET /health` → `200 OK`
//! - `POST /workflows/run` → `200 OK` + `{ "saga_id", "outcome" }`
//! - `POST /workflows/resume` → `200 OK` + `{ "saga_id", "outcome" }`
//!
//! Wire custom run/resume hooks for saga coordination (requires in-process journal).

mod actor_routes;
mod actor_types;
mod host_router;
mod introspect_routes;
mod introspect_types;
mod routes;
mod types;
mod upgrade_routes;
mod upgrade_types;
mod workflow_routes;
mod workflow_types;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::Router;
use axum::http::{HeaderMap, Method, Uri};
use trembita_jobs::{
    BatchRequeueResult, EnqueueOptions, JobId, JobListFilter, JobListPage, JobStatus, LeaseId,
    QueueError, WorkerId,
};
use trembita_runtime::{CastError, ClusterAskError};

pub use actor_types::{ActorsApiError, AskAccepted};
pub use host_router::{HostRouter, is_local_dev_host, normalize_host};
pub use introspect_types::IntrospectApiError;
pub use routes::parse_enqueue_body;
pub use trembita_dashboard::{
    ActorView, ClusterView, NodeSummary, NodeView, Observer, QueueStreamView, QueuesView,
    RaftGroupSummary, RaftGroupsView, Readiness, SagaRecordView,
};
pub use types::{
    AckBatchAccepted, AckBatchBody, EnqueueAccepted, EnqueueBatchAccepted, EnqueueBatchBody,
    EnqueueBatchJobBody, EnqueueJsonBody, JobListResponse, JobStatusResponse, JobsApiError,
    LeasedByResponse, RequeueAccepted, RequeueBatchAccepted, RequeueBatchBody,
    RequeueFailureResponse,
};
pub use upgrade_routes::{UpgradeApi, UpgradeApiState, upgrade_router};
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

/// Optional async auth hook for product gateway routes.
pub type AuthFn = Arc<
    dyn Fn(Method, Uri, HeaderMap) -> Pin<Box<dyn Future<Output = Result<(), JobsApiError>> + Send>>
        + Send
        + Sync,
>;

/// Shared Axum state for job routes.
pub struct JobsApiState {
    pub(crate) enqueue: EnqueueFn,
    pub(crate) enqueue_batch: EnqueueBatchFn,
    pub(crate) ack_batch: AckBatchFn,
    pub(crate) job_status: JobStatusFn,
    pub(crate) list_jobs: ListJobsFn,
    pub(crate) requeue_dead_letter: RequeueDeadLetterFn,
    pub(crate) requeue_dead_letter_batch: RequeueDeadLetterBatchFn,
    pub(crate) auth: Option<AuthFn>,
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

    /// Axum routes for job enqueue and lookup. Merge into your app and call [`Self::into_state`].
    pub fn router(&self) -> Router<Arc<JobsApiState>> {
        routes::jobs_router()
    }

    /// State handle for [`Self::router`].
    #[must_use]
    pub fn into_state(self) -> JobsApiState {
        self.into_state_with_auth(None)
    }

    /// State handle with an optional gateway auth hook.
    #[must_use]
    pub fn into_state_with_auth(self, auth: Option<AuthFn>) -> JobsApiState {
        JobsApiState {
            enqueue: self.enqueue,
            enqueue_batch: self.enqueue_batch,
            ack_batch: self.ack_batch,
            job_status: self.job_status,
            list_jobs: self.list_jobs,
            requeue_dead_letter: self.requeue_dead_letter,
            requeue_dead_letter_batch: self.requeue_dead_letter_batch,
            auth,
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

/// Shared Axum state for actor routes.
pub struct ActorsApiState {
    pub(crate) ask: AskFn,
    pub(crate) cast: CastFn,
    pub(crate) auth: Option<AuthFn>,
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

    /// Axum routes for actor cast and ask. Merge into your app and call [`Self::into_state`].
    pub fn router(&self) -> Router<Arc<ActorsApiState>> {
        actor_routes::actors_router()
    }

    /// State handle for [`Self::router`].
    #[must_use]
    pub fn into_state(self) -> ActorsApiState {
        self.into_state_with_auth(None)
    }

    /// State handle with an optional gateway auth hook.
    #[must_use]
    pub fn into_state_with_auth(self, auth: Option<AuthFn>) -> ActorsApiState {
        ActorsApiState {
            ask: self.ask,
            cast: self.cast,
            auth,
        }
    }
}

pub use workflow_types::{SagaBody, WorkflowAccepted, WorkflowsApiError};

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

/// Shared Axum state for workflow routes.
pub struct WorkflowsApiState {
    pub(crate) run: RunWorkflowFn,
    pub(crate) resume: ResumeWorkflowFn,
    pub(crate) auth: Option<AuthFn>,
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

    /// Axum routes for workflow run/resume. Merge into your app and call [`Self::into_state`].
    pub fn router(&self) -> Router<Arc<WorkflowsApiState>> {
        workflow_routes::workflows_router()
    }

    /// State handle for [`Self::router`].
    #[must_use]
    pub fn into_state(self) -> WorkflowsApiState {
        self.into_state_with_auth(None)
    }

    /// State handle with an optional gateway auth hook.
    #[must_use]
    pub fn into_state_with_auth(self, auth: Option<AuthFn>) -> WorkflowsApiState {
        WorkflowsApiState {
            run: self.run,
            resume: self.resume,
            auth,
        }
    }
}

/// Bind and serve workflow routes on `addr` (background task).
///
/// Used by workflows showcases when saga coordination must stay in-process on node 1
/// (`TREMBITA_TRIGGER` listener).
///
/// # Errors
/// Returns [`std::io::Error`] when the listen socket cannot be bound.
pub async fn spawn_workflows_server(
    api: WorkflowsApi,
    addr: std::net::SocketAddr,
) -> std::io::Result<()> {
    let router = api.router().with_state(Arc::new(api.into_state()));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("trembita: workflows API listening on http://{addr}");
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            eprintln!("trembita: workflows API on {addr} failed: {e}");
        }
    });
    Ok(())
}

/// Shared Axum state for introspection routes.
pub struct IntrospectApiState {
    pub(crate) observer: Arc<dyn Observer>,
    pub(crate) auth: Option<AuthFn>,
}

/// HTTP cluster introspection API (read-only Observer snapshots).
#[derive(Clone)]
pub struct IntrospectApi {
    observer: Arc<dyn Observer>,
}

impl IntrospectApi {
    /// Build from an [`Observer`] implementation (typically [`TrembitaApp::introspect_observer`]).
    #[must_use]
    pub fn new(observer: Arc<dyn Observer>) -> Self {
        Self { observer }
    }

    /// Axum routes for introspection snapshots. Merge into your app and call [`Self::into_state`].
    pub fn router(&self) -> Router<Arc<IntrospectApiState>> {
        introspect_routes::introspect_router()
    }

    /// State handle for [`Self::router`].
    #[must_use]
    pub fn into_state(self) -> IntrospectApiState {
        self.into_state_with_auth(None)
    }

    /// State handle with an optional gateway auth hook.
    #[must_use]
    pub fn into_state_with_auth(self, auth: Option<AuthFn>) -> IntrospectApiState {
        IntrospectApiState {
            observer: self.observer,
            auth,
        }
    }
}
