//! HTTP product helpers for crafty ([background-jobs](../../docs/scenarios/background-jobs.md)).
//!
//! # Jobs API
//!
//! [`JobsApi`] exposes:
//!
//! - `POST /jobs/{stream}` → `202 Accepted` + `{ "job_id": … }`
//! - `POST /jobs/{stream}/batch` → `202 Accepted` + `{ "job_ids": […] }`
//! - `POST /jobs/{stream}/ack-batch` → `200 OK` + `{ "acked": N }`
//! - `POST /jobs/{stream}/{id}/requeue` → `200 OK` + `{ "job_id": … }` (dead-letter retry)
//! - `GET /jobs/{stream}/{id}` → job metadata when the queue supports lookup
//!
//! Wire it to [`CraftyApp::jobs_api`](https://docs.rs/crafty/latest/crafty/struct.CraftyApp.html#method.jobs_api)
//! or custom enqueue / lookup closures.
//!
//! # Actors API
//!
//! [`ActorsApi`] exposes:
//!
//! - `POST /actors/{group}/ask` → `200 OK` + `{ "reply_b64": … }` (or raw bytes with `Accept: application/octet-stream`)
//! - `POST /actors/{group}/cast` → `202 Accepted`
//!
//! Wire it to [`CraftyApp::actors_api`](https://docs.rs/crafty/latest/crafty/struct.CraftyApp.html#method.actors_api).
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
mod routes;
mod types;
mod workflow_routes;
mod workflow_types;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::Router;
use crafty_actor::{
    CastError, ClusterAskError, EnqueueOptions, JobId, JobStatus, LeaseId, QueueError, WorkerId,
};

pub use actor_types::{ActorsApiError, AskAccepted};
pub use routes::parse_enqueue_body;
pub use types::{
    AckBatchAccepted, AckBatchBody, EnqueueAccepted, EnqueueBatchAccepted, EnqueueBatchBody,
    EnqueueBatchJobBody, EnqueueJsonBody, JobStatusResponse, JobsApiError, LeasedByResponse,
    RequeueAccepted,
};

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

/// Async dead-letter requeue hook used by [`JobsApi`].
pub type RequeueDeadLetterFn = Arc<
    dyn Fn(String, u64) -> Pin<Box<dyn Future<Output = Result<(), QueueError>> + Send>>
        + Send
        + Sync,
>;

/// Shared Axum state for job routes.
pub struct JobsApiState {
    pub(crate) enqueue: EnqueueFn,
    pub(crate) enqueue_batch: EnqueueBatchFn,
    pub(crate) ack_batch: AckBatchFn,
    pub(crate) job_status: JobStatusFn,
    pub(crate) requeue_dead_letter: RequeueDeadLetterFn,
}

/// HTTP job enqueue + lookup API.
#[derive(Clone)]
pub struct JobsApi {
    enqueue: EnqueueFn,
    enqueue_batch: EnqueueBatchFn,
    ack_batch: AckBatchFn,
    job_status: JobStatusFn,
    requeue_dead_letter: RequeueDeadLetterFn,
}

impl JobsApi {
    /// Build from custom enqueue, batch, and job-status closures.
    #[must_use]
    pub fn new(
        enqueue: EnqueueFn,
        enqueue_batch: EnqueueBatchFn,
        ack_batch: AckBatchFn,
        job_status: JobStatusFn,
        requeue_dead_letter: RequeueDeadLetterFn,
    ) -> Self {
        Self {
            enqueue,
            enqueue_batch,
            ack_batch,
            job_status,
            requeue_dead_letter,
        }
    }

    /// Axum routes for job enqueue and lookup. Merge into your app and call [`Self::into_state`].
    pub fn router(&self) -> Router<Arc<JobsApiState>> {
        routes::jobs_router()
    }

    /// State handle for [`Self::router`].
    #[must_use]
    pub fn into_state(self) -> JobsApiState {
        JobsApiState {
            enqueue: self.enqueue,
            enqueue_batch: self.enqueue_batch,
            ack_batch: self.ack_batch,
            job_status: self.job_status,
            requeue_dead_letter: self.requeue_dead_letter,
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
        ActorsApiState {
            ask: self.ask,
            cast: self.cast,
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
        WorkflowsApiState {
            run: self.run,
            resume: self.resume,
        }
    }
}

/// Bind and serve workflow routes on `addr` (background task).
///
/// Used by tier A showcases when saga coordination must stay in-process on node 1
/// (`CRAFTY_TRIGGER` listener).
///
/// # Errors
/// Returns [`std::io::Error`] when the listen socket cannot be bound.
pub async fn spawn_workflows_server(
    api: WorkflowsApi,
    addr: std::net::SocketAddr,
) -> std::io::Result<()> {
    let router = api.router().with_state(Arc::new(api.into_state()));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!("crafty: workflows API listening on http://{addr}");
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            eprintln!("crafty: workflows API on {addr} failed: {e}");
        }
    });
    Ok(())
}
