//! HTTP product helpers for crafty ([background-jobs](../../docs/scenarios/background-jobs.md)).
//!
//! # Jobs API
//!
//! [`JobsApi`] exposes:
//!
//! - `POST /jobs/{stream}` → `202 Accepted` + `{ "job_id": … }`
//! - `GET /jobs/{stream}/{id}` → job metadata when the queue supports lookup
//!
//! Wire it to [`CraftyApp::jobs_api`](https://docs.rs/crafty/latest/crafty/struct.CraftyApp.html#method.jobs_api)
//! or custom enqueue / lookup closures.

mod routes;
mod types;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::Router;
use crafty_actor::{EnqueueOptions, JobId, JobStatus, QueueError};

pub use routes::parse_enqueue_body;
pub use types::{
    EnqueueAccepted, EnqueueJsonBody, JobStatusResponse, JobsApiError, LeasedByResponse,
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

/// Async job lookup hook used by [`JobsApi`].
pub type JobStatusFn = Arc<
    dyn Fn(
            String,
            u64,
        ) -> Pin<Box<dyn Future<Output = Result<Option<JobStatus>, QueueError>> + Send>>
        + Send
        + Sync,
>;

/// Shared Axum state for job routes.
pub struct JobsApiState {
    pub(crate) enqueue: EnqueueFn,
    pub(crate) job_status: JobStatusFn,
}

/// HTTP job enqueue + lookup API.
#[derive(Clone)]
pub struct JobsApi {
    enqueue: EnqueueFn,
    job_status: JobStatusFn,
}

impl JobsApi {
    /// Build from custom enqueue and job-status closures.
    #[must_use]
    pub fn new(enqueue: EnqueueFn, job_status: JobStatusFn) -> Self {
        Self {
            enqueue,
            job_status,
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
            job_status: self.job_status,
        }
    }
}
