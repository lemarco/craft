//! HTTP product helpers for crafty ([background-jobs](../../docs/scenarios/background-jobs.md)).
//!
//! # Job enqueue
//!
//! [`JobsApi`] exposes `POST /jobs/{stream}` → `202 Accepted` + `{ "job_id": … }`.
//! Wire it to [`CraftyApp::jobs_api`](https://docs.rs/crafty/latest/crafty/struct.CraftyApp.html#method.jobs_api)
//! or any custom enqueue closure.

mod routes;
mod types;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::Router;
use crafty_actor::{EnqueueOptions, JobId, QueueError};

pub use routes::parse_enqueue_body;
pub use types::{EnqueueAccepted, EnqueueJsonBody, JobsApiError};

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

/// Shared Axum state for job routes.
pub struct JobsApiState {
    pub(crate) enqueue: EnqueueFn,
}

/// HTTP job enqueue API (`POST /jobs/{stream}`).
#[derive(Clone)]
pub struct JobsApi {
    enqueue: EnqueueFn,
}

impl JobsApi {
    /// Build from a custom enqueue closure (typically wrapping [`CraftyApp::enqueue`](https://docs.rs/crafty)).
    #[must_use]
    pub fn new(enqueue: EnqueueFn) -> Self {
        Self { enqueue }
    }

    /// Axum routes for job enqueue. Merge into your app and call [`Self::into_state`].
    pub fn router(&self) -> Router<Arc<JobsApiState>> {
        routes::jobs_router()
    }

    /// State handle for [`Self::router`].
    #[must_use]
    pub fn into_state(self) -> JobsApiState {
        JobsApiState {
            enqueue: self.enqueue,
        }
    }
}
