//! JSON wire types for the workflows HTTP API.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

/// Request body for run / resume workflow endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct SagaBody {
    /// Unique saga identifier (journal key).
    pub saga_id: String,
}

/// Successful workflow response (`200 OK`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkflowAccepted {
    /// Echo of the saga id from the request.
    pub saga_id: String,
    /// Human-readable outcome (`completed`, `compensated`, …).
    pub outcome: String,
}

/// HTTP-layer workflow failure mapped to status codes.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowsApiError {
    /// Request body could not be interpreted.
    #[error("{0}")]
    BadRequest(String),
    /// Saga coordination failed.
    #[error("{0}")]
    Failed(String),
    /// Gateway identity check failed.
    #[error("{0}")]
    Unauthorized(String),
}

impl IntoResponse for WorkflowsApiError {
    fn into_response(self) -> Response {
        let (status, msg) = match &self {
            Self::BadRequest(m) => (StatusCode::BAD_REQUEST, m.clone()),
            Self::Failed(m) => (StatusCode::INTERNAL_SERVER_ERROR, m.clone()),
            Self::Unauthorized(m) => (StatusCode::UNAUTHORIZED, m.clone()),
        };
        (status, msg).into_response()
    }
}
