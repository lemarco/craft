//! JSON wire types for the jobs HTTP API.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

/// Successful enqueue response (`202 Accepted`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EnqueueAccepted {
    /// Assigned durable job id.
    pub job_id: u64,
}

/// JSON envelope for `POST /jobs/{stream}` when `Content-Type: application/json`.
///
/// Provide exactly one of `payload` (UTF-8 string) or `payload_b64` (standard base64).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct EnqueueJsonBody {
    /// UTF-8 string stored as opaque job bytes.
    pub payload: Option<String>,
    /// Base64-encoded opaque job bytes.
    pub payload_b64: Option<String>,
}

/// HTTP-layer enqueue failure mapped to status codes.
#[derive(Debug, thiserror::Error)]
pub enum JobsApiError {
    /// Request body could not be interpreted.
    #[error("{0}")]
    BadRequest(String),
    /// Enqueue failed at the queue backend.
    #[error("{0}")]
    Queue(String),
}

impl IntoResponse for JobsApiError {
    fn into_response(self) -> Response {
        let (status, msg) = match &self {
            Self::BadRequest(m) => (StatusCode::BAD_REQUEST, m.clone()),
            Self::Queue(m) => (StatusCode::SERVICE_UNAVAILABLE, m.clone()),
        };
        (status, msg).into_response()
    }
}
