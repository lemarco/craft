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

/// Job lookup response (`200 OK`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobStatusResponse {
    /// Job id within the stream.
    pub job_id: u64,
    /// `pending`, `leased`, or `delayed`.
    pub state: &'static str,
    /// Byte length of stored payload.
    pub payload_len: u64,
    /// Enqueue priority.
    pub priority: u8,
    /// Present when `state` is `leased`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub leased_by: Option<LeasedByResponse>,
}

/// Worker holding a leased job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LeasedByResponse {
    /// Hosting cluster node.
    pub node: u64,
    /// Worker instance on that node.
    pub instance: u32,
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
    /// Job id was not found in the stream.
    #[error("job not found")]
    NotFound,
}

impl IntoResponse for JobsApiError {
    fn into_response(self) -> Response {
        let (status, msg) = match &self {
            Self::BadRequest(m) => (StatusCode::BAD_REQUEST, m.clone()),
            Self::Queue(m) => (StatusCode::SERVICE_UNAVAILABLE, m.clone()),
            Self::NotFound => (StatusCode::NOT_FOUND, self.to_string()),
        };
        (status, msg).into_response()
    }
}
