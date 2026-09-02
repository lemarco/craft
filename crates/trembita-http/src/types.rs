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

/// Successful batch enqueue response (`202 Accepted`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EnqueueBatchAccepted {
    /// Assigned job ids in request order.
    pub job_ids: Vec<u64>,
}

/// One job in a batch enqueue body.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct EnqueueBatchJobBody {
    /// UTF-8 string stored as opaque job bytes.
    pub payload: Option<String>,
    /// Base64-encoded opaque job bytes.
    pub payload_b64: Option<String>,
    /// Job priority (0–255).
    #[serde(default)]
    pub priority: u8,
    /// Client dedup / idempotency key.
    #[serde(default)]
    pub dedup: Option<String>,
    /// Maximum delivery attempts before dead letter (`0` = unlimited).
    #[serde(default)]
    pub max_attempts: Option<u32>,
}

/// JSON body for `POST /jobs/{stream}/batch`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct EnqueueBatchBody {
    /// Jobs to enqueue (capped at [`trembita_jobs::DEFAULT_QUEUE_BATCH_MAX`]).
    pub jobs: Vec<EnqueueBatchJobBody>,
}

/// JSON body for `POST /jobs/{stream}/ack-batch`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AckBatchBody {
    /// Cluster node id of the leasing worker.
    pub worker_node: u64,
    /// Worker instance id on that node.
    pub worker_instance: u32,
    /// Lease tokens from a prior lease.
    pub lease_ids: Vec<u64>,
}

/// Successful batch ack response (`200 OK`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AckBatchAccepted {
    /// Number of leases acknowledged.
    pub acked: usize,
}

/// Successful dead-letter requeue response (`200 OK`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RequeueAccepted {
    /// Job id moved back to pending.
    pub job_id: u64,
}

/// JSON body for `POST /jobs/{stream}/requeue-batch`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RequeueBatchBody {
    /// Dead-letter job ids to move back to pending.
    pub job_ids: Vec<u64>,
}

/// Per-job failure in a batch requeue response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RequeueFailureResponse {
    /// Job id that could not be requeued.
    pub job_id: u64,
    /// Why requeue failed for this id.
    pub error: String,
}

/// Successful batch requeue response (`200 OK`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RequeueBatchAccepted {
    /// Job ids successfully moved back to pending.
    pub requeued: Vec<u64>,
    /// Per-id failures (not dead letter, unknown id, …).
    pub failures: Vec<RequeueFailureResponse>,
}

/// Job list response (`200 OK`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobListResponse {
    /// Matching jobs in ascending job-id order.
    pub jobs: Vec<JobStatusResponse>,
    /// `true` when more rows exist beyond this page.
    pub has_more: bool,
}

/// Job lookup response (`200 OK`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobStatusResponse {
    /// Job id within the stream.
    pub job_id: u64,
    /// `pending`, `leased`, `delayed`, or `dead_letter`.
    pub state: &'static str,
    /// Byte length of stored payload.
    pub payload_len: u64,
    /// Enqueue priority.
    pub priority: u8,
    /// Delivery attempts recorded so far.
    pub attempts: u32,
    /// Configured retry ceiling (`0` = unlimited).
    pub max_attempts: u32,
    /// `true` when `attempts > 1` — handler must be idempotent.
    pub is_redelivery: bool,
    /// Client dedup key from enqueue, when set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dedup: Option<String>,
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
    /// Gateway identity check failed.
    #[error("{0}")]
    Unauthorized(String),
}

impl IntoResponse for JobsApiError {
    fn into_response(self) -> Response {
        let (status, msg) = match &self {
            Self::BadRequest(m) => (StatusCode::BAD_REQUEST, m.clone()),
            Self::Queue(m) => (StatusCode::SERVICE_UNAVAILABLE, m.clone()),
            Self::NotFound => (StatusCode::NOT_FOUND, self.to_string()),
            Self::Unauthorized(m) => (StatusCode::UNAUTHORIZED, m.clone()),
        };
        (status, msg).into_response()
    }
}
