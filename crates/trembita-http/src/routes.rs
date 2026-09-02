//! Axum routes for job enqueue and lookup.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use bytes::Bytes;
use trembita_jobs::{
    DEFAULT_QUEUE_BATCH_MAX, EnqueueOptions, JobLifecycle, JobListFilter, LeaseId, WorkerId,
};

use crate::JobsApiState;
use crate::types::{
    AckBatchAccepted, AckBatchBody, EnqueueAccepted, EnqueueBatchAccepted, EnqueueBatchBody,
    EnqueueBatchJobBody, EnqueueJsonBody, JobListResponse, JobStatusResponse, JobsApiError,
    LeasedByResponse, RequeueAccepted, RequeueBatchAccepted, RequeueBatchBody,
    RequeueFailureResponse,
};

/// Query parameters for optional enqueue behaviour.
#[derive(Debug, Default, serde::Deserialize)]
pub struct EnqueueQuery {
    /// Job priority (0–255).
    pub priority: Option<u8>,
    /// Client dedup / idempotency key.
    pub dedup: Option<String>,
    /// Maximum delivery attempts before dead letter (`0` = unlimited).
    pub max_attempts: Option<u32>,
}

/// Query parameters for listing jobs in a stream.
#[derive(Debug, Default, serde::Deserialize)]
pub struct ListJobsQuery {
    /// Filter by lifecycle: `pending`, `leased`, `delayed`, or `dead_letter`.
    pub state: Option<String>,
    /// Only jobs with at least this many recorded attempts.
    pub min_attempts: Option<u32>,
    /// Exact client dedup key match.
    pub dedup: Option<String>,
    /// Page size (default 50, max 256).
    pub limit: Option<u32>,
    /// Pagination cursor — return jobs with id strictly greater than this.
    pub after: Option<u64>,
}

/// Axum sub-router for job queue routes.
pub fn jobs_router() -> Router<Arc<JobsApiState>> {
    Router::new()
        .route("/jobs/{stream}", post(post_job).get(list_jobs))
        .route("/jobs/{stream}/batch", post(post_job_batch))
        .route("/jobs/{stream}/ack-batch", post(post_ack_batch))
        .route("/jobs/{stream}/requeue-batch", post(post_requeue_batch))
        .route("/jobs/{stream}/{job_id}/requeue", post(post_requeue))
        .route("/jobs/{stream}/{job_id}", get(get_job))
}

async fn authorize(
    state: &JobsApiState,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
) -> Result<(), JobsApiError> {
    if let Some(auth) = &state.auth {
        auth(method.clone(), uri.clone(), headers.clone()).await?;
    }
    Ok(())
}

async fn post_job(
    State(state): State<Arc<JobsApiState>>,
    Path(stream): Path<String>,
    Query(query): Query<EnqueueQuery>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, JobsApiError> {
    authorize(&state, &method, &uri, &headers).await?;
    let payload = parse_enqueue_body(&headers, &body)?;
    let opts = enqueue_options_from_query(&query);
    let job_id = (state.enqueue)(stream, payload, opts)
        .await
        .map_err(|e| JobsApiError::Queue(e.to_string()))?;
    Ok((
        StatusCode::ACCEPTED,
        axum::Json(EnqueueAccepted { job_id: job_id.0 }),
    ))
}

async fn post_job_batch(
    State(state): State<Arc<JobsApiState>>,
    Path(stream): Path<String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, JobsApiError> {
    authorize(&state, &method, &uri, &headers).await?;
    let batch: EnqueueBatchBody = serde_json::from_slice(&body)
        .map_err(|e| JobsApiError::BadRequest(format!("invalid json body: {e}")))?;
    if batch.jobs.is_empty() {
        return Err(JobsApiError::BadRequest("jobs must not be empty".into()));
    }
    if batch.jobs.len() > DEFAULT_QUEUE_BATCH_MAX {
        return Err(JobsApiError::BadRequest(format!(
            "batch size {} exceeds max {DEFAULT_QUEUE_BATCH_MAX}",
            batch.jobs.len()
        )));
    }
    let jobs = batch
        .jobs
        .into_iter()
        .map(parse_batch_job)
        .collect::<Result<Vec<_>, _>>()?;
    let ids = (state.enqueue_batch)(stream, jobs)
        .await
        .map_err(|e| JobsApiError::Queue(e.to_string()))?;
    Ok((
        StatusCode::ACCEPTED,
        axum::Json(EnqueueBatchAccepted {
            job_ids: ids.into_iter().map(|id| id.0).collect(),
        }),
    ))
}

async fn post_ack_batch(
    State(state): State<Arc<JobsApiState>>,
    Path(stream): Path<String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, JobsApiError> {
    authorize(&state, &method, &uri, &headers).await?;
    let req: AckBatchBody = serde_json::from_slice(&body)
        .map_err(|e| JobsApiError::BadRequest(format!("invalid json body: {e}")))?;
    if req.lease_ids.is_empty() {
        return Err(JobsApiError::BadRequest(
            "lease_ids must not be empty".into(),
        ));
    }
    if req.lease_ids.len() > DEFAULT_QUEUE_BATCH_MAX {
        return Err(JobsApiError::BadRequest(format!(
            "batch size {} exceeds max {DEFAULT_QUEUE_BATCH_MAX}",
            req.lease_ids.len()
        )));
    }
    let worker = WorkerId {
        node: trembita_proto::NodeId(req.worker_node),
        instance: req.worker_instance,
    };
    let acked = req.lease_ids.len();
    let lease_ids: Vec<LeaseId> = req.lease_ids.into_iter().map(LeaseId).collect();
    (state.ack_batch)(stream, worker, lease_ids)
        .await
        .map_err(|e| JobsApiError::Queue(e.to_string()))?;
    Ok((StatusCode::OK, axum::Json(AckBatchAccepted { acked })))
}

async fn get_job(
    State(state): State<Arc<JobsApiState>>,
    Path((stream, job_id)): Path<(String, u64)>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Result<impl IntoResponse, JobsApiError> {
    authorize(&state, &method, &uri, &headers).await?;
    let status = (state.job_status)(stream, job_id)
        .await
        .map_err(|e| JobsApiError::Queue(e.to_string()))?;
    let Some(status) = status else {
        return Err(JobsApiError::NotFound);
    };
    Ok(axum::Json(status_to_response(&status)))
}

async fn list_jobs(
    State(state): State<Arc<JobsApiState>>,
    Path(stream): Path<String>,
    Query(query): Query<ListJobsQuery>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Result<impl IntoResponse, JobsApiError> {
    authorize(&state, &method, &uri, &headers).await?;
    let filter = list_filter_from_query(&query)?;
    let page = (state.list_jobs)(stream, filter)
        .await
        .map_err(|e| JobsApiError::Queue(e.to_string()))?;
    Ok(axum::Json(JobListResponse {
        jobs: page
            .jobs
            .into_iter()
            .map(|j| status_to_response(&j))
            .collect(),
        has_more: page.has_more,
    }))
}

async fn post_requeue(
    State(state): State<Arc<JobsApiState>>,
    Path((stream, job_id)): Path<(String, u64)>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Result<impl IntoResponse, JobsApiError> {
    authorize(&state, &method, &uri, &headers).await?;
    (state.requeue_dead_letter)(stream, job_id)
        .await
        .map_err(|e| JobsApiError::Queue(e.to_string()))?;
    Ok((StatusCode::OK, axum::Json(RequeueAccepted { job_id })))
}

async fn post_requeue_batch(
    State(state): State<Arc<JobsApiState>>,
    Path(stream): Path<String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, JobsApiError> {
    authorize(&state, &method, &uri, &headers).await?;
    let req: RequeueBatchBody = serde_json::from_slice(&body)
        .map_err(|e| JobsApiError::BadRequest(format!("invalid json body: {e}")))?;
    if req.job_ids.is_empty() {
        return Err(JobsApiError::BadRequest("job_ids must not be empty".into()));
    }
    if req.job_ids.len() > DEFAULT_QUEUE_BATCH_MAX {
        return Err(JobsApiError::BadRequest(format!(
            "batch size {} exceeds max {DEFAULT_QUEUE_BATCH_MAX}",
            req.job_ids.len()
        )));
    }
    let result = (state.requeue_dead_letter_batch)(stream, req.job_ids)
        .await
        .map_err(|e| JobsApiError::Queue(e.to_string()))?;
    Ok((
        StatusCode::OK,
        axum::Json(RequeueBatchAccepted {
            requeued: result.requeued.into_iter().map(|id| id.0).collect(),
            failures: result
                .failures
                .into_iter()
                .map(|(id, err)| RequeueFailureResponse {
                    job_id: id.0,
                    error: err.to_string(),
                })
                .collect(),
        }),
    ))
}

const fn lifecycle_name(lifecycle: JobLifecycle) -> &'static str {
    match lifecycle {
        JobLifecycle::Pending => "pending",
        JobLifecycle::Leased => "leased",
        JobLifecycle::Delayed => "delayed",
        JobLifecycle::DeadLetter => "dead_letter",
    }
}

fn enqueue_options_from_query(query: &EnqueueQuery) -> EnqueueOptions {
    let mut opts = EnqueueOptions::default();
    if let Some(p) = query.priority {
        opts.priority = p;
    }
    if let Some(key) = &query.dedup {
        opts.dedup_key = Some(key.as_bytes().to_vec());
    }
    if let Some(max) = query.max_attempts {
        opts.max_attempts = Some(max);
    }
    opts
}

fn status_to_response(status: &trembita_jobs::JobStatus) -> JobStatusResponse {
    JobStatusResponse {
        job_id: status.job_id.0,
        state: lifecycle_name(status.lifecycle),
        payload_len: status.payload_len,
        priority: status.priority,
        attempts: status.attempts,
        max_attempts: status.max_attempts,
        is_redelivery: status.attempts > 1,
        dedup: status
            .dedup_key
            .as_ref()
            .map(|k| String::from_utf8_lossy(k).into_owned()),
        leased_by: status.leased_by.map(|w| LeasedByResponse {
            node: w.node.0,
            instance: w.instance,
        }),
    }
}

fn list_filter_from_query(query: &ListJobsQuery) -> Result<JobListFilter, JobsApiError> {
    let lifecycle = match query.state.as_deref() {
        None => None,
        Some("pending") => Some(JobLifecycle::Pending),
        Some("leased") => Some(JobLifecycle::Leased),
        Some("delayed") => Some(JobLifecycle::Delayed),
        Some("dead_letter") => Some(JobLifecycle::DeadLetter),
        Some(other) => {
            return Err(JobsApiError::BadRequest(format!(
                "unknown state {other:?}; use pending, leased, delayed, or dead_letter"
            )));
        }
    };
    Ok(JobListFilter {
        lifecycle,
        min_attempts: query.min_attempts,
        dedup_key: query
            .dedup
            .as_ref()
            .map(String::as_bytes)
            .map(<[u8]>::to_vec),
        limit: query.limit.map(|n| n as usize),
        after_job_id: query.after.map(trembita_jobs::JobId),
    })
}

fn parse_batch_job(job: EnqueueBatchJobBody) -> Result<(Vec<u8>, EnqueueOptions), JobsApiError> {
    if job.payload.is_some() && job.payload_b64.is_some() {
        return Err(JobsApiError::BadRequest(
            "provide only one of payload or payload_b64 per job".into(),
        ));
    }
    let payload = if let Some(text) = job.payload {
        text.into_bytes()
    } else if let Some(b64) = job.payload_b64 {
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64)
            .map_err(|e| JobsApiError::BadRequest(format!("invalid payload_b64: {e}")))?
    } else {
        return Err(JobsApiError::BadRequest(
            "each job requires payload or payload_b64".into(),
        ));
    };
    let opts = EnqueueOptions {
        priority: job.priority,
        dedup_key: job.dedup.map(String::into_bytes),
        max_attempts: job.max_attempts,
        ..Default::default()
    };
    Ok((payload, opts))
}

/// Parse request body as raw bytes or JSON envelope.
///
/// # Errors
/// Returns [`JobsApiError::BadRequest`] when JSON is invalid or both payload fields are set.
pub fn parse_enqueue_body(headers: &HeaderMap, body: &Bytes) -> Result<Vec<u8>, JobsApiError> {
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if ct.starts_with("application/json") {
        let env: EnqueueJsonBody = serde_json::from_slice(body)
            .map_err(|e| JobsApiError::BadRequest(format!("invalid json body: {e}")))?;
        if env.payload.is_some() && env.payload_b64.is_some() {
            return Err(JobsApiError::BadRequest(
                "provide only one of payload or payload_b64".into(),
            ));
        }
        if let Some(text) = env.payload {
            return Ok(text.into_bytes());
        }
        if let Some(b64) = env.payload_b64 {
            return base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64)
                .map_err(|e| JobsApiError::BadRequest(format!("invalid payload_b64: {e}")));
        }
        return Err(JobsApiError::BadRequest(
            "json body requires payload or payload_b64".into(),
        ));
    }
    Ok(body.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use std::future;
    use tower::ServiceExt;
    use trembita_jobs::{JobId, JobLifecycle, JobStatus};

    fn test_state(
        enqueue: crate::EnqueueFn,
        enqueue_batch: crate::EnqueueBatchFn,
        ack_batch: crate::AckBatchFn,
        job_status: crate::JobStatusFn,
        list_jobs: crate::ListJobsFn,
        requeue_dead_letter: crate::RequeueDeadLetterFn,
        requeue_dead_letter_batch: crate::RequeueDeadLetterBatchFn,
    ) -> Arc<JobsApiState> {
        Arc::new(JobsApiState {
            enqueue,
            enqueue_batch,
            ack_batch,
            job_status,
            list_jobs,
            requeue_dead_letter,
            requeue_dead_letter_batch,
            auth: None,
        })
    }

    fn noop_list() -> crate::ListJobsFn {
        Arc::new(|_, _| Box::pin(future::ready(Ok(trembita_jobs::JobListPage::default()))))
    }

    fn noop_requeue_batch() -> crate::RequeueDeadLetterBatchFn {
        Arc::new(|_, _| {
            Box::pin(future::ready(Ok(
                trembita_jobs::BatchRequeueResult::default(),
            )))
        })
    }

    fn noop_requeue() -> crate::RequeueDeadLetterFn {
        Arc::new(|_, _| Box::pin(future::ready(Ok(()))))
    }

    fn noop_batch() -> crate::EnqueueBatchFn {
        Arc::new(|_, _| Box::pin(future::ready(Ok(Vec::new()))))
    }

    fn noop_ack() -> crate::AckBatchFn {
        Arc::new(|_, _, _| Box::pin(future::ready(Ok(()))))
    }

    #[tokio::test]
    async fn post_job_returns_202_with_id() {
        let state = test_state(
            Arc::new(|stream, payload, _opts| {
                assert_eq!(stream, "emails");
                assert_eq!(payload, b"hello");
                Box::pin(future::ready(Ok(JobId(42))))
            }),
            noop_batch(),
            noop_ack(),
            Arc::new(|_, _| Box::pin(future::ready(Ok(None)))),
            noop_list(),
            noop_requeue(),
            noop_requeue_batch(),
        );
        let app = jobs_router().with_state(state);
        let req = Request::builder()
            .method("POST")
            .uri("/jobs/emails")
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::from("hello"))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn post_job_batch_returns_202_with_ids() {
        let state = test_state(
            Arc::new(|_, _, _| Box::pin(future::ready(Ok(JobId(1))))),
            Arc::new(|stream, jobs| {
                assert_eq!(stream, "emails");
                assert_eq!(jobs.len(), 2);
                Box::pin(future::ready(Ok(vec![JobId(10), JobId(11)])))
            }),
            noop_ack(),
            Arc::new(|_, _| Box::pin(future::ready(Ok(None)))),
            noop_list(),
            noop_requeue(),
            noop_requeue_batch(),
        );
        let app = jobs_router().with_state(state);
        let req = Request::builder()
            .method("POST")
            .uri("/jobs/emails/batch")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"jobs":[{"payload":"a"},{"payload":"b"}]}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn post_ack_batch_returns_ok() {
        let state = test_state(
            Arc::new(|_, _, _| Box::pin(future::ready(Ok(JobId(1))))),
            noop_batch(),
            Arc::new(|stream, worker, lease_ids| {
                assert_eq!(stream, "emails");
                assert_eq!(worker.node.0, 1);
                assert_eq!(worker.instance, 0);
                assert_eq!(lease_ids.len(), 2);
                Box::pin(future::ready(Ok(())))
            }),
            Arc::new(|_, _| Box::pin(future::ready(Ok(None)))),
            noop_list(),
            noop_requeue(),
            noop_requeue_batch(),
        );
        let app = jobs_router().with_state(state);
        let req = Request::builder()
            .method("POST")
            .uri("/jobs/emails/ack-batch")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"worker_node":1,"worker_instance":0,"lease_ids":[100,101]}"#,
            ))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_job_returns_metadata() {
        let state = test_state(
            Arc::new(|_, _, _| Box::pin(future::ready(Ok(JobId(1))))),
            noop_batch(),
            noop_ack(),
            Arc::new(|stream, job_id| {
                assert_eq!(stream, "emails");
                assert_eq!(job_id, 7);
                Box::pin(future::ready(Ok(Some(JobStatus {
                    job_id: JobId(7),
                    lifecycle: JobLifecycle::Pending,
                    payload_len: 5,
                    priority: 2,
                    leased_by: None,
                    attempts: 0,
                    max_attempts: 0,
                    dedup_key: None,
                }))))
            }),
            noop_list(),
            noop_requeue(),
            noop_requeue_batch(),
        );
        let app = jobs_router().with_state(state);
        let req = Request::builder()
            .method("GET")
            .uri("/jobs/emails/7")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_job_missing_returns_404() {
        let state = test_state(
            Arc::new(|_, _, _| Box::pin(future::ready(Ok(JobId(1))))),
            noop_batch(),
            noop_ack(),
            Arc::new(|_, _| Box::pin(future::ready(Ok(None)))),
            noop_list(),
            noop_requeue(),
            noop_requeue_batch(),
        );
        let app = jobs_router().with_state(state);
        let req = Request::builder()
            .method("GET")
            .uri("/jobs/emails/99")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn post_requeue_returns_ok() {
        let state = test_state(
            Arc::new(|_, _, _| Box::pin(future::ready(Ok(JobId(1))))),
            noop_batch(),
            noop_ack(),
            Arc::new(|_, _| Box::pin(future::ready(Ok(None)))),
            noop_list(),
            Arc::new(|stream, job_id| {
                assert_eq!(stream, "emails");
                assert_eq!(job_id, 9);
                Box::pin(future::ready(Ok(())))
            }),
            noop_requeue_batch(),
        );
        let app = jobs_router().with_state(state);
        let req = Request::builder()
            .method("POST")
            .uri("/jobs/emails/9/requeue")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn json_payload_string() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        let body = Bytes::from(r#"{"payload":"hi"}"#);
        assert_eq!(parse_enqueue_body(&headers, &body).unwrap(), b"hi".to_vec());
    }
}
