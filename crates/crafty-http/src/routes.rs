//! Axum routes for job enqueue and lookup.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use bytes::Bytes;
use crafty_actor::{DEFAULT_QUEUE_BATCH_MAX, EnqueueOptions, JobLifecycle, LeaseId, WorkerId};

use crate::JobsApiState;
use crate::types::{
    AckBatchAccepted, AckBatchBody, EnqueueAccepted, EnqueueBatchAccepted, EnqueueBatchBody,
    EnqueueBatchJobBody, EnqueueJsonBody, JobStatusResponse, JobsApiError, LeasedByResponse,
};

/// Query parameters for optional enqueue behaviour.
#[derive(Debug, Default, serde::Deserialize)]
pub struct EnqueueQuery {
    /// Job priority (0–255).
    pub priority: Option<u8>,
    /// Client dedup / idempotency key.
    pub dedup: Option<String>,
}

/// Axum sub-router for tier C job routes.
pub fn jobs_router() -> Router<Arc<JobsApiState>> {
    Router::new()
        .route("/jobs/{stream}", post(post_job))
        .route("/jobs/{stream}/batch", post(post_job_batch))
        .route("/jobs/{stream}/ack-batch", post(post_ack_batch))
        .route("/jobs/{stream}/{job_id}", get(get_job))
}

async fn post_job(
    State(state): State<Arc<JobsApiState>>,
    Path(stream): Path<String>,
    Query(query): Query<EnqueueQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, JobsApiError> {
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
    body: Bytes,
) -> Result<impl IntoResponse, JobsApiError> {
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
    body: Bytes,
) -> Result<impl IntoResponse, JobsApiError> {
    let req: AckBatchBody = serde_json::from_slice(&body)
        .map_err(|e| JobsApiError::BadRequest(format!("invalid json body: {e}")))?;
    if req.lease_ids.is_empty() {
        return Err(JobsApiError::BadRequest("lease_ids must not be empty".into()));
    }
    if req.lease_ids.len() > DEFAULT_QUEUE_BATCH_MAX {
        return Err(JobsApiError::BadRequest(format!(
            "batch size {} exceeds max {DEFAULT_QUEUE_BATCH_MAX}",
            req.lease_ids.len()
        )));
    }
    let worker = WorkerId {
        node: crafty_proto::NodeId(req.worker_node),
        instance: req.worker_instance,
    };
    let acked = req.lease_ids.len();
    let lease_ids: Vec<LeaseId> = req.lease_ids.into_iter().map(LeaseId).collect();
    (state.ack_batch)(stream, worker, lease_ids)
        .await
        .map_err(|e| JobsApiError::Queue(e.to_string()))?;
    Ok((
        StatusCode::OK,
        axum::Json(AckBatchAccepted { acked }),
    ))
}

async fn get_job(
    State(state): State<Arc<JobsApiState>>,
    Path((stream, job_id)): Path<(String, u64)>,
) -> Result<impl IntoResponse, JobsApiError> {
    let status = (state.job_status)(stream, job_id)
        .await
        .map_err(|e| JobsApiError::Queue(e.to_string()))?;
    let Some(status) = status else {
        return Err(JobsApiError::NotFound);
    };
    Ok(axum::Json(JobStatusResponse {
        job_id: status.job_id.0,
        state: lifecycle_name(status.lifecycle),
        payload_len: status.payload_len,
        priority: status.priority,
        leased_by: status.leased_by.map(|w| LeasedByResponse {
            node: w.node.0,
            instance: w.instance,
        }),
    }))
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
    opts
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
    let mut opts = EnqueueOptions::default();
    opts.priority = job.priority;
    if let Some(key) = job.dedup {
        opts.dedup_key = Some(key.into_bytes());
    }
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
    use crafty_actor::{JobId, JobLifecycle, JobStatus};
    use std::future;
    use tower::ServiceExt;

    fn test_state(
        enqueue: crate::EnqueueFn,
        enqueue_batch: crate::EnqueueBatchFn,
        ack_batch: crate::AckBatchFn,
        job_status: crate::JobStatusFn,
    ) -> Arc<JobsApiState> {
        Arc::new(JobsApiState {
            enqueue,
            enqueue_batch,
            ack_batch,
            job_status,
        })
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
        );
        let app = jobs_router().with_state(state);
        let req = Request::builder()
            .method("POST")
            .uri("/jobs/emails/batch")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"jobs":[{"payload":"a"},{"payload":"b"}]}"#,
            ))
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
                }))))
            }),
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

    #[test]
    fn json_payload_string() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        let body = Bytes::from(r#"{"payload":"hi"}"#);
        assert_eq!(parse_enqueue_body(&headers, &body).unwrap(), b"hi".to_vec());
    }
}
