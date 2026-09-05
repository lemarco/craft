//! Route table for job enqueue and lookup.

use std::collections::HashMap;
use std::sync::Arc;

use http::header;
use http::{Method, StatusCode, Uri};
use trembita_jobs::{
    DEFAULT_QUEUE_BATCH_MAX, EnqueueOptions, JobLifecycle, JobListFilter, LeaseId, WorkerId,
};

use crate::JobsApiState;
use crate::routing::{HttpError, RequestCtx, Response, RouteTable};
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

/// Route table for job queue routes.
#[must_use]
pub fn route_table(state: Arc<JobsApiState>) -> RouteTable {
    let s1 = Arc::clone(&state);
    let s2 = Arc::clone(&state);
    let s3 = Arc::clone(&state);
    let s4 = Arc::clone(&state);
    let s5 = Arc::clone(&state);
    let s6 = Arc::clone(&state);
    let s7 = Arc::clone(&state);
    RouteTable::new()
        .route(
            Method::POST,
            "/jobs/{stream}",
            crate::routing::AuthMode::Open,
            move |ctx| {
                let state = Arc::clone(&s1);
                async move { post_job(state, ctx).await }
            },
        )
        .route(
            Method::GET,
            "/jobs/{stream}",
            crate::routing::AuthMode::Open,
            move |ctx| {
                let state = Arc::clone(&s2);
                async move { list_jobs(state, ctx).await }
            },
        )
        .post("/jobs/{stream}/batch", move |ctx| {
            let state = Arc::clone(&s3);
            async move { post_job_batch(state, ctx).await }
        })
        .post("/jobs/{stream}/ack-batch", move |ctx| {
            let state = Arc::clone(&s4);
            async move { post_ack_batch(state, ctx).await }
        })
        .post("/jobs/{stream}/requeue-batch", move |ctx| {
            let state = Arc::clone(&s5);
            async move { post_requeue_batch(state, ctx).await }
        })
        .post("/jobs/{stream}/{job_id}/requeue", move |ctx| {
            let state = Arc::clone(&s6);
            async move { post_requeue(state, ctx).await }
        })
        .get("/jobs/{stream}/{job_id}", move |ctx| {
            let state = Arc::clone(&s7);
            async move { get_job(state, ctx).await }
        })
}

fn ctx_uri(ctx: &RequestCtx) -> Uri {
    let mut path = ctx.path().to_string();
    if !ctx.query().is_empty() {
        let qs: Vec<String> = ctx
            .query()
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        path.push('?');
        path.push_str(&qs.join("&"));
    }
    path.parse().unwrap_or_else(|_| Uri::from_static("/"))
}

fn parse_query<T: serde::de::DeserializeOwned>(ctx: &RequestCtx) -> Result<T, JobsApiError> {
    let map: HashMap<String, String> = ctx.query().clone();
    serde_json::from_value(serde_json::json!(map))
        .map_err(|e| JobsApiError::BadRequest(format!("invalid query: {e}")))
}

async fn authorize(state: &JobsApiState, ctx: &RequestCtx) -> Result<(), JobsApiError> {
    if let Some(auth) = &state.auth {
        auth(ctx.method().clone(), ctx_uri(ctx), ctx.headers().clone()).await?;
    }
    Ok(())
}

async fn post_job(state: Arc<JobsApiState>, ctx: RequestCtx) -> Result<Response, HttpError> {
    match post_job_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn post_job_inner(state: &JobsApiState, ctx: RequestCtx) -> Result<Response, JobsApiError> {
    authorize(state, &ctx).await?;
    let stream = ctx
        .params()
        .get("stream")
        .ok_or_else(|| JobsApiError::BadRequest("missing stream".into()))?
        .to_string();
    let query: EnqueueQuery = parse_query(&ctx)?;
    let payload = parse_enqueue_body(ctx.headers(), ctx.body())?;
    let opts = enqueue_options_from_query(&query);
    let job_id = (state.enqueue)(stream, payload, opts)
        .await
        .map_err(|e| JobsApiError::Queue(e.to_string()))?;
    json_response(StatusCode::ACCEPTED, EnqueueAccepted { job_id: job_id.0 })
}

async fn post_job_batch(state: Arc<JobsApiState>, ctx: RequestCtx) -> Result<Response, HttpError> {
    match post_job_batch_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn post_job_batch_inner(
    state: &JobsApiState,
    ctx: RequestCtx,
) -> Result<Response, JobsApiError> {
    authorize(state, &ctx).await?;
    let stream = ctx
        .params()
        .get("stream")
        .ok_or_else(|| JobsApiError::BadRequest("missing stream".into()))?
        .to_string();
    let batch: EnqueueBatchBody = ctx
        .json()
        .map_err(|e| JobsApiError::BadRequest(e.message().to_string()))?;
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
    json_response(
        StatusCode::ACCEPTED,
        EnqueueBatchAccepted {
            job_ids: ids.into_iter().map(|id| id.0).collect(),
        },
    )
}

async fn post_ack_batch(state: Arc<JobsApiState>, ctx: RequestCtx) -> Result<Response, HttpError> {
    match post_ack_batch_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn post_ack_batch_inner(
    state: &JobsApiState,
    ctx: RequestCtx,
) -> Result<Response, JobsApiError> {
    authorize(state, &ctx).await?;
    let stream = ctx
        .params()
        .get("stream")
        .ok_or_else(|| JobsApiError::BadRequest("missing stream".into()))?
        .to_string();
    let req: AckBatchBody = ctx
        .json()
        .map_err(|e| JobsApiError::BadRequest(e.message().to_string()))?;
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
    json_response(StatusCode::OK, AckBatchAccepted { acked })
}

async fn get_job(state: Arc<JobsApiState>, ctx: RequestCtx) -> Result<Response, HttpError> {
    match get_job_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn get_job_inner(state: &JobsApiState, ctx: RequestCtx) -> Result<Response, JobsApiError> {
    authorize(state, &ctx).await?;
    let stream = ctx
        .params()
        .get("stream")
        .ok_or_else(|| JobsApiError::BadRequest("missing stream".into()))?
        .to_string();
    let job_id: u64 = ctx
        .params()
        .get("job_id")
        .ok_or_else(|| JobsApiError::BadRequest("missing job_id".into()))?
        .parse()
        .map_err(|_| JobsApiError::BadRequest("invalid job_id".into()))?;
    let status = (state.job_status)(stream, job_id)
        .await
        .map_err(|e| JobsApiError::Queue(e.to_string()))?;
    let Some(status) = status else {
        return Err(JobsApiError::NotFound);
    };
    json_response(StatusCode::OK, status_to_response(&status))
}

async fn list_jobs(state: Arc<JobsApiState>, ctx: RequestCtx) -> Result<Response, HttpError> {
    match list_jobs_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn list_jobs_inner(state: &JobsApiState, ctx: RequestCtx) -> Result<Response, JobsApiError> {
    authorize(state, &ctx).await?;
    let stream = ctx
        .params()
        .get("stream")
        .ok_or_else(|| JobsApiError::BadRequest("missing stream".into()))?
        .to_string();
    let query: ListJobsQuery = parse_query(&ctx)?;
    let filter = list_filter_from_query(&query)?;
    let page = (state.list_jobs)(stream, filter)
        .await
        .map_err(|e| JobsApiError::Queue(e.to_string()))?;
    json_response(
        StatusCode::OK,
        JobListResponse {
            jobs: page
                .jobs
                .into_iter()
                .map(|j| status_to_response(&j))
                .collect(),
            has_more: page.has_more,
        },
    )
}

async fn post_requeue(state: Arc<JobsApiState>, ctx: RequestCtx) -> Result<Response, HttpError> {
    match post_requeue_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn post_requeue_inner(
    state: &JobsApiState,
    ctx: RequestCtx,
) -> Result<Response, JobsApiError> {
    authorize(state, &ctx).await?;
    let stream = ctx
        .params()
        .get("stream")
        .ok_or_else(|| JobsApiError::BadRequest("missing stream".into()))?
        .to_string();
    let job_id: u64 = ctx
        .params()
        .get("job_id")
        .ok_or_else(|| JobsApiError::BadRequest("missing job_id".into()))?
        .parse()
        .map_err(|_| JobsApiError::BadRequest("invalid job_id".into()))?;
    (state.requeue_dead_letter)(stream, job_id)
        .await
        .map_err(|e| JobsApiError::Queue(e.to_string()))?;
    json_response(StatusCode::OK, RequeueAccepted { job_id })
}

async fn post_requeue_batch(
    state: Arc<JobsApiState>,
    ctx: RequestCtx,
) -> Result<Response, HttpError> {
    match post_requeue_batch_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn post_requeue_batch_inner(
    state: &JobsApiState,
    ctx: RequestCtx,
) -> Result<Response, JobsApiError> {
    authorize(state, &ctx).await?;
    let stream = ctx
        .params()
        .get("stream")
        .ok_or_else(|| JobsApiError::BadRequest("missing stream".into()))?
        .to_string();
    let req: RequeueBatchBody = ctx
        .json()
        .map_err(|e| JobsApiError::BadRequest(e.message().to_string()))?;
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
    json_response(
        StatusCode::OK,
        RequeueBatchAccepted {
            requeued: result.requeued.into_iter().map(|id| id.0).collect(),
            failures: result
                .failures
                .into_iter()
                .map(|(id, err)| RequeueFailureResponse {
                    job_id: id.0,
                    error: err.to_string(),
                })
                .collect(),
        },
    )
}

fn json_response<T: serde::Serialize>(
    status: StatusCode,
    value: T,
) -> Result<Response, JobsApiError> {
    let json = serde_json::to_value(value)
        .map_err(|e| JobsApiError::BadRequest(format!("json encode: {e}")))?;
    Ok(Response::json(status, json))
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
pub fn parse_enqueue_body(
    headers: &http::HeaderMap,
    body: &bytes::Bytes,
) -> Result<Vec<u8>, JobsApiError> {
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
    use bytes::Bytes;
    use http::Method;
    use std::future;
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

    async fn dispatch(
        table: &RouteTable,
        method: Method,
        path: &str,
        body: Bytes,
    ) -> http::StatusCode {
        table
            .dispatch_open(&method, path, HashMap::new(), http::HeaderMap::new(), body)
            .await
            .expect("dispatch")
            .status_code()
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
        let table = route_table(state);
        let status = dispatch(&table, Method::POST, "/jobs/emails", Bytes::from("hello")).await;
        assert_eq!(status, StatusCode::ACCEPTED);
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
        let table = route_table(state);
        let status = dispatch(
            &table,
            Method::POST,
            "/jobs/emails/batch",
            Bytes::from(r#"{"jobs":[{"payload":"a"},{"payload":"b"}]}"#),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
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
        let table = route_table(state);
        let status = dispatch(&table, Method::GET, "/jobs/emails/99", Bytes::new()).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[test]
    fn json_payload_string() {
        let mut headers = http::HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        let body = Bytes::from(r#"{"payload":"hi"}"#);
        assert_eq!(parse_enqueue_body(&headers, &body).unwrap(), b"hi".to_vec());
    }
}
