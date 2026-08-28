//! Axum routes for job enqueue.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::post;
use bytes::Bytes;
use crafty_actor::EnqueueOptions;

use crate::JobsApiState;
use crate::types::{EnqueueAccepted, EnqueueJsonBody, JobsApiError};

/// Query parameters for optional enqueue behaviour.
#[derive(Debug, Default, serde::Deserialize)]
pub struct EnqueueQuery {
    /// Job priority (0–255).
    pub priority: Option<u8>,
    /// Client dedup / idempotency key.
    pub dedup: Option<String>,
}

/// Axum sub-router for `POST /jobs/{stream}`.
pub fn jobs_router() -> Router<Arc<JobsApiState>> {
    Router::new().route("/jobs/{stream}", post(post_job))
}

async fn post_job(
    State(state): State<Arc<JobsApiState>>,
    Path(stream): Path<String>,
    Query(query): Query<EnqueueQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, JobsApiError> {
    let payload = parse_enqueue_body(&headers, &body)?;
    let mut opts = EnqueueOptions::default();
    if let Some(p) = query.priority {
        opts.priority = p;
    }
    if let Some(key) = query.dedup {
        opts.dedup_key = Some(key.into_bytes());
    }
    let job_id = (state.enqueue)(stream, payload, opts)
        .await
        .map_err(|e| JobsApiError::Queue(e.to_string()))?;
    Ok((
        StatusCode::ACCEPTED,
        axum::Json(EnqueueAccepted { job_id: job_id.0 }),
    ))
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
    use crafty_actor::JobId;
    use std::future;
    use tower::ServiceExt;

    #[tokio::test]
    async fn post_job_returns_202_with_id() {
        let state = Arc::new(JobsApiState {
            enqueue: Arc::new(|stream, payload, _opts| {
                assert_eq!(stream, "emails");
                assert_eq!(payload, b"hello");
                Box::pin(future::ready(Ok(JobId(42))))
            }),
        });
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

    #[test]
    fn json_payload_string() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        let body = Bytes::from(r#"{"payload":"hi"}"#);
        assert_eq!(parse_enqueue_body(&headers, &body).unwrap(), b"hi".to_vec());
    }
}
