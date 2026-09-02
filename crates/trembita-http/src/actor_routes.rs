//! Axum routes for actor cast / ask.

use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::IntoResponse;
use axum::routing::post;
use base64::Engine;
use bytes::Bytes;
use trembita_runtime::{CastError, ClusterAskError};

use crate::ActorsApiState;
use crate::actor_types::{ActorsApiError, AskAccepted};
use crate::routes::parse_enqueue_body;
use crate::types::JobsApiError;

/// Axum sub-router for actor cast / ask routes.
pub fn actors_router() -> Router<Arc<ActorsApiState>> {
    Router::new()
        .route("/actors/{group}/ask", post(post_ask))
        .route("/actors/{group}/cast", post(post_cast))
}

async fn authorize(
    state: &ActorsApiState,
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
) -> Result<(), ActorsApiError> {
    if let Some(auth) = &state.auth {
        auth(method.clone(), uri.clone(), headers.clone())
            .await
            .map_err(|e| match e {
                JobsApiError::Unauthorized(m) => ActorsApiError::Unauthorized(m),
                other => ActorsApiError::BadRequest(other.to_string()),
            })?;
    }
    Ok(())
}

async fn post_ask(
    State(state): State<Arc<ActorsApiState>>,
    Path(group): Path<String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<axum::response::Response, ActorsApiError> {
    authorize(&state, &method, &uri, &headers).await?;
    let payload = parse_enqueue_body(&headers, &body).map_err(map_body_error)?;
    let reply = (state.ask)(group, payload).await.map_err(map_ask_error)?;
    let ct = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if ct.starts_with("application/octet-stream") {
        return Ok((StatusCode::OK, reply).into_response());
    }
    Ok((
        StatusCode::OK,
        axum::Json(AskAccepted {
            reply_b64: base64::engine::general_purpose::STANDARD.encode(reply),
        }),
    )
        .into_response())
}

async fn post_cast(
    State(state): State<Arc<ActorsApiState>>,
    Path(group): Path<String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, ActorsApiError> {
    authorize(&state, &method, &uri, &headers).await?;
    let payload = parse_enqueue_body(&headers, &body).map_err(map_body_error)?;
    (state.cast)(group, payload).await.map_err(map_cast_error)?;
    Ok(StatusCode::ACCEPTED)
}

fn map_body_error(err: JobsApiError) -> ActorsApiError {
    match err {
        JobsApiError::NotFound => ActorsApiError::BadRequest("not found".into()),
        JobsApiError::BadRequest(m) | JobsApiError::Queue(m) => ActorsApiError::BadRequest(m),
        JobsApiError::Unauthorized(m) => ActorsApiError::Unauthorized(m),
    }
}

fn map_ask_error(err: ClusterAskError) -> ActorsApiError {
    match err {
        ClusterAskError::NoTarget(g) => {
            ActorsApiError::NoTarget(format!("no live instance of group `{g}`"))
        }
        ClusterAskError::Timeout(_) | ClusterAskError::NoReply => ActorsApiError::Timeout,
        other => ActorsApiError::Actor(other.to_string()),
    }
}

fn map_cast_error(err: CastError) -> ActorsApiError {
    match err {
        CastError::NoTarget(g) => {
            ActorsApiError::NoTarget(format!("no live instance of group `{g}`"))
        }
        other => ActorsApiError::Actor(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use std::future;
    use tower::ServiceExt;

    fn test_state(ask: crate::AskFn, cast: crate::CastFn) -> Arc<ActorsApiState> {
        Arc::new(ActorsApiState {
            ask,
            cast,
            auth: None,
        })
    }

    #[tokio::test]
    async fn post_ask_returns_json_reply() {
        let state = test_state(
            Arc::new(|group, payload| {
                assert_eq!(group, "workers");
                assert_eq!(payload, b"ping");
                Box::pin(future::ready(Ok(b"pong".to_vec())))
            }),
            Arc::new(|_, _| Box::pin(future::ready(Ok(())))),
        );
        let app = actors_router().with_state(state);
        let req = Request::builder()
            .method("POST")
            .uri("/actors/workers/ask")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"payload":"ping"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn post_cast_returns_202() {
        let state = test_state(
            Arc::new(|_, _| Box::pin(future::ready(Ok(Vec::new())))),
            Arc::new(|group, payload| {
                assert_eq!(group, "workers");
                assert_eq!(payload, b"hi");
                Box::pin(future::ready(Ok(())))
            }),
        );
        let app = actors_router().with_state(state);
        let req = Request::builder()
            .method("POST")
            .uri("/actors/workers/cast")
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::from("hi"))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn post_ask_no_target_returns_503() {
        let state = test_state(
            Arc::new(|_, _| {
                Box::pin(future::ready(Err(ClusterAskError::NoTarget(
                    "workers".into(),
                ))))
            }),
            Arc::new(|_, _| Box::pin(future::ready(Ok(())))),
        );
        let app = actors_router().with_state(state);
        let req = Request::builder()
            .method("POST")
            .uri("/actors/workers/ask")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"payload":"x"}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
