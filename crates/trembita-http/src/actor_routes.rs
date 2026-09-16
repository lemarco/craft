//! Route table for actor cast / ask.

use std::sync::Arc;

use base64::Engine;
use http::StatusCode;
use http::header;

use crate::ActorsApiState;
use crate::actor_types::{ActorsApiError, AskAccepted};
use crate::routes::parse_enqueue_body;
use crate::routing::{HttpError, RequestCtx, Response, RouteTable};
use crate::types::JobsApiError;

/// Route table for actor cast / ask routes.
#[must_use]
pub fn route_table(state: Arc<ActorsApiState>) -> RouteTable {
    let ask_state = Arc::clone(&state);
    let cast_state = state;
    RouteTable::new()
        .post("/actors/{group}/ask", move |ctx| {
            let state = Arc::clone(&ask_state);
            async move { post_ask(state, ctx).await }
        })
        .post("/actors/{group}/cast", move |ctx| {
            let state = Arc::clone(&cast_state);
            async move { post_cast(state, ctx).await }
        })
}

async fn post_ask(state: Arc<ActorsApiState>, ctx: RequestCtx) -> Result<Response, HttpError> {
    match post_ask_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn post_ask_inner(
    state: &ActorsApiState,
    ctx: RequestCtx,
) -> Result<Response, ActorsApiError> {
    let group = ctx
        .params()
        .get("group")
        .ok_or_else(|| ActorsApiError::BadRequest("missing group".into()))?
        .to_string();
    let (payload, _) = parse_enqueue_body(ctx.headers(), ctx.body()).map_err(map_body_error)?;
    let reply = (state.ask)(group, payload).await.map_err(map_ask_error)?;
    let ct = ctx
        .headers()
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if ct.starts_with("application/octet-stream") {
        return Ok(Response::text(
            StatusCode::OK,
            String::from_utf8_lossy(&reply),
        ));
    }
    let json = serde_json::to_value(AskAccepted {
        reply_b64: base64::engine::general_purpose::STANDARD.encode(reply),
    })
    .map_err(|e| ActorsApiError::BadRequest(format!("json encode: {e}")))?;
    Ok(Response::json(StatusCode::OK, json))
}

async fn post_cast(state: Arc<ActorsApiState>, ctx: RequestCtx) -> Result<Response, HttpError> {
    match post_cast_inner(&state, ctx).await {
        Ok(r) => Ok(r),
        Err(e) => Ok(e.into_http_response()),
    }
}

async fn post_cast_inner(
    state: &ActorsApiState,
    ctx: RequestCtx,
) -> Result<Response, ActorsApiError> {
    let group = ctx
        .params()
        .get("group")
        .ok_or_else(|| ActorsApiError::BadRequest("missing group".into()))?
        .to_string();
    let (payload, _) = parse_enqueue_body(ctx.headers(), ctx.body()).map_err(map_body_error)?;
    (state.cast)(group, payload).await.map_err(map_cast_error)?;
    Ok(Response::status(StatusCode::ACCEPTED))
}

fn map_body_error(err: JobsApiError) -> ActorsApiError {
    match err {
        JobsApiError::NotFound => ActorsApiError::BadRequest("not found".into()),
        JobsApiError::BadRequest(m) | JobsApiError::Queue(m) => ActorsApiError::BadRequest(m),
        JobsApiError::Unauthorized(m) => ActorsApiError::Unauthorized(m),
    }
}

fn map_ask_error(err: trembita_runtime::ClusterAskError) -> ActorsApiError {
    match err {
        trembita_runtime::ClusterAskError::NoTarget(g) => {
            ActorsApiError::NoTarget(format!("no live instance of group `{g}`"))
        }
        trembita_runtime::ClusterAskError::Timeout(_)
        | trembita_runtime::ClusterAskError::NoReply => ActorsApiError::Timeout,
        other => ActorsApiError::Actor(other.to_string()),
    }
}

fn map_cast_error(err: trembita_runtime::CastError) -> ActorsApiError {
    match err {
        trembita_runtime::CastError::NoTarget(g) => {
            ActorsApiError::NoTarget(format!("no live instance of group `{g}`"))
        }
        other => ActorsApiError::Actor(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::Method;
    use std::collections::HashMap;
    use std::future;
    use trembita_runtime::ClusterAskError;

    fn test_state(ask: crate::AskFn, cast: crate::CastFn) -> Arc<ActorsApiState> {
        Arc::new(ActorsApiState { ask, cast })
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
        let table = route_table(state);
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            "application/json".parse().expect("ct"),
        );
        let resp = table
            .dispatch_open(
                &Method::POST,
                "/actors/workers/ask",
                HashMap::new(),
                headers,
                Bytes::from(r#"{"payload":"ping"}"#),
            )
            .await
            .expect("dispatch");
        assert_eq!(resp.status_code(), StatusCode::OK);
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
        let table = route_table(state);
        let resp = table
            .dispatch_open(
                &Method::POST,
                "/actors/workers/ask",
                HashMap::new(),
                http::HeaderMap::new(),
                Bytes::from(r#"{"payload":"x"}"#),
            )
            .await
            .expect("dispatch");
        assert_eq!(resp.status_code(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
