//! Authenticated HTTP handlers on the product gateway (POST + GET examples).

use std::time::Duration;

use http::StatusCode;
use serde::{Deserialize, Serialize};
use trembita::TrembitaGatewayState;
use trembita_http::{HttpError, RequestCtx, Response};

use crate::debug;

const SESSION_TTL: Duration = Duration::from_secs(3600);

#[derive(Deserialize)]
pub struct ChatPost {
    /// Chat line to cast to the sticky worker.
    pub message: String,
}

#[derive(Serialize)]
pub struct ChatAck {
    /// Whether the cast was accepted by the runtime.
    pub ok: bool,
    /// Sticky session key (authenticated user).
    pub user: String,
}

#[derive(Serialize)]
pub struct MeResponse {
    /// Authenticated user id / session key.
    pub user: String,
}

fn ctx_uri(ctx: &RequestCtx) -> http::Uri {
    let query = ctx.query();
    if query.is_empty() {
        ctx.path().parse().expect("path uri")
    } else {
        let qs = query
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        format!("{}?{qs}", ctx.path()).parse().expect("path+query uri")
    }
}

/// `POST /chat` — JSON body; auth via Bearer + `X-Trembita-User` or `?user=` (see `GatewayBearerIdentity`).
pub async fn post_chat(
    state: TrembitaGatewayState,
    ctx: RequestCtx,
) -> Result<Response, HttpError> {
    let uri = ctx_uri(&ctx);
    let mut handle = state
        .open_actor_session_parts("chat", ctx.method(), &uri, ctx.headers(), Some(SESSION_TTL))
        .await
        .map_err(session_err)?;
    let body: ChatPost = ctx.json()?;
    let user = handle.session_key().to_string();
    let payload = trembita::proto::encode(&body.message).map_err(|e| {
        HttpError::Internal(format!("encode: {e}"))
    })?;
    match handle.cast(payload).await {
        Ok(()) => {
            debug::http_message(&user, &body.message, true);
            Ok(Response::json(
                StatusCode::OK,
                serde_json::to_value(ChatAck { ok: true, user }).expect("json"),
            ))
        }
        Err(e) => {
            debug::http_message(&user, &body.message, false);
            Ok(Response::text(StatusCode::BAD_GATEWAY, e.to_string()))
        }
    }
}

/// `GET /me?user=…` — auth check; returns session key as JSON.
pub async fn get_me(state: TrembitaGatewayState, ctx: RequestCtx) -> Result<Response, HttpError> {
    let uri = ctx_uri(&ctx);
    match state
        .extract_session_parts(ctx.method(), &uri, ctx.headers())
        .await
    {
        Ok(extracted) => Ok(Response::json(
            StatusCode::OK,
            serde_json::to_value(MeResponse {
                user: extracted.session_key().to_string(),
            })
            .expect("json"),
        )),
        Err(err) => Ok(err.into_http_response()),
    }
}

fn session_err(err: trembita::OpenActorSessionError) -> HttpError {
    match err {
        trembita::OpenActorSessionError::Identity(e) => HttpError::Unauthorized(e.to_string()),
        trembita::OpenActorSessionError::NoWorker(e) => HttpError::Internal(e.to_string()),
    }
}
