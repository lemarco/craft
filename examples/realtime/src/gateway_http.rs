//! Authenticated HTTP handlers on the product gateway (POST + GET examples).

use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use crafty::CraftyGatewayState;
use serde::{Deserialize, Serialize};

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

/// `POST /chat` — JSON body; auth via Bearer + `X-Crafty-User` or `?user=` (see `GatewayBearerIdentity`).
pub async fn post_chat(
    State(state): State<CraftyGatewayState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Json(body): Json<ChatPost>,
) -> Response {
    let mut handle = match state
        .open_actor_session_parts("chat", &method, &uri, &headers, Some(SESSION_TTL))
        .await
    {
        Ok(h) => h,
        Err(err) => return err.into_response(),
    };
    let user = handle.session_key().to_string();
    let payload = match crafty::proto::encode(&body.message) {
        Ok(p) => p,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };
    match handle.cast(payload).await {
        Ok(()) => {
            debug::http_message(&user, &body.message, true);
            Json(ChatAck { ok: true, user }).into_response()
        }
        Err(e) => {
            debug::http_message(&user, &body.message, false);
            (StatusCode::BAD_GATEWAY, e.to_string()).into_response()
        }
    }
}

/// `GET /me?user=…` — auth check; returns session key as JSON.
///
/// Uses [`CraftyGatewayState::extract_session_parts`] (same as POST). Call
/// [`CraftyGatewayState::extract_session_from`] when middleware already gave you a
/// [`Request`](axum::http::Request).
pub async fn get_me(
    State(state): State<CraftyGatewayState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    match state
        .extract_session_parts(&method, &uri, &headers)
        .await
    {
        Ok(extracted) => Json(MeResponse {
            user: extracted.session_key().to_string(),
        })
        .into_response(),
        Err(err) => err.into_response(),
    }
}
