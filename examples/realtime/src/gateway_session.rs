//! Cookie session flow: login → `Set-Cookie` → `post_session` routes.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use http::StatusCode;
use serde::{Deserialize, Serialize};
use trembita::{HttpError, RequestCtx, Response, SessionGate, SessionHandle, TrembitaGatewayState};

use crate::debug;

const SESSION_TTL: Duration = Duration::from_secs(3600);

/// In-memory session tokens issued by [`post_login`].
#[derive(Clone, Default)]
pub struct SessionStore(Arc<Mutex<HashSet<String>>>);

impl SessionStore {
    /// Empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a token as valid until logout / expiry (showcase only).
    pub fn insert(&self, token: impl Into<String>) {
        self.0.lock().unwrap().insert(token.into());
    }

    /// Whether `token` was issued by login.
    #[must_use]
    pub fn contains(&self, token: &str) -> bool {
        self.0.lock().unwrap().contains(token)
    }
}

#[derive(Serialize)]
pub struct LoginResponse {
    /// Authenticated user / session key.
    pub user: String,
}

#[derive(Deserialize)]
pub struct ChatPost {
    /// Chat line to cast to the sticky worker.
    pub message: String,
}

#[derive(Serialize)]
pub struct ChatAck {
    /// Whether the cast was accepted by the runtime.
    pub ok: bool,
    /// Sticky session key.
    pub user: String,
}

#[derive(Serialize)]
pub struct MeResponse {
    /// Session key from the cookie.
    pub user: String,
}

/// Build a [`SessionGate`] backed by `store` with showcase cookie defaults.
#[must_use]
pub fn session_gate(store: SessionStore) -> SessionGate {
    let cookie = trembita::CookieConfig::from_env("REALTIME", "sess");
    SessionGate::validate(cookie.name.clone(), {
        let store = store.clone();
        move |token| {
            let store = store.clone();
            async move {
                if store.contains(&token) {
                    Ok(())
                } else {
                    Err(HttpError::Unauthorized("invalid or expired session".into()))
                }
            }
        }
    })
    .with_cookie_config(cookie)
}

/// `POST /login` — gateway identity → issue session cookie (`Set-Cookie: sess=…`).
pub async fn post_login(
    state: TrembitaGatewayState,
    gate: SessionGate,
    store: SessionStore,
    ctx: RequestCtx,
) -> Result<Response, HttpError> {
    let extracted = state
        .extract_session_parts(ctx.method(), &ctx.uri(), ctx.headers())
        .await
        .map_err(|e| HttpError::Unauthorized(e.to_string()))?;
    let user = extracted.session_key().to_string();
    store.insert(&user);
    let mut resp = Response::json(
        StatusCode::OK,
        serde_json::to_value(LoginResponse { user: user.clone() }).expect("json"),
    );
    gate.set_session_cookie(&mut resp, &user)?;
    debug::session_open(&user, true);
    Ok(resp)
}

/// `POST /chat` — requires session cookie (see [`post_login`]).
pub async fn post_chat(
    state: TrembitaGatewayState,
    ctx: RequestCtx,
) -> Result<Response, HttpError> {
    let token = ctx
        .cookie("sess")
        .ok_or_else(|| HttpError::Unauthorized("missing session cookie".into()))?;
    let mut handle = SessionHandle::open(&state.app, "chat", token, Some(SESSION_TTL))
        .ok_or_else(|| HttpError::Internal("no chat worker".into()))?;
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

/// `GET /me` — session cookie → JSON profile.
pub async fn get_me(ctx: RequestCtx) -> Result<Response, HttpError> {
    let user = ctx
        .cookie("sess")
        .ok_or_else(|| HttpError::Unauthorized("missing session cookie".into()))?
        .to_string();
    Ok(Response::json(
        StatusCode::OK,
        serde_json::to_value(MeResponse { user }).expect("json"),
    ))
}
