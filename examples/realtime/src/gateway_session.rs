//! Cookie session flow: login → cluster-signed `Set-Cookie` → `post_session` routes.
//!
//! Any gateway node verifies the cookie with `TREMBITA_GATEWAY_SESSION_SECRET` (no per-process `HashSet`).

use std::time::Duration;

use http::StatusCode;
use serde::{Deserialize, Serialize};
use trembita::{
    ClusterSessionSecret, CookieConfig, HttpError, RequestCtx, Response, SessionGate,
    SessionHandle, TrembitaGatewayState, cluster_session_gate, session_user_from_cookie,
};

use crate::capabilities::chat::Append;
use crate::debug;

const SESSION_TTL: Duration = Duration::from_secs(3600);

/// Shared cluster session wiring for the realtime gateway surface.
#[derive(Clone)]
pub struct ClusterGatewaySession {
    /// HMAC secret — same on every node in the cluster.
    pub secret: ClusterSessionSecret,
    /// Cookie gate for `AuthMode::Session` routes.
    pub gate: SessionGate,
    /// Cookie name (`REALTIME_COOKIE_*` / `sess`).
    pub cookie_name: String,
}

impl ClusterGatewaySession {
    /// Load secret from env (or showcase dev fallback) and build [`SessionGate`].
    #[must_use]
    pub fn from_env() -> Self {
        let secret = ClusterSessionSecret::from_env().unwrap_or_else(|_| {
            eprintln!(
                "warning: TREMBITA_GATEWAY_SESSION_SECRET unset — using showcase dev secret (single-node only)"
            );
            ClusterSessionSecret::showcase_dev()
        });
        let cookie = CookieConfig::from_env("REALTIME", "sess");
        let cookie_name = cookie.name.clone();
        let gate = cluster_session_gate(secret.clone(), cookie);
        Self {
            secret,
            gate,
            cookie_name,
        }
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

/// `POST /login` — gateway identity → issue cluster session cookie.
pub async fn post_login(
    state: TrembitaGatewayState,
    session: ClusterGatewaySession,
    ctx: RequestCtx,
) -> Result<Response, HttpError> {
    let extracted = state
        .extract_session_parts(ctx.method(), &ctx.uri(), ctx.headers())
        .await
        .map_err(|e| HttpError::Unauthorized(e.to_string()))?;
    let user = extracted.session_key().to_string();
    let token = session
        .secret
        .issue(&user, SESSION_TTL)
        .map_err(|e| HttpError::Internal(e.to_string()))?;
    let mut resp = Response::json(
        StatusCode::OK,
        serde_json::to_value(LoginResponse { user: user.clone() }).expect("json"),
    );
    session.gate.set_session_cookie(&mut resp, &token)?;
    debug::session_open(&user, true);
    Ok(resp)
}

/// `POST /chat` — requires session cookie (see [`post_login`]).
pub async fn post_chat(
    state: TrembitaGatewayState,
    session: ClusterGatewaySession,
    ctx: RequestCtx,
) -> Result<Response, HttpError> {
    let user = session_user_from_cookie(&session.cookie_name, ctx.headers(), &session.secret)?;
    let mut handle = SessionHandle::open_for::<Append>(&state.app, &user, Some(SESSION_TTL))
        .ok_or_else(|| HttpError::Internal("no chat worker".into()))?;
    let body: ChatPost = ctx.json()?;
    match handle
        .fire_cap(Append {
            text: body.message.clone(),
        })
        .await
    {
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
pub async fn get_me(
    session: ClusterGatewaySession,
    ctx: RequestCtx,
) -> Result<Response, HttpError> {
    let user = session_user_from_cookie(&session.cookie_name, ctx.headers(), &session.secret)?;
    Ok(Response::json(
        StatusCode::OK,
        serde_json::to_value(MeResponse { user }).expect("json"),
    ))
}
