//! JSON wire types for the actors HTTP API.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

/// Successful ask response (`200 OK`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AskAccepted {
    /// Standard base64-encoded reply bytes from the actor.
    pub reply_b64: String,
}

/// HTTP-layer actor delivery failure mapped to status codes.
#[derive(Debug, thiserror::Error)]
pub enum ActorsApiError {
    /// Request body could not be interpreted.
    #[error("{0}")]
    BadRequest(String),
    /// No live worker in the group.
    #[error("{0}")]
    NoTarget(String),
    /// The actor did not reply in time.
    #[error("actor did not reply in time")]
    Timeout,
    /// Delivery or reply failed at the actor layer.
    #[error("{0}")]
    Actor(String),
    /// Gateway identity check failed.
    #[error("{0}")]
    Unauthorized(String),
}

impl IntoResponse for ActorsApiError {
    fn into_response(self) -> Response {
        let (status, msg) = match &self {
            Self::BadRequest(m) => (StatusCode::BAD_REQUEST, m.clone()),
            Self::NoTarget(m) => (StatusCode::SERVICE_UNAVAILABLE, m.clone()),
            Self::Timeout => (StatusCode::GATEWAY_TIMEOUT, self.to_string()),
            Self::Actor(m) => (StatusCode::BAD_GATEWAY, m.clone()),
            Self::Unauthorized(m) => (StatusCode::UNAUTHORIZED, m.clone()),
        };
        (status, msg).into_response()
    }
}
