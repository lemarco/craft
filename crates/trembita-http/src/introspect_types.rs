//! JSON wire errors for the introspection HTTP API.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// HTTP-layer introspection failure mapped to status codes.
#[derive(Debug, thiserror::Error)]
pub enum IntrospectApiError {
    /// Path parameter could not be parsed.
    #[error("{0}")]
    BadRequest(String),
    /// Requested actor or node was not found.
    #[error("{0}")]
    NotFound(String),
    /// Gateway identity check failed.
    #[error("{0}")]
    Unauthorized(String),
}

impl IntoResponse for IntrospectApiError {
    fn into_response(self) -> Response {
        let (status, msg) = match &self {
            Self::BadRequest(m) => (StatusCode::BAD_REQUEST, m.clone()),
            Self::NotFound(m) => (StatusCode::NOT_FOUND, m.clone()),
            Self::Unauthorized(m) => (StatusCode::UNAUTHORIZED, m.clone()),
        };
        (status, msg).into_response()
    }
}
