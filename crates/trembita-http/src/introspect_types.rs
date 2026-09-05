//! JSON wire errors for the introspection HTTP API.

use http::StatusCode;

use crate::routing::Response;

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

impl IntrospectApiError {
    /// Map to a gateway [`Response`].
    #[must_use]
    pub fn into_http_response(self) -> Response {
        let (status, msg) = match &self {
            Self::BadRequest(m) => (StatusCode::BAD_REQUEST, m.clone()),
            Self::NotFound(m) => (StatusCode::NOT_FOUND, m.clone()),
            Self::Unauthorized(m) => (StatusCode::UNAUTHORIZED, m.clone()),
        };
        Response::text(status, msg)
    }
}
