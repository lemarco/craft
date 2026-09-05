//! HTTP handler and routing errors.

use http::StatusCode;
use thiserror::Error;

/// Error returned by route handlers and gateway wiring.
#[derive(Debug, Error)]
pub enum HttpError {
    /// Client sent a malformed or incomplete request.
    #[error("{0}")]
    BadRequest(String),
    /// Caller is not authenticated.
    #[error("{0}")]
    Unauthorized(String),
    /// Caller lacks permission for the resource.
    #[error("{0}")]
    Forbidden(String),
    /// No route matched method and path.
    #[error("not found")]
    NotFound,
    /// JSON body could not be parsed (Actix-compatible **400**, not 422).
    #[error("{0}")]
    InvalidJson(String),
    /// Handler or upstream failed unexpectedly.
    #[error("{0}")]
    Internal(String),
}

impl HttpError {
    /// HTTP status code for this error.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) | Self::InvalidJson(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Plain-text body for default error responses.
    #[must_use]
    pub fn message(&self) -> &str {
        match self {
            Self::BadRequest(msg)
            | Self::Unauthorized(msg)
            | Self::Forbidden(msg)
            | Self::InvalidJson(msg)
            | Self::Internal(msg) => msg,
            Self::NotFound => "not found",
        }
    }
}
