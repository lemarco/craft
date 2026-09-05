//! JSON types for the cluster upgrade API.

use http::StatusCode;
use serde::{Deserialize, Serialize};
use trembita_core::{ArtifactManifest, UpgradeView};

use crate::routing::Response;

/// Body for `POST /cluster/upgrade/desired`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetDesiredBody {
    /// Target application semver.
    pub app_version: String,
    /// Artifact download URL.
    pub url: String,
    /// Lowercase hex SHA-256 of the artifact.
    pub sha256_hex: String,
    /// Optional minimum wire protocol version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_protocol: Option<u32>,
}

impl From<SetDesiredBody> for ArtifactManifest {
    fn from(body: SetDesiredBody) -> Self {
        Self {
            app_version: body.app_version,
            url: body.url,
            sha256_hex: body.sha256_hex,
            min_protocol: body.min_protocol,
        }
    }
}

/// Upgrade API errors.
#[derive(Debug, thiserror::Error)]
pub enum UpgradeApiError {
    /// Missing or invalid credentials.
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    /// Invalid JSON body.
    #[error("bad request: {0}")]
    BadRequest(String),
    /// Backend propose/query failed.
    #[error("upgrade backend: {0}")]
    Backend(String),
}

impl UpgradeApiError {
    /// Map to a gateway [`Response`].
    #[must_use]
    pub fn into_http_response(self) -> Response {
        let status = match &self {
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Backend(_) => StatusCode::SERVICE_UNAVAILABLE,
        };
        Response::text(status, self.to_string())
    }
}

/// JSON snapshot for `GET /cluster/upgrade`.
pub type UpgradeStatusResponse = UpgradeView;
