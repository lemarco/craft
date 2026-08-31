//! JSON types for the cluster upgrade API.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use crafty_core::{ArtifactManifest, UpgradeView};
use serde::{Deserialize, Serialize};

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
    /// Invalid JSON body.
    #[error("bad request: {0}")]
    BadRequest(String),
    /// Backend propose/query failed.
    #[error("upgrade backend: {0}")]
    Backend(String),
}

impl IntoResponse for UpgradeApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Backend(_) => StatusCode::SERVICE_UNAVAILABLE,
        };
        (status, self.to_string()).into_response()
    }
}

/// JSON snapshot for `GET /cluster/upgrade`.
pub type UpgradeStatusResponse = UpgradeView;
