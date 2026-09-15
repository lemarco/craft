//! JSON wire types for the topics HTTP API.

use http::StatusCode;
use serde::Serialize;

use crate::routing::Response;

/// Successful publish response (`202 Accepted`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PublishAccepted {
    /// Assigned event id within the topic log.
    pub event_id: u64,
}

/// Per-subscription gauges in a topic metrics snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TopicSubscriptionMetricsResponse {
    /// Subscription name.
    pub name: String,
    /// Events behind the topic head.
    pub lag: u64,
    /// Pending deliveries.
    pub pending: u64,
    /// In-flight leases.
    pub leased: u64,
    /// Events discarded by retention while this subscription lagged.
    pub retention_discards: u64,
}

/// Topic depth and lag snapshot (`GET /topics/{name}`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TopicMetricsResponse {
    /// Retained events in the log.
    pub event_count: u64,
    /// Log head (last assigned event id, `0` when empty).
    pub head: u64,
    /// Compaction floor.
    pub compact_head: u64,
    /// Age of the oldest retained event, in whole seconds.
    pub oldest_event_age_secs: u64,
    /// Per-subscription lag and counters.
    pub subscriptions: Vec<TopicSubscriptionMetricsResponse>,
}

/// HTTP-layer topic failure mapped to status codes.
#[derive(Debug, thiserror::Error)]
pub enum TopicsApiError {
    /// Request body could not be interpreted.
    #[error("{0}")]
    BadRequest(String),
    /// Unknown topic or subscription.
    #[error("{0}")]
    NotFound(String),
    /// Publish or metrics read failed.
    #[error("{0}")]
    Failed(String),
    /// Gateway identity check failed.
    #[error("{0}")]
    Unauthorized(String),
}

impl TopicsApiError {
    /// Map to a gateway [`Response`].
    #[must_use]
    pub fn into_http_response(self) -> Response {
        let (status, msg) = match &self {
            Self::BadRequest(m) => (StatusCode::BAD_REQUEST, m.clone()),
            Self::NotFound(m) => (StatusCode::NOT_FOUND, m.clone()),
            Self::Failed(m) => (StatusCode::INTERNAL_SERVER_ERROR, m.clone()),
            Self::Unauthorized(m) => (StatusCode::UNAUTHORIZED, m.clone()),
        };
        Response::text(status, msg)
    }
}
