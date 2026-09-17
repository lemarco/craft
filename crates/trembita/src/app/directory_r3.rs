//! R3 directory visibility snapshot (B-36) — merge lag, NoTarget totals, retry config.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::Serialize;
use trembita_runtime::DirectoryPolicy;

use super::runtime::TrembitaApp;

/// Live R3 counters and directory retry configuration for operators.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DirectoryR3Snapshot {
    /// Active [`DirectoryPolicy`](trembita_runtime::DirectoryPolicy).
    pub directory_policy: String,
    /// Configured retry attempts (before rebalance boost).
    pub directory_retry_max_attempts: u32,
    /// Configured backoff between RYW attempts in milliseconds.
    pub directory_retry_backoff_ms: u64,
    /// Whether post-rebalance retry boost is active on this node.
    pub directory_retry_boost_active: bool,
    /// This node's last published directory epoch.
    pub local_directory_epoch: u64,
    /// Max epoch lag vs cluster members in the merged view.
    pub merge_lag_epochs: u64,
    /// Cumulative NoTarget counts per worker group (since process start).
    pub deliver_no_target_totals: BTreeMap<String, u64>,
}

impl TrembitaApp {
    /// Point-in-time R3 visibility (metrics mirror for JSON introspect).
    #[must_use]
    pub fn directory_r3_snapshot(&self) -> DirectoryR3Snapshot {
        let cluster = self.cluster();
        let messaging = cluster.messaging();
        let policy = messaging.directory_policy();
        let retry = messaging.directory_retry();
        let local_epoch = cluster.directory_local_epoch();
        let merge_lag = cluster
            .directory()
            .merge_lag_epochs(local_epoch, cluster.members());
        let deliver_no_target_totals = messaging
            .directory_stats()
            .map(|s| s.no_target_totals())
            .unwrap_or_default();
        DirectoryR3Snapshot {
            directory_policy: match policy {
                DirectoryPolicy::Eventual => "eventual".into(),
                DirectoryPolicy::ReadYourWrites => "read_your_writes".into(),
            },
            directory_retry_max_attempts: retry.max_attempts,
            directory_retry_backoff_ms: u64::try_from(retry.backoff.as_millis())
                .unwrap_or(u64::MAX),
            directory_retry_boost_active: messaging.directory_retry_boost_active(),
            local_directory_epoch: local_epoch,
            merge_lag_epochs: merge_lag,
            deliver_no_target_totals,
        }
    }
}

#[cfg(feature = "http-jobs")]
pub(crate) fn directory_r3_route_table(app: Arc<TrembitaApp>) -> trembita_http::RouteTable {
    use http::StatusCode;
    use trembita_http::{RequestCtx, Response, RouteTable};

    RouteTable::new().get("/introspect/directory-r3", move |_ctx: RequestCtx| {
        let app = Arc::clone(&app);
        async move {
            let snap = app.directory_r3_snapshot();
            Ok(Response::json(
                StatusCode::OK,
                serde_json::to_value(snap).unwrap_or_default(),
            ))
        }
    })
}
