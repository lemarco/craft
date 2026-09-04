//! HTTP [`UpgradeApi`](trembita_http::UpgradeApi) wired to a [`TrembitaCluster`](crate::cluster_handle::TrembitaCluster).

use std::sync::Arc;

use trembita_core::UpgradeCommand;
use trembita_core::UpgradeMachine;
use trembita_http::{AuthFn, UpgradeApi, UpgradeApiError};

use super::client::{propose_upgrade, query_upgrade_view};
use crate::cluster_handle::TrembitaCluster;
use crate::gateway::bearer_auth_from_env;

/// Build cluster upgrade routes backed by Raft propose/query on `cluster`.
///
/// When `GATEWAY_TOKEN` / `TREMBITA_GATEWAY_TOKEN` is set, routes require Bearer auth.
#[must_use]
pub fn upgrade_api(cluster: Arc<TrembitaCluster<UpgradeMachine>>) -> UpgradeApi {
    upgrade_api_with_auth(cluster, bearer_auth_from_env())
}

/// Like [`upgrade_api`] with an explicit auth hook (`None` = open routes; not recommended).
#[must_use]
pub fn upgrade_api_with_auth(
    cluster: Arc<TrembitaCluster<UpgradeMachine>>,
    auth: Option<AuthFn>,
) -> UpgradeApi {
    let view_cluster = Arc::clone(&cluster);
    let set_cluster = cluster;
    let mut api = UpgradeApi::new(
        Arc::new(move || {
            let cluster = Arc::clone(&view_cluster);
            Box::pin(async move {
                let members = cluster.members().to_vec();
                query_upgrade_view(cluster.as_ref(), &members)
                    .await
                    .map_err(|e| UpgradeApiError::Backend(e.to_string()))
            })
        }),
        Arc::new(move |manifest| {
            let cluster = Arc::clone(&set_cluster);
            Box::pin(async move {
                propose_upgrade(cluster.as_ref(), UpgradeCommand::SetDesired(manifest))
                    .await
                    .map(|_| ())
                    .map_err(|e| UpgradeApiError::Backend(e.to_string()))
            })
        }),
    );
    if let Some(auth) = auth {
        api = api.with_auth(auth);
    }
    api
}
