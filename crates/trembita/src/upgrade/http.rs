//! HTTP [`UpgradeApi`](trembita_http::UpgradeApi) wired to a [`TrembitaCluster`](crate::cluster_handle::TrembitaCluster).

use std::sync::Arc;

use trembita_core::UpgradeCommand;
use trembita_core::UpgradeMachine;
use trembita_http::{UpgradeApi, UpgradeApiError};

use super::client::{propose_upgrade, query_upgrade_view};
use crate::cluster_handle::TrembitaCluster;

/// Build cluster upgrade routes backed by Raft propose/query on `cluster`.
#[must_use]
pub fn upgrade_api(cluster: Arc<TrembitaCluster<UpgradeMachine>>) -> UpgradeApi {
    let view_cluster = Arc::clone(&cluster);
    let set_cluster = cluster;
    UpgradeApi::new(
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
    )
}
