//! HTTP [`UpgradeApi`](crafty_http::UpgradeApi) wired to a [`CraftyCluster`](crate::cluster::CraftyCluster).

use std::sync::Arc;

use crafty_core::upgrade::{UpgradeCommand, UpgradeQuery, UpgradeResponse};
use crafty_core::UpgradeMachine;
use crafty_http::{UpgradeApi, UpgradeApiError};

use crate::cluster::CraftyCluster;

/// Build cluster upgrade routes backed by Raft propose/query on `cluster`.
#[must_use]
pub fn upgrade_api(cluster: Arc<CraftyCluster<UpgradeMachine>>) -> UpgradeApi {
    let view_cluster = Arc::clone(&cluster);
    let set_cluster = cluster;
    UpgradeApi::new(
        Arc::new(move || {
            let cluster = Arc::clone(&view_cluster);
            Box::pin(async move {
                let members = cluster.members().to_vec();
                let response = cluster
                    .handle()
                    .query(UpgradeQuery::View { members })
                    .await
                    .map_err(|e| UpgradeApiError::Backend(e.to_string()))?;
                match response {
                    UpgradeResponse::View(view) => Ok(view),
                    UpgradeResponse::Ok => Err(UpgradeApiError::Backend(
                        "unexpected upgrade view response".into(),
                    )),
                }
            })
        }),
        Arc::new(move |manifest| {
            let cluster = Arc::clone(&set_cluster);
            Box::pin(async move {
                cluster
                    .handle()
                    .propose(UpgradeCommand::SetDesired(manifest))
                    .await
                    .map_err(|e| UpgradeApiError::Backend(e.to_string()))?;
                Ok(())
            })
        }),
    )
}
