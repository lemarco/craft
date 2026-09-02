//! Raft propose/query with transparent leader forwarding (any cluster member).

use trembita_client::{RemoteClient, TypedClient};
use trembita_core::UpgradeMachine;
use trembita_core::upgrade::{UpgradeCommand, UpgradeQuery, UpgradeResponse, UpgradeView};

use super::coordinator::UpgradeRunError;
use crate::cluster_handle::TrembitaCluster;

fn typed_client(
    cluster: &TrembitaCluster<UpgradeMachine>,
) -> TypedClient<RemoteClient, UpgradeMachine> {
    TypedClient::new(RemoteClient::new(
        cluster.transport.clone(),
        cluster.members().to_vec(),
    ))
}

/// Propose an upgrade command (forwards to the leader when called on a follower).
pub async fn propose_upgrade(
    cluster: &TrembitaCluster<UpgradeMachine>,
    command: UpgradeCommand,
) -> Result<UpgradeResponse, UpgradeRunError> {
    typed_client(cluster)
        .propose(&command)
        .await
        .map_err(Into::into)
}

/// Linearizable upgrade fleet view (forwards to the leader when needed).
pub async fn query_upgrade_view(
    cluster: &TrembitaCluster<UpgradeMachine>,
    members: &[trembita_proto::NodeId],
) -> Result<UpgradeView, UpgradeRunError> {
    let response = typed_client(cluster)
        .query(&UpgradeQuery::View {
            members: members.to_vec(),
        })
        .await?;
    match response {
        UpgradeResponse::View(view) => Ok(view),
        UpgradeResponse::Ok => Err(UpgradeRunError::Client(
            trembita_runtime::ClientError::Driver("unexpected upgrade query response".into()),
        )),
    }
}
