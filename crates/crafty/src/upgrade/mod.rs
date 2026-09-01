//! Rolling self-update coordinator ([upgrade-coordinator](../../../docs/decisions/upgrade-coordinator.md)).
//!
//! Reference [`UpgradeMachine`] in `crafty-core`, leader reconcile + local executor in
//! [`spawn_upgrade_coordinator`], HTTP hooks via `crafty-http` [`UpgradeApi`](crafty_http::UpgradeApi).

mod client;
mod coordinator;
mod fetch;
#[cfg(feature = "http-jobs")]
mod http;
mod install;

pub use coordinator::{UpgradeOpts, UpgradeRunError, spawn_upgrade_coordinator};
pub use fetch::{UpgradeFetchError, fetch_artifact};
#[cfg(feature = "http-jobs")]
pub use http::upgrade_api;
pub use install::{
    UpgradeInstallError, atomic_symlink_install, running_app_version, verify_sha256_hex,
};

pub use crafty_core::upgrade::{
    ArtifactManifest, UpgradeCommand, UpgradeError, UpgradeMachine, UpgradePhase, UpgradeQuery,
    UpgradeResponse, UpgradeState, UpgradeStateMachine, UpgradeView, plan_next_grant,
    upgrade_state_for_planning, upgrade_view,
};

use std::sync::Arc;

use crafty_core::UpgradeMachine as UpgradeStateMachineAlias;

use crate::cluster_handle::CraftyCluster;

use self::client::{propose_upgrade, query_upgrade_view};

/// After restart, report `Ready` when the running build matches the desired manifest.
///
/// # Errors
/// Propagates Raft client errors.
pub async fn report_upgrade_boot(
    cluster: &CraftyCluster<UpgradeStateMachineAlias>,
) -> Result<(), UpgradeRunError> {
    let members: Vec<crafty_proto::NodeId> = cluster.members().to_vec();
    let view = query_upgrade_view(cluster, &members).await?;
    if view
        .desired
        .as_ref()
        .is_some_and(|d| d.app_version == running_app_version())
        && !view.completed.contains(&cluster.node_id())
    {
        propose_upgrade(
            cluster,
            crafty_core::UpgradeCommand::Report {
                node_id: cluster.node_id(),
                phase: crafty_core::UpgradePhase::Ready,
            },
        )
        .await?;
    }
    Ok(())
}

/// Convenience: boot hook + background coordinator.
pub fn spawn_upgrade_runtime(
    cluster: Arc<CraftyCluster<UpgradeStateMachineAlias>>,
    opts: UpgradeOpts,
) -> tokio::task::JoinHandle<()> {
    let cluster_for_boot = Arc::clone(&cluster);
    tokio::spawn(async move {
        if let Err(e) = report_upgrade_boot(&cluster_for_boot).await {
            tracing::warn!(error = %e, "upgrade boot report failed");
        }
    });
    spawn_upgrade_coordinator(cluster, opts)
}
