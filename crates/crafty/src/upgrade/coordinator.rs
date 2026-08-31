//! Leader reconcile + local executor loop for [`UpgradeMachine`](crafty_core::UpgradeMachine).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crafty_core::upgrade::{
    UpgradeCommand, UpgradePhase, plan_next_grant, upgrade_state_for_planning,
};
use crafty_core::UpgradeMachine;
use crafty_proto::NodeId;
use thiserror::Error;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::client::{propose_upgrade, query_upgrade_view};
use super::fetch::{UpgradeFetchError, fetch_artifact};
use super::install::{
    UpgradeInstallError, atomic_symlink_install, running_app_version, verify_sha256_hex,
};
use crate::cluster::CraftyCluster;

/// Options for [`spawn_upgrade_coordinator`].
#[derive(Clone, Debug)]
pub struct UpgradeOpts {
    /// Directory for versioned binaries (`app-{version}`).
    pub install_dir: PathBuf,
    /// Symlink updated atomically (`current` → active binary).
    pub current_link: PathBuf,
    /// Reconcile tick interval.
    pub tick_period: Duration,
    /// Download/verify/install without `leave()` / process exit (demos/tests).
    pub dry_run: bool,
}

impl UpgradeOpts {
    /// Sensible defaults under `/opt/crafty/bin`.
    #[must_use]
    pub fn default_paths() -> Self {
        Self {
            install_dir: PathBuf::from("/opt/crafty/bin"),
            current_link: PathBuf::from("/opt/crafty/bin/current"),
            tick_period: Duration::from_secs(10),
            dry_run: false,
        }
    }

    /// Demo-friendly paths under a data directory.
    #[must_use]
    pub fn under_data_dir(data_dir: impl AsRef<Path>) -> Self {
        let base = data_dir.as_ref().join("bin");
        Self {
            install_dir: base.clone(),
            current_link: base.join("current"),
            tick_period: Duration::from_secs(5),
            dry_run: false,
        }
    }
}

/// Coordinator/runtime errors (logged; surfaced via SM `Failed` reports).
#[derive(Debug, Error)]
pub enum UpgradeRunError {
    /// Raft client error.
    #[error("{0}")]
    Client(#[from] crafty_actor::ClientError),
    /// Remote client error (leader forwarding).
    #[error("{0}")]
    Remote(#[from] crafty_client::ClientError),
    /// Download failed.
    #[error("{0}")]
    Fetch(#[from] UpgradeFetchError),
    /// Install failed.
    #[error("{0}")]
    Install(#[from] UpgradeInstallError),
}

/// Spawn background leader reconcile + local executor tasks.
///
/// Returns a join handle; drop or abort to stop ticking (does not cancel in-flight download).
pub fn spawn_upgrade_coordinator(
    cluster: Arc<CraftyCluster<UpgradeMachine>>,
    opts: UpgradeOpts,
) -> JoinHandle<()> {
    let executing = Arc::new(AtomicBool::new(false));
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(opts.tick_period);
        loop {
            interval.tick().await;
            if let Err(e) = tick(&cluster, &opts, &executing).await {
                warn!(error = %e, "upgrade tick failed");
            }
        }
    })
}

async fn tick(
    cluster: &Arc<CraftyCluster<UpgradeMachine>>,
    opts: &UpgradeOpts,
    executing: &Arc<AtomicBool>,
) -> Result<(), UpgradeRunError> {
    let members: Vec<NodeId> = cluster.members().to_vec();
    let view = query_upgrade_view(cluster, &members).await?;

    if cluster.is_leader().await {
        leader_reconcile(cluster, &members, &view).await?;
    }

    if view.granted == Some(cluster.node_id())
        && !executing.swap(true, Ordering::SeqCst)
        && view.desired.is_some()
    {
        let cluster = Arc::clone(cluster);
        let opts = opts.clone();
        let executing = Arc::clone(executing);
        tokio::spawn(async move {
            if let Err(e) = run_local_upgrade(&cluster, &opts).await {
                warn!(error = %e, "local upgrade failed");
                let _ = propose_upgrade(
                    cluster.as_ref(),
                    UpgradeCommand::Report {
                        node_id: cluster.node_id(),
                        phase: UpgradePhase::Failed {
                            message: e.to_string(),
                        },
                    },
                )
                .await;
            }
            executing.store(false, Ordering::SeqCst);
        });
    }

    Ok(())
}

async fn leader_reconcile(
    cluster: &CraftyCluster<UpgradeMachine>,
    members: &[NodeId],
    view: &crafty_core::UpgradeView,
) -> Result<(), UpgradeRunError> {
    if view.aborted.is_some() || view.desired.is_none() || view.fleet_ready {
        return Ok(());
    }
    if view.granted.is_some() {
        return Ok(());
    }
    let leader_id = cluster
        .status()
        .await
        .and_then(|s| s.leader)
        .unwrap_or(cluster.node_id());
    let state = upgrade_state_for_planning(view);
    let Some(next) = plan_next_grant(&state, members, leader_id) else {
        return Ok(());
    };
    info!(?next, "upgrade coordinator granting slot");
    propose_upgrade(cluster, UpgradeCommand::Grant { node_id: next }).await?;
    Ok(())
}

async fn run_local_upgrade(
    cluster: &CraftyCluster<UpgradeMachine>,
    opts: &UpgradeOpts,
) -> Result<(), UpgradeRunError> {
    let members: Vec<NodeId> = cluster.members().to_vec();
    let view = query_upgrade_view(cluster, &members).await?;
    let Some(desired) = view.desired else {
        return Ok(());
    };

    if running_app_version() == desired.app_version {
        propose_upgrade(
            cluster,
            UpgradeCommand::Report {
                node_id: cluster.node_id(),
                phase: UpgradePhase::Ready,
            },
        )
        .await?;
        return Ok(());
    }

    propose_upgrade(
        cluster,
        UpgradeCommand::Report {
            node_id: cluster.node_id(),
            phase: UpgradePhase::Downloading,
        },
    )
    .await?;

    let bytes = fetch_artifact(&desired.url).await?;
    verify_sha256_hex(&bytes, &desired.sha256_hex)?;

    if !opts.dry_run {
        atomic_symlink_install(
            &bytes,
            &opts.install_dir,
            &opts.current_link,
            &desired.app_version,
        )?;
    }

    if opts.dry_run {
        propose_upgrade(
            cluster,
            UpgradeCommand::Report {
                node_id: cluster.node_id(),
                phase: UpgradePhase::Ready,
            },
        )
        .await?;
        return Ok(());
    }

    propose_upgrade(
        cluster,
        UpgradeCommand::Report {
            node_id: cluster.node_id(),
            phase: UpgradePhase::Installed,
        },
    )
    .await?;

    propose_upgrade(
        cluster,
        UpgradeCommand::Report {
            node_id: cluster.node_id(),
            phase: UpgradePhase::Restarting,
        },
    )
    .await?;

    if cluster.members().len() > 1 {
        let _ = cluster.leave().await;
    }
    cluster.shutdown();
    std::process::exit(0);
}
