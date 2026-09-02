//! # Self-update showcase
//!
//! Demonstrates the [upgrade-coordinator](https://gitlab.com/lemarco/trembita/-/blob/main/docs/decisions/upgrade-coordinator.md)
//! pattern: Raft leader grants one node at a time; each node downloads an artifact,
//! verifies SHA-256, installs under `data_dir/bin/`, and (unless `TREMBITA_UPGRADE_DRY_RUN=1`)
//! calls `leave()` and exits for systemd restart.
//!
//! ## HTTP
//!
//! - `GET  /cluster/upgrade` — rolling status JSON
//! - `POST /cluster/upgrade/desired` — start rolling (`202 Accepted`)
//!
//! ## Local cluster
//!
//! ```text
//! ./cluster.sh setup && ./cluster.sh up
//! ./trigger-upgrade.sh
//! ```

mod debug;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use trembita::cluster::{TrembitaCluster, PemSecurity};
use trembita::upgrade::{UpgradeMachine, UpgradeOpts, spawn_upgrade_runtime, upgrade_api};
use trembita::ReadyOpts;
use trembita_showcase_common::{data_dir, display_addr, env_flag};

const DATA_DIR_NAME: &str = "trembita-showcase-self-update";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    debug::init_tracing();
    debug::startup();

    let cfg = debug::config_from_env()?;
    let seeds = cfg.join_seeds.clone();

    let mut builder = TrembitaCluster::builder(cfg.node_id, UpgradeMachine::default())
        .members(cfg.members.iter().copied());
    if let Some(admin) = cfg.admin {
        builder = builder.admin_addr(admin);
    }
    if cfg.allow_join {
        builder = builder.allow_join(true);
    }
    if cfg.allow_leave {
        builder = builder.allow_leave(true);
    }
    if let Some(data_dir) = &cfg.data_dir {
        builder = builder.data_dir(data_dir);
    }
    if !seeds.is_empty() {
        builder = builder.join_seeds(seeds);
    }

    let cluster = if let Some(paths) = cfg.pem_paths {
        let pem = PemSecurity {
            security: cfg.security,
            paths,
        };
        Arc::new(
            builder
                .start_quic_pem(pem, cfg.listen, cfg.peers)
                .await?,
        )
    } else {
        Arc::new(builder.start_quic(cfg.security, cfg.listen, cfg.peers).await?)
    };

    let mut upgrade_opts = UpgradeOpts::under_data_dir(cfg.data_dir.as_ref().map_or_else(
        || data_dir(DATA_DIR_NAME),
        Clone::clone,
    ));
    upgrade_opts.dry_run = env_flag("TREMBITA_UPGRADE_DRY_RUN");
    upgrade_opts.tick_period = Duration::from_secs(5);
    let _upgrade = spawn_upgrade_runtime(Arc::clone(&cluster), upgrade_opts);

    if let Some(gateway) = cfg.gateway {
        spawn_upgrade_http(Arc::clone(&cluster), gateway).await?;
    }

    cluster
        .wait_until_ready(ReadyOpts::default())
        .await;
    debug::ready(&cluster);

    tokio::signal::ctrl_c().await?;
    debug::shutdown();
    if cfg.graceful_leave && cluster.members().len() > 1 {
        let _ = cluster.leave().await;
    }
    cluster.shutdown();
    Ok(())
}

async fn spawn_upgrade_http(
    cluster: Arc<TrembitaCluster<UpgradeMachine>>,
    addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let api = upgrade_api(cluster);
    let router = api.router().with_state(Arc::new(api.into_state()));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!(
        "trembita: upgrade API http://{} (GET /cluster/upgrade, POST /cluster/upgrade/desired)",
        display_addr(&addr.to_string())
    );
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            eprintln!("trembita: upgrade API failed: {e}");
        }
    });
    Ok(())
}
