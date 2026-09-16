//! Self-update showcase (custom [`UpgradeMachine`] — workspace only).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use trembita::Gateway;
use trembita::ReadyOpts;
use trembita::cluster::{PemSecurity, TrembitaCluster};
use trembita::upgrade::{UpgradeMachine, UpgradeOpts, spawn_upgrade_runtime, upgrade_api};
use trembita_assembly::TrembitaClusterBuilder;

use super::self_update_config as debug;

fn env_flag(key: &str) -> bool {
    matches!(
        std::env::var(key).ok().as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "on")
    )
}

/// Run the upgrade-coordinator showcase until Ctrl-C.
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    debug::init_tracing();
    debug::startup();

    let cfg = debug::config_from_env()?;
    let seeds = cfg.join_seeds.clone();

    let mut builder = TrembitaClusterBuilder::new(cfg.node_id, UpgradeMachine::default())
        .members(cfg.members.iter().copied());
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
        Arc::new(builder.start_quic_pem(pem, cfg.listen, cfg.peers).await?)
    } else {
        Arc::new(
            builder
                .start_quic(cfg.security, cfg.listen, cfg.peers)
                .await?,
        )
    };

    let mut upgrade_opts = UpgradeOpts::under_data_dir(cfg.data_dir.clone().expect("data_dir"));
    upgrade_opts.dry_run = env_flag("TREMBITA_UPGRADE_DRY_RUN");
    upgrade_opts.tick_period = Duration::from_secs(5);
    let _upgrade = spawn_upgrade_runtime(Arc::clone(&cluster), upgrade_opts);

    if let Some(gateway) = cfg.gateway {
        spawn_upgrade_http(Arc::clone(&cluster), gateway).await?;
    }

    cluster.wait_until_ready(ReadyOpts::default()).await;
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
    use hyper::server::conn::http1;
    use hyper_util::rt::TokioIo;
    use hyper_util::service::TowerToHyperService;

    let api = upgrade_api(cluster);
    let gateway = Gateway::new(false).dev_fallback(api.route_table());
    let service = gateway.build_service()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    eprintln!(
        "trembita: upgrade API http://{addr} (GET /cluster/upgrade, POST /cluster/upgrade/desired)"
    );
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let service = service.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let hyper_service = TowerToHyperService::new(service);
                let _ = http1::Builder::new()
                    .serve_connection(io, hyper_service)
                    .with_upgrades()
                    .await;
            });
        }
    });
    Ok(())
}
