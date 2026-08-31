//! # Stateful workers showcase (messaging **tier B**)
//!
//! Cast body: JSON `{"payload":"<order-id>"}`.
//!
//! ## Subcommands
//!
//! | Command | Purpose |
//! |---------|---------|
//! | *(default)* | Run HTTP server (local or cluster) |
//! | `cast N` | Dev client — POST order id `N` |
//! | `migrate-demo` | 2-node LocalNetwork migration walkthrough |
//!
//! QUIC migration: `CRAFTY_MIGRATE_DEMO=1` + `./cluster.sh 1-migrate|2-migrate`, then `./cluster.sh migrate-run`.

mod debug;
mod migrate_counter;
mod migrate_demo;
mod migrate_http;
mod processor;

use std::env;
use std::sync::Arc;
use std::time::Duration;

use crafty::net::LocalNetwork;
use crafty::{CraftyApp, CraftyAppBuilder, NodeId, ReadyOpts, app_config_from_env};
use crafty_showcase_common::{cluster_mode, data_dir, display_addr, env_flag};

use crate::migrate_counter::StatefulCounter;
use crate::processor::{OrderProcessor, ProcessorCfg};

const DATA_DIR_NAME: &str = "crafty-showcase-stateful-workers";

fn migrate_demo_mode() -> bool {
    env_flag("CRAFTY_MIGRATE_DEMO")
}

fn processor_cfg() -> ProcessorCfg {
    ProcessorCfg {
        data_dir: data_dir(DATA_DIR_NAME),
    }
}

fn apply_actors(builder: CraftyAppBuilder) -> CraftyAppBuilder {
    if migrate_demo_mode() {
        builder.manage::<StatefulCounter>("counter", 1, 0)
    } else {
        builder.manage::<OrderProcessor>("orders", 1, processor_cfg())
    }
}

fn apply_routes(builder: CraftyAppBuilder) -> CraftyAppBuilder {
    if migrate_demo_mode() {
        builder.http_routes(|app| migrate_http::migrate_routes(app))
    } else {
        builder
    }
}

async fn start_local() -> Result<Arc<CraftyApp>, Box<dyn std::error::Error>> {
    let dir = data_dir(DATA_DIR_NAME);
    std::fs::create_dir_all(&dir)?;
    let gateway: std::net::SocketAddr = env::var("CRAFTY_GATEWAY")
        .unwrap_or_else(|_| "127.0.0.1:8190".into())
        .parse()?;
    let net = LocalNetwork::new();
    Ok(
        apply_routes(apply_actors(
            CraftyApp::builder(NodeId(1))
                .data_dir(&dir)
                .members([NodeId(1)])
                .tick_period(Duration::from_millis(10)),
        ))
        .admin_addr("127.0.0.1:9280".parse()?)
        .gateway_addr(gateway)
        .gateway_jobs_api(false)
        .gateway_actors_api(!migrate_demo_mode())
        .start_local_shared(&net)
        .await,
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    debug::init_tracing();
    let mode = env::args().nth(1).unwrap_or_default();

    match mode.as_str() {
        "migrate-demo" => return migrate_demo::run_local().await,
        "cast" => {
            let order: u64 = env::args().nth(2).unwrap_or_else(|| "1001".into()).parse()?;
            return cast_order(order).await;
        }
        "" if cluster_mode() => { /* QUIC server below */ }
        "" => {
            let app = start_local().await?;
            debug::startup("local", app.cluster().node_id().0, &data_dir(DATA_DIR_NAME));
            print_banner(false).await;
            tokio::signal::ctrl_c().await?;
            debug::shutdown();
            app.cluster().shutdown();
            return Ok(());
        }
        other => return Err(format!("unknown mode {other:?}").into()),
    }

    let cfg = app_config_from_env().map_err(|e| format!("config: {e}"))?;
    let app = apply_routes(apply_actors(
        CraftyAppBuilder::from_config(&cfg)
            .tick_period(Duration::from_millis(10))
            .reconcile_period(Duration::from_millis(20)),
    ))
    .start_quic_shared(cfg.security, cfg.listen, cfg.peers)
    .await
    .map_err(|e| format!("start: {e}"))?;

    if !app.wait_until_ready(ReadyOpts::default()).await {
        tracing::warn!(target: "showcase", showcase = debug::NAME, "cluster not ready after 60s");
        eprintln!("warn: no leader yet — start nodes 2+3, then retry");
    } else {
        debug::cluster_ready();
    }

    debug::startup("quic", app.cluster().node_id().0, &data_dir(DATA_DIR_NAME));
    print_banner(true).await;
    tokio::signal::ctrl_c().await?;
    debug::shutdown();
    app.cluster().shutdown();
    Ok(())
}

async fn print_banner(cluster: bool) {
    let node_id = env::var("CRAFTY_NODE_ID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    println!("crafty showcase · stateful workers (tier B)");
    if migrate_demo_mode() {
        println!("  mode     migration demo (counter actor)");
        if cluster {
            println!("  migrate  ./cluster.sh migrate-run  (POST /demo/migrate/run)");
        }
    } else if cluster {
        println!("  mode     QUIC cluster (node {node_id})");
        println!("  listen   {}", env::var("CRAFTY_LISTEN").unwrap_or_default());
        if env::var("CRAFTY_GATEWAY").is_ok_and(|g| g != "-") {
            let gw = env::var("CRAFTY_GATEWAY").unwrap_or_default();
            println!(
                "  gateway  http://{}/actors/orders/cast",
                display_addr(&gw)
            );
        }
        if let Ok(admin) = env::var("CRAFTY_ADMIN") {
            if admin != "-" {
                println!("  admin    http://{}/dashboard", display_addr(&admin));
            }
        }
        println!("  actor    orders (idempotent store)");
    } else {
        let gateway: std::net::SocketAddr = env::var("CRAFTY_GATEWAY")
            .unwrap_or_else(|_| "127.0.0.1:8190".into())
            .parse()
            .expect("gateway");
        println!("  mode     local (single process)");
        println!("  gateway  http://{gateway}/actors/orders/cast");
        println!("  admin    http://127.0.0.1:9280/dashboard");
        println!("  cluster  ./cluster.sh setup && ./cluster.sh up");
    }
    if !migrate_demo_mode() {
        println!("  trigger  ./trigger.sh <order-id>");
    }
    println!("  debug    RUST_LOG=showcase=debug");
    println!("  migrate  cargo run --release -- migrate-demo");
    println!("  data_dir {}", data_dir(DATA_DIR_NAME).display());
    println!("press Ctrl-C to stop");
}

async fn cast_order(order_id: u64) -> Result<(), Box<dyn std::error::Error>> {
    let gateway = env::var("CRAFTY_GATEWAY").unwrap_or_else(|_| "127.0.0.1:8190".into());
    debug::order_cast(order_id, &gateway);
    let resp = crafty_showcase_client::cast_actor(&gateway, "orders", &order_id.to_string()).await?;
    if resp.is_success() {
        println!("cast order {order_id} → HTTP {}", resp.status);
        Ok(())
    } else {
        Err(format!("unexpected HTTP {}:\n{}", resp.status, String::from_utf8_lossy(resp.body())).into())
    }
}
