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
use std::time::Duration;

use crafty::{
    ActorGroupOpts, CraftyApp, CraftyAppBuilder, CraftyConfigure, GatewayOpts, ReadyOpts, RunOpts,
};
use crafty_showcase_common::{data_dir, display_addr, env_flag};

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
        builder.actors::<StatefulCounter>("counter", ActorGroupOpts::fixed(0, 1))
    } else {
        builder.actors::<OrderProcessor>("orders", ActorGroupOpts::fixed(processor_cfg(), 1))
    }
}

fn gateway_opts() -> GatewayOpts {
    if migrate_demo_mode() {
        GatewayOpts::default().routes(|app| migrate_http::migrate_routes(app))
    } else {
        GatewayOpts::default().with_actors_api(true)
    }
}

fn server_builder() -> CraftyAppBuilder {
    let dir = data_dir(DATA_DIR_NAME);
    let _ = std::fs::create_dir_all(&dir);
    let gateway: std::net::SocketAddr = env::var("CRAFTY_GATEWAY")
        .unwrap_or_else(|_| "127.0.0.1:8190".into())
        .parse()
        .expect("gateway");
    apply_actors(
        CraftyApp::builder()
            .data_dir(dir)
            .configure(CraftyConfigure {
                tick_period: Duration::from_millis(10),
                reconcile_period: Duration::from_millis(20),
                admin_addr: Some("127.0.0.1:9280".parse().expect("admin")),
                ..CraftyConfigure::default()
            }),
    )
    .gateway(gateway, gateway_opts())
}

async fn run_server() -> Result<(), Box<dyn std::error::Error>> {
    debug::startup("quic", 0, &data_dir(DATA_DIR_NAME));
    print_banner();
    server_builder()
        .run(RunOpts::default().with_wait_ready(ReadyOpts::default()))
        .await?;
    debug::shutdown();
    Ok(())
}

async fn main() -> Result<(), Box<dyn std::error::Error>> {
    debug::init_tracing();
    let mode = env::args().nth(1).unwrap_or_default();

    match mode.as_str() {
        "migrate-demo" => migrate_demo::run_local().await,
        "cast" => {
            let order: u64 = env::args().nth(2).unwrap_or_else(|| "1001".into()).parse()?;
            cast_order(order).await
        }
        "" => run_server().await,
        other => Err(format!("unknown mode {other:?}").into()),
    }
}

fn print_banner() {
    println!("crafty showcase · stateful workers (tier B)");
    if migrate_demo_mode() {
        println!("  mode     migration demo (counter actor)");
        println!("  migrate  ./cluster.sh migrate-run  (POST /demo/migrate/run)");
    } else {
        println!("  listen   {}", env::var("CRAFTY_LISTEN").unwrap_or_else(|_| "0.0.0.0:7443".into()));
        if env::var("CRAFTY_GATEWAY").is_ok_and(|g| g != "-") {
            let gw = env::var("CRAFTY_GATEWAY").unwrap_or_else(|_| "127.0.0.1:8190".into());
            println!(
                "  gateway  http://{}/actors/orders/cast",
                display_addr(&gw)
            );
        }
        if env::var("CRAFTY_JOIN_SEEDS").is_ok() {
            println!("  join     via CRAFTY_JOIN_SEEDS");
        } else {
            println!("  role     seed");
        }
        println!("  actor    orders (idempotent store)");
    }
    if !migrate_demo_mode() {
        println!("  trigger  ./trigger.sh <order-id>");
    }
    println!("  cluster  ./cluster.sh setup && ./cluster.sh up");
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
        Err(format!(
            "unexpected HTTP {}:\n{}",
            resp.status,
            String::from_utf8_lossy(resp.body())
        )
        .into())
    }
}
