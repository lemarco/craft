//! # Stateful workers showcase (capability + migration demo)
//!
//! Default mode registers the **`orders`** capability ([`capabilities::orders`]) with
//! idempotent [`ActorStateStore`] keys (B-23a — per-op store, not RAM snapshot).
//!
//! **`migrate-demo`** / `TREMBITA_MIGRATE_DEMO=1` is an **advanced** `UserActor` RAM
//! migration walkthrough only — not the product migration path.
//!
//! ## Subcommands
//!
//! | Command | Purpose |
//! |---------|---------|
//! | *(default)* | Run HTTP server (local or cluster) |
//! | `cast N` | Dev client — POST order id `N` |
//! | `migrate-demo` | 2-node LocalNetwork migration walkthrough |
//!
//! QUIC migration: `TREMBITA_MIGRATE_DEMO=1` + `./cluster.sh 1-migrate|2-migrate`, then `./cluster.sh migrate-run`.

mod capabilities;
mod debug;
mod gateway_orders;
mod migrate_counter;
mod migrate_demo;
mod migrate_http;

use std::env;
use std::time::Duration;

use trembita::{
    AppManifest, CapGroup, CapManifest, Gateway, GatewayOpts, TrembitaApp, TrembitaAppBuilder,
    TrembitaConfigure, TrembitaGatewayState, WorkerOpts, WorkerScale, workers,
};
use trembita_tools::gateway_auth::ShowcaseGatewayIdentity;
use trembita_tools::showcase_common::{
    data_dir, display_addr, env_flag, http_bind_display, http_bind_from_env, http_disabled,
};

use crate::capabilities::orders::{OrdersState, process_op};
use crate::migrate_counter::StatefulCounter;

const DATA_DIR_NAME: &str = "trembita-showcase-stateful-workers";

fn migrate_demo_mode() -> bool {
    env_flag("TREMBITA_MIGRATE_DEMO")
}

fn apply_capabilities(builder: TrembitaAppBuilder) -> TrembitaAppBuilder {
    if migrate_demo_mode() {
        builder.workers(workers!(
            WorkerOpts::<StatefulCounter>::new("counter")
                .config(0)
                .scale(WorkerScale::Fixed(1)),
        ))
    } else {
        let caps = CapManifest::new().group(
            CapGroup::<OrdersState>::with_state("orders")
                .instances(1)
                .op(process_op()),
        );
        builder.manifest(AppManifest::new().capabilities(caps))
    }
}

fn gateway_surfaces(state: TrembitaGatewayState) -> Gateway {
    Gateway::new(false).dev_fallback(
        gateway_orders::order_routes(state.clone())
            .merge(state.app.ops_api().route_table()),
    )
}

fn gateway_opts(addr: std::net::SocketAddr) -> GatewayOpts {
    let opts = GatewayOpts::new(addr);
    if migrate_demo_mode() {
        opts.surfaces(|state| migrate_http::surfaces(state))
    } else {
        opts.identity(ShowcaseGatewayIdentity::from_env())
            .surfaces(gateway_surfaces)
    }
}

fn server_builder() -> TrembitaAppBuilder {
    let dir = data_dir(DATA_DIR_NAME);
    let _ = std::fs::create_dir_all(&dir);
    let gateway = http_bind_from_env("127.0.0.1:8190");
    apply_capabilities(
        TrembitaApp::builder()
            .data_dir(dir)
            .configure(TrembitaConfigure {
                tick_period: Duration::from_millis(10),
                reconcile_period: Duration::from_millis(20),
                ..TrembitaConfigure::default()
            }),
    )
    .gateway(gateway_opts(gateway))
}

async fn run_server() -> Result<(), Box<dyn std::error::Error>> {
    debug::startup("quic", 0, &data_dir(DATA_DIR_NAME));
    print_banner();
    server_builder()
        .run()
        .await?;
    debug::shutdown();
    Ok(())
}

#[tokio::main]
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
    println!("trembita showcase · stateful workers (capabilities + migration demo)");
    if migrate_demo_mode() {
        println!("  mode     advanced UserActor migration (counter, not capabilities)");
        println!("  migrate  ./cluster.sh migrate-run  (POST /demo/migrate/run)");
    } else {
        println!("  listen   {}", env::var("TREMBITA_LISTEN").unwrap_or_else(|_| "0.0.0.0:7443".into()));
        if !http_disabled() {
            let host = display_addr(&http_bind_display("127.0.0.1:8190"));
            println!("  http     http://{host}  (product + ops on one listener)");
            println!("  submit   POST http://{host}/orders/submit?user=tenant-1  (capability fire)");
            println!("  ops      http://{host}/dashboard  /health  /metrics");
        }
        if env::var("TREMBITA_JOIN_SEEDS").is_ok() {
            println!("  join     via TREMBITA_JOIN_SEEDS");
        } else {
            println!("  role     seed");
        }
        println!("  capability orders.process (keyed by order id)");
    }
    if !migrate_demo_mode() {
        println!("  trigger  ./trigger.sh <order-id>");
        println!("  auth     ./trigger-auth.sh tenant-1 <order-id>");
    }
    println!("  cluster  ./cluster.sh setup && ./cluster.sh up");
    println!("  migrate  cargo run --release -- migrate-demo");
    println!("  data_dir {}", data_dir(DATA_DIR_NAME).display());
    println!("press Ctrl-C to stop");
}

async fn cast_order(order_id: u64) -> Result<(), Box<dyn std::error::Error>> {
    let gateway = http_bind_display("127.0.0.1:8190");
    debug::order_cast(order_id, &gateway);
    let url = format!("http://{gateway}/orders/submit?user=dev-cast&token=showcase");
    let client = reqwest::Client::new();
    let resp = client
        .post(url)
        .json(&serde_json::json!({ "order_id": order_id }))
        .send()
        .await?;
    if resp.status().is_success() {
        println!("submit order {order_id} → HTTP {}", resp.status());
        Ok(())
    } else {
        Err(format!(
            "unexpected HTTP {}:\n{}",
            resp.status(),
            resp.text().await.unwrap_or_default()
        )
        .into())
    }
}
