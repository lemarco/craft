//! # Background jobs showcase (durable job queue)
//!
//! Every run mode is a QUIC cluster member: solo `cargo run` is a one-node seed
//! (`TREMBITA_ALLOW_JOIN=1`); `./cluster.sh` adds nodes via dynamic join.

mod capabilities;
mod cron_bootstrap;
mod debug;

use std::time::Duration;

use trembita::{
    AppManifest, CapRequest, RouteTable, TrembitaApp, TrembitaConfigure, cap_enqueue,
};
use trembita_tools::showcase_common::{
    data_dir, display_addr, http_bind_display, http_disabled, wire_bind_from_env,
};

use crate::capabilities::email::DeliverEmail;
use crate::capabilities::manifest as capabilities_manifest;

const DATA_DIR_NAME: &str = "trembita-showcase-background-jobs";

fn server_builder() -> Result<trembita::TrembitaAppBuilder, Box<dyn std::error::Error>> {
    let dir = data_dir(DATA_DIR_NAME);
    let _ = std::fs::create_dir_all(&dir);
    Ok(TrembitaApp::from_env()?
        .manifest(AppManifest::new().capabilities(capabilities_manifest()))
        .gateway_routes(|state| {
            RouteTable::new().post(
                "/jobs/emails",
                cap_enqueue::<DeliverEmail>(state),
            )
        })
        .configure(
            TrembitaConfigure::default()
                .with_local_gateway_apis()
                .with_data_dir(dir)
                .with_tick_period(Duration::from_millis(10))
                .with_reconcile_period(Duration::from_millis(20)),
        ))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    debug::init_tracing();
    debug::startup("quic", 0, &data_dir(DATA_DIR_NAME));
    print_banner();
    cron_bootstrap::register_weekly_cron(server_builder()?)
        .run()
        .await?;
    debug::shutdown(1);
    Ok(())
}

fn print_banner() {
    let stream = DeliverEmail::QUEUE_STREAM;
    println!("trembita showcase · background jobs (background jobs)");
    println!("  listen   {} (QUIC + HTTP same port)", wire_bind_from_env("127.0.0.1:8090"));
    if !http_disabled() {
        let gw = http_bind_display("127.0.0.1:8090");
        println!("  http     POST http://{}/jobs/emails  (cap enqueue → `{stream}`)", display_addr(&gw));
        println!("  ops      http://{}/dashboard", display_addr(&gw));
    }
    if std::env::var("TREMBITA_JOIN_SEEDS").is_ok() {
        println!("  join     via TREMBITA_JOIN_SEEDS");
    } else {
        println!("  role     seed (TREMBITA_ALLOW_JOIN when unset)");
    }
    println!("  cluster  ./cluster.sh setup && ./cluster.sh up");
    println!("  trigger  ./trigger.sh <payload>");
    println!("  dedup    ./trigger-idempotent.sh <payload>");
    println!("  data_dir {}", data_dir(DATA_DIR_NAME).display());
    println!("press Ctrl-C to stop");
}
