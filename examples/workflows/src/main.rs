//! # Workflows showcase — saga coordination machinery (not embedded DB)

mod debug;
mod onboarding;

use std::env;
use std::time::Duration;

use trembita::{TrembitaApp, TrembitaConfigure, GatewayOpts, ReadyOpts, RunOpts, WorkflowOpts};
use trembita_tools::showcase_common::{data_dir, display_addr};

use crate::onboarding::{apply_workers, build_plan, run_onboarding_plan};

const DATA_DIR_NAME: &str = "trembita-showcase-workflows";

fn server_builder() -> trembita::TrembitaAppBuilder {
    let dir = data_dir(DATA_DIR_NAME);
    let _ = std::fs::create_dir_all(&dir);
    let gateway: std::net::SocketAddr = env::var("TREMBITA_GATEWAY")
        .unwrap_or_else(|_| "127.0.0.1:8490".into())
        .parse()
        .expect("gateway");
    apply_workers(
        TrembitaApp::builder()
            .data_dir(dir)
            .workflows([WorkflowOpts::named("onboard", build_plan, run_onboarding_plan)])
            .configure(TrembitaConfigure {
                tick_period: Duration::from_millis(10),
                reconcile_period: Duration::from_millis(20),
                admin_addr: Some("127.0.0.1:9480".parse().expect("admin")),
                ..TrembitaConfigure::default()
            })
            .gateway(GatewayOpts::new(gateway).with_workflows_api(true)),
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    debug::init_tracing();
    debug::startup("quic", 0, &data_dir(DATA_DIR_NAME));
    print_banner();
    server_builder()
        .run(RunOpts::default().with_wait_ready(ReadyOpts::default()))
        .await?;
    debug::shutdown();
    Ok(())
}

fn print_banner() {
    println!("trembita showcase · workflows (coordination saga)");
    println!("  listen   {}", env::var("TREMBITA_LISTEN").unwrap_or_else(|_| "0.0.0.0:7443".into()));
    if env::var("TREMBITA_GATEWAY").is_ok_and(|g| g != "-") {
        let gw = env::var("TREMBITA_GATEWAY").unwrap_or_else(|_| "127.0.0.1:8490".into());
        println!("  gateway  http://{}/workflows/run", display_addr(&gw));
    }
    if let Ok(admin) = env::var("TREMBITA_ADMIN") {
        if admin != "-" {
            println!("  admin    http://{}/dashboard", display_addr(&admin));
        }
    }
    if env::var("TREMBITA_JOIN_SEEDS").is_ok() {
        println!("  join     via TREMBITA_JOIN_SEEDS");
    } else {
        println!("  role     seed");
    }
    println!("  cluster  ./cluster.sh setup && ./cluster.sh up");
    println!("  trigger  ./trigger.sh [saga-id]");
    println!("  data_dir {}", data_dir(DATA_DIR_NAME).display());
    println!("press Ctrl-C to stop");
}
