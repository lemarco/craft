//! # Workflows showcase — saga coordination machinery (not embedded DB)

mod capabilities;
mod debug;
mod onboarding;

use std::time::Duration;

use trembita::{TrembitaApp, TrembitaConfigure, WorkflowOpts};
use trembita_tools::showcase_common::{
    data_dir, display_addr, http_bind_display, http_disabled,
};

use crate::onboarding::{app_manifest, build_plan, run_onboarding_plan};

const DATA_DIR_NAME: &str = "trembita-showcase-workflows";

fn server_builder() -> trembita::TrembitaAppBuilder {
    let dir = data_dir(DATA_DIR_NAME);
    let _ = std::fs::create_dir_all(&dir);
    TrembitaApp::from_env()
        .expect("TREMBITA_LISTEN")
        .data_dir(dir)
        .manifest(app_manifest())
        .workflows([WorkflowOpts::named("onboard", build_plan, run_onboarding_plan)])
        .configure(TrembitaConfigure {
            tick_period: Duration::from_millis(10),
            reconcile_period: Duration::from_millis(20),
            ..TrembitaConfigure::default()
        })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    debug::init_tracing();
    debug::startup("quic", 0, &data_dir(DATA_DIR_NAME));
    print_banner();
    server_builder()
        .run()
        .await?;
    debug::shutdown();
    Ok(())
}

fn print_banner() {
    println!("trembita showcase · workflows (saga journal)");
    if !http_disabled() {
        let gw = http_bind_display("127.0.0.1:8490");
        println!("  http     http://{}/workflows/run", display_addr(&gw));
        println!("  ops      http://{}/dashboard", display_addr(&gw));
    }
    println!("  data_dir {}", data_dir(DATA_DIR_NAME).display());
    println!("press Ctrl-C to stop");
}
