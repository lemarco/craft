//! Minimal product app for [`e2e/elastic_lb.sh`](../../../../e2e/elastic_lb.sh) (B-34).

mod cap;
mod gateway;

use std::time::Duration;

use trembita::{AppManifest, TrembitaApp, TrembitaConfigure};

pub use cap::capabilities_manifest;

const DATA_DIR_NAME: &str = "trembita-e2e-elastic";

/// Product builder shared by the elastic E2E binary.
pub fn server_builder() -> Result<trembita::TrembitaAppBuilder, Box<dyn std::error::Error>> {
    let dir = crate::showcase_common::data_dir(DATA_DIR_NAME);
    let _ = std::fs::create_dir_all(&dir);
    Ok(TrembitaApp::from_env()?
        .manifest(AppManifest::new().capabilities(capabilities_manifest()))
        .gateway_routes(gateway::route_table)
        .configure(
            TrembitaConfigure::default()
                .with_local_gateway_apis()
                .with_data_dir(dir)
                .with_tick_period(Duration::from_millis(10))
                .with_reconcile_period(Duration::from_millis(20))
                .with_directory_publish_period(Duration::from_millis(20)),
        ))
}
