//! Helpers for capability integration tests.

use std::path::Path;
use std::time::Duration;

use trembita::{AppManifest, CapManifest, ReadyOpts, TrembitaApp, TrembitaConfigure};

use crate::facade::boot_local_app_with_consumers;

/// Boot a local app whose manifest includes the given [`CapManifest`] (spawns queue bridges).
///
/// # Panics
/// When boot fails.
#[allow(clippy::large_futures)]
pub async fn boot_local_app_with_capabilities(
    caps: CapManifest,
    data_dir: &Path,
) -> trembita::TestBoot {
    let _ = std::fs::create_dir_all(data_dir);
    boot_local_app_with_consumers(
        || {
            TrembitaApp::builder()
                .configure(TrembitaConfigure {
                    data_dir: Some(data_dir.into()),
                    tick_period: Duration::from_millis(5),
                    reconcile_period: Duration::from_millis(20),
                    directory_publish_period: Duration::from_millis(20),
                    ..TrembitaConfigure::default()
                })
                .manifest(AppManifest::new().capabilities(caps))
        },
        Some(ReadyOpts::default()),
    )
    .await
}

/// Convenience: build manifest wrapper only (caller supplies full `TrembitaAppBuilder` chain).
#[must_use]
pub fn manifest_with_capabilities(caps: CapManifest) -> AppManifest {
    AppManifest::new().capabilities(caps)
}
