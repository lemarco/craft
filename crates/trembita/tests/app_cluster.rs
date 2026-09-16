//! [`TrembitaAppBuilder`] cluster wiring (leader tasks).

#![allow(clippy::large_futures)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use trembita::{TrembitaApp, TrembitaConfigure};
use trembita_runtime::LeaderLoopOpts;
use trembita_test_support::{boot_local_app, eventually_default, wait_for_trembita_app_leader};

fn temp_base(label: &str) -> PathBuf {
    let base = std::env::temp_dir().join(format!(
        "trembita-app-cluster-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).expect("tempdir");
    base
}

#[tokio::test(start_paused = true)]
async fn trembita_app_on_leader_runs_on_product_builder() {
    let base = temp_base("on-leader");
    let data_dir = base.clone();
    let ticks = Arc::new(AtomicUsize::new(0));
    let ticks_in_task = Arc::clone(&ticks);

    let app = boot_local_app(
        move || {
            TrembitaApp::builder()
                .configure(
                    TrembitaConfigure::default()
                        .with_local_gateway_apis()
                        .with_data_dir(data_dir)
                        .with_tick_period(Duration::from_millis(5)),
                )
                .on_leader(
                    LeaderLoopOpts::new(Duration::from_millis(10)).run_on_acquire(),
                    move |_| {
                        let ticks = Arc::clone(&ticks_in_task);
                        async move {
                            ticks.fetch_add(1, Ordering::SeqCst);
                        }
                    },
                )
        },
        None,
    )
    .await;

    wait_for_trembita_app_leader(&app).await;
    eventually_default("on_leader ticks", || ticks.load(Ordering::SeqCst) >= 2).await;

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
