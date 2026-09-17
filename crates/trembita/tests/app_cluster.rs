//! [`TrembitaAppBuilder`] cluster wiring (leader tasks).

#![allow(clippy::large_futures)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use trembita::{TrembitaApp, TrembitaConfigure};
use trembita_runtime::LeaderLoopOpts;
use trembita_test_facade::{boot_local_app, wait_for_trembita_app_leader};
use trembita_test_support::eventually_default;

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
async fn b41_on_leader_ticks_while_raft_leader() {
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

#[tokio::test]
async fn b41_durable_mailbox_boot_creates_spool_redb() {
    let base = temp_base("mailbox-spool");
    let data_dir = base.clone();

    let app = boot_local_app(
        || {
            TrembitaApp::builder().configure(
                TrembitaConfigure::default()
                    .with_data_dir(data_dir)
                    .with_durable_mailbox(true)
                    .with_tick_period(Duration::from_millis(5)),
            )
        },
        None,
    )
    .await;

    assert!(
        base.join("mailbox-spool.redb").is_file(),
        "durable mailbox should open {}/mailbox-spool.redb",
        base.display()
    );

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}

#[tokio::test]
async fn b41_builder_with_durable_mailbox_matches_configure_flag() {
    let base = temp_base("mailbox-builder");
    let data_dir = base.clone();

    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .configure(
                    TrembitaConfigure::default()
                        .with_data_dir(data_dir)
                        .with_tick_period(Duration::from_millis(5)),
                )
                .with_durable_mailbox(true)
        },
        None,
    )
    .await;

    assert!(base.join("mailbox-spool.redb").is_file());

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
