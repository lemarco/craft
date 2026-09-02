//! Workload governor: ingress signals tune consumers and token ceiling.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use trembita::{ConsumerOpts, JobOpts, TrembitaApp, TrembitaConfigure, WorkloadOpts, consumer};
use trembita_runtime::ManualExternalLoad;
use trembita_test_support::boot_local_app;

static HANDLED: AtomicUsize = AtomicUsize::new(0);

#[consumer("tasks")]
#[allow(clippy::unused_async)]
async fn handle_task(_payload: &[u8]) -> Result<(), ()> {
    HANDLED.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[tokio::test]
async fn workload_governor_tunes_for_connections_and_depth() {
    HANDLED.store(0, Ordering::SeqCst);
    let base = std::env::temp_dir().join(format!(
        "trembita-workload-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let opts = WorkloadOpts::balanced()
        .api_protect_connections(2)
        .max_compute_tokens(4);
    let opportunistic_batch = opts.when_opportunistic.batch;
    let protective_batch = opts.when_protective.batch;

    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .data_dir(&base)
                .workload(WorkloadOpts {
                    tick: Duration::from_millis(20),
                    ..opts
                })
                .jobs([JobOpts::new("tasks")
                    .lease(Duration::from_secs(30))
                    .consumer(&HandleTaskConsumer)])
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    ..TrembitaConfigure::default()
                })
        },
        None,
    )
    .await;

    for _ in 0..200 {
        if app.is_leader().await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(app.is_leader().await);

    let wl = app
        .cluster()
        .workload_runtime()
        .expect("workload runtime should be wired");
    let mut tune = wl.tune();

    let connections = wl.connections();
    let hot_a = connections.track();
    let hot_b = connections.track();
    tokio::time::timeout(Duration::from_secs(2), async {
        while tune.borrow().batch != protective_batch {
            tune.changed().await.unwrap();
        }
    })
    .await
    .expect("governor should publish protective tune under hot ingress");
    drop(hot_a);
    drop(hot_b);

    app.enqueue("tasks", b"work").await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while tune.borrow().batch != opportunistic_batch {
            tune.changed().await.unwrap();
        }
    })
    .await
    .expect("governor should publish opportunistic tune when idle with depth");

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let consumer = app.spawn_consumer(
        HandleTaskConsumer,
        ConsumerOpts::default().idle_sleep(Duration::from_millis(10)),
        stop_rx,
    );
    for _ in 0..50 {
        if HANDLED.load(Ordering::SeqCst) >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    stop_tx.send(true).ok();
    let _ = consumer.await;
    assert!(HANDLED.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn external_load_triggers_protective_tune() {
    let base = std::env::temp_dir().join(format!(
        "trembita-external-load-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let external = Arc::new(ManualExternalLoad::new());
    let opts = WorkloadOpts::balanced()
        .api_protect_connections(4)
        .max_compute_tokens(4)
        .external_load(external.clone());
    let protective_batch = opts.when_protective.batch;

    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .data_dir(&base)
                .workload(WorkloadOpts {
                    tick: Duration::from_millis(20),
                    ..opts
                })
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    ..TrembitaConfigure::default()
                })
        },
        None,
    )
    .await;

    let wl = app
        .cluster()
        .workload_runtime()
        .expect("workload runtime should be wired");
    let mut tune = wl.tune();

    external.set(4);
    tokio::time::timeout(Duration::from_secs(2), async {
        while tune.borrow().batch != protective_batch {
            tune.changed().await.unwrap();
        }
    })
    .await
    .expect("governor should publish protective tune under external load");
}
