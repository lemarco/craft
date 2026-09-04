//! External backlog: leader feeder → consumer → settle back to source.

#![allow(clippy::large_futures)] // boot_local_app future grows with product builder surface

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use trembita::{
    BacklogFeedOpts, ConsumerOpts, ExternalBacklog, JobOpts, TrembitaApp, TrembitaConfigure,
    consumer,
};
use trembita_jobs::{BacklogItem, EnqueueOptions, InMemoryExternalBacklog, Settlement};
use trembita_test_support::boot_local_app;

static PROCESSED: AtomicUsize = AtomicUsize::new(0);

#[consumer("imports")]
#[allow(clippy::unused_async)]
async fn import_row(payload: &[u8]) -> Result<(), ()> {
    assert_eq!(payload, b"row-1");
    PROCESSED.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[tokio::test]
async fn external_backlog_feeds_consumer_and_settles() {
    PROCESSED.store(0, Ordering::SeqCst);
    let backlog = Arc::new(InMemoryExternalBacklog::new());
    backlog.push(BacklogItem {
        key: b"row-1".to_vec(),
        payload: b"row-1".to_vec(),
        priority: 0,
    });

    let base = std::env::temp_dir().join(format!(
        "trembita-ext-backlog-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        || {
            TrembitaApp::builder()
                .data_dir(&base)
                .jobs([JobOpts::new("imports")
                    .lease(Duration::from_millis(500))
                    .batch(1)
                    .idle_sleep(Duration::from_millis(10))
                    .backlog(
                        Arc::clone(&backlog) as Arc<dyn ExternalBacklog>,
                        BacklogFeedOpts::default()
                            .pending_target_per_consumer(1)
                            .poll(Duration::from_millis(20)),
                    )
                    .consumer(&ImportRowConsumer)])
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

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let consumer = app.spawn_consumer(
        ImportRowConsumer,
        ConsumerOpts::default()
            .batch(1)
            .idle_sleep(Duration::from_millis(10)),
        stop_rx,
    );

    for i in 0..100 {
        if PROCESSED.load(Ordering::SeqCst) >= 1 {
            break;
        }
        if i == 99 {
            let metrics = app.job_queue("imports").unwrap().metrics().await;
            let claim_probe = backlog.claim(1).await;
            eprintln!(
                "DEBUG metrics={metrics:?} depth={:?} claim_probe={claim_probe:?} settled={:?}",
                backlog.depth().await,
                backlog.settled()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    stop_tx.send(true).ok();
    let _ = consumer.await;

    assert_eq!(PROCESSED.load(Ordering::SeqCst), 1);
    assert_eq!(backlog.depth().await.unwrap(), 0);
    assert_eq!(
        backlog.settled().get(b"row-1".as_slice()),
        Some(&Settlement::Done { attempts: 0 })
    );

    app.enqueue_opts("imports", b"direct", EnqueueOptions::dedup_key("direct-1"))
        .await
        .unwrap();
}
