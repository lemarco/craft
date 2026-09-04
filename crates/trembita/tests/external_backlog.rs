//! External backlog: leader feeder → consumer → settle back to source.

#![allow(clippy::large_futures)] // boot_local_app future grows with product builder surface

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use trembita::{
    BacklogFeedOpts, ConsumerOpts, ExternalBacklog, JobOpts, TrembitaApp, TrembitaConfigure,
    consumer,
};
use trembita_jobs::{
    BacklogItem, ConsumerCount, EnqueueOptions, InMemoryExternalBacklog, Settlement,
};
use trembita_test_support::{
    advance, boot_local_app, eventually_async_default, wait_for_trembita_app_leader,
};

static PROCESSED: AtomicUsize = AtomicUsize::new(0);

#[consumer("imports")]
#[allow(clippy::unused_async)]
async fn import_row(payload: &[u8]) -> Result<(), ()> {
    assert_eq!(payload, b"row-1");
    PROCESSED.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[tokio::test(start_paused = true)]
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
                            .poll(Duration::from_millis(20))
                            .consumer_instances(ConsumerCount::Fixed(1)),
                    )])
                .configure(TrembitaConfigure {
                    tick_period: Duration::from_millis(5),
                    ..TrembitaConfigure::default()
                })
        },
        None,
    )
    .await;

    wait_for_trembita_app_leader(&app).await;
    advance(Duration::from_millis(200)).await;

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let consumer = app.spawn_consumer(
        ImportRowConsumer,
        ConsumerOpts::default()
            .batch(1)
            .idle_sleep(Duration::from_millis(10)),
        stop_rx,
    );

    eventually_async_default("external backlog row processed", || async {
        PROCESSED.load(Ordering::SeqCst) >= 1
    })
    .await;

    eventually_async_default("external backlog settled", || async {
        backlog.depth().await.is_ok_and(|d| d == 0)
            && backlog.settled().contains_key(b"row-1".as_slice())
    })
    .await;

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

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
