//! [`CraftyApp::spawn_consumer`] and `#[consumer]` macro integration.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crafty::{ConsumerOpts, CraftyApp, CraftyConfigure, QueueOpts, consumer};
use crafty_test_support::{advance, boot_local_app, wait_for_crafty_app_leader};

static HANDLED: AtomicUsize = AtomicUsize::new(0);

#[consumer("jobs")]
#[allow(clippy::unused_async)] // `#[consumer]` requires an async fn signature.
async fn handle_job(payload: &[u8]) -> Result<(), ()> {
    assert_eq!(payload, b"work");
    HANDLED.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn consumer_macro_spawns_and_processes_job() {
    HANDLED.store(0, Ordering::SeqCst);

    let base = std::env::temp_dir().join(format!(
        "crafty-consumer-macro-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let app = boot_local_app(
        CraftyApp::builder()
            .data_dir(&base)
            .queue([QueueOpts::new("jobs", Duration::from_secs(60))])
            .configure(CraftyConfigure {
                tick_period: Duration::from_millis(5),
                ..CraftyConfigure::default()
            }),
        None,
    )
    .await;

    wait_for_crafty_app_leader(&app).await;
    advance(Duration::from_millis(200)).await;

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let opts = ConsumerOpts {
        batch: 1,
        idle_sleep: Duration::from_millis(10),
        ..ConsumerOpts::default()
    };
    let consumer = app.spawn_consumer(HandleJobConsumer, opts, stop_rx);

    app.enqueue("jobs", b"work").await.expect("enqueue");
    advance(Duration::from_millis(500)).await;

    assert_eq!(HANDLED.load(Ordering::SeqCst), 1);

    stop_tx.send(true).unwrap();
    consumer.await.unwrap();

    app.shutdown();
    let _ = std::fs::remove_dir_all(base);
}
