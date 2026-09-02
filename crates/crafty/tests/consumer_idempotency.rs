//! B-13 acceptance: a redelivered job runs its side effect exactly once.
//!
//! The queue is at-least-once by design, so this exercises the guard, not the
//! queue: the handler fails *after* its side effect, forcing a redelivery, and
//! the second delivery must be skipped.
//!
//! The two cases use separate consumers, streams, and counters so they stay
//! correct under `cargo test`'s thread-parallel runner as well as nextest's
//! process-per-test isolation.
//!
//! These run on the real clock, not `start_paused`: retry backoff is computed
//! from `SystemTime`, so advancing tokio's clock alone never makes a nacked job
//! eligible again. First retry lands ~1s after the nack (`1000ms * attempts`).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crafty::{
    ConsumerOpts, CraftyApp, CraftyConfigure, IdempotencyOpts, JobContext, QueueOpts, consumer,
};
use crafty_actor::{ActorStateStore, EnqueueOptions, InMemoryStore};
use crafty_test_support::boot_local_app;

static GUARDED_SIDE_EFFECTS: AtomicUsize = AtomicUsize::new(0);
static GUARDED_DELIVERIES: AtomicUsize = AtomicUsize::new(0);
static PLAIN_SIDE_EFFECTS: AtomicUsize = AtomicUsize::new(0);

/// Charges once, then crashes before the ack — the classic redelivery window.
#[consumer("guarded")]
#[allow(clippy::unused_async)]
async fn charge_guarded(payload: &[u8], ctx: JobContext<'_>) -> Result<(), ()> {
    GUARDED_DELIVERIES.fetch_add(1, Ordering::SeqCst);
    assert_eq!(payload, b"charge");
    GUARDED_SIDE_EFFECTS.fetch_add(1, Ordering::SeqCst);
    if ctx.attempts == 1 {
        return Err(());
    }
    Ok(())
}

/// Same shape, no guard — the control case.
#[consumer("plain")]
#[allow(clippy::unused_async)]
async fn charge_plain(payload: &[u8], ctx: JobContext<'_>) -> Result<(), ()> {
    assert_eq!(payload, b"charge");
    PLAIN_SIDE_EFFECTS.fetch_add(1, Ordering::SeqCst);
    if ctx.attempts == 1 {
        return Err(());
    }
    Ok(())
}

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let base = std::env::temp_dir().join(format!(
        "crafty-{tag}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    base
}

async fn boot(stream: &'static str, tag: &str) -> Arc<CraftyApp> {
    let base = temp_dir(tag);
    let app = boot_local_app(
        || {
            CraftyApp::builder()
                .data_dir(&base)
                .queue([QueueOpts::new(stream, Duration::from_millis(200))])
                .configure(CraftyConfigure {
                    tick_period: Duration::from_millis(5),
                    ..CraftyConfigure::default()
                })
        },
        None,
    )
    .await;
    // `wait_for_crafty_app_leader` drives tokio's frozen clock; these tests run on
    // the real one, so poll with real sleeps instead.
    for _ in 0..500 {
        if app.is_leader().await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(app.is_leader().await, "app failed to elect a leader");
    tokio::time::sleep(Duration::from_millis(200)).await;
    app
}

#[tokio::test]
async fn redelivery_runs_side_effect_once() {
    let app = boot("guarded", "idem-once").await;

    let store: Arc<dyn ActorStateStore> = Arc::new(InMemoryStore::new());
    let opts = ConsumerOpts::default()
        .batch(1)
        .idle_sleep(Duration::from_millis(10))
        .idempotency(IdempotencyOpts::by_dedup_key(
            Arc::clone(&store),
            "idem:guarded:",
        ));

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let consumer = app.spawn_consumer(ChargeGuardedConsumer, opts, stop_rx);

    app.enqueue_opts(
        "guarded",
        b"charge",
        EnqueueOptions::dedup_key("order-4711"),
    )
    .await
    .expect("enqueue");

    // Long enough for the nack, the ~1s retry backoff, and the redelivery.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let _ = stop_tx.send(true);
    let _ = consumer.await;

    assert_eq!(
        GUARDED_SIDE_EFFECTS.load(Ordering::SeqCst),
        1,
        "side effect must run exactly once across redeliveries (deliveries reaching the handler: {})",
        GUARDED_DELIVERIES.load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn unguarded_consumer_reruns_the_side_effect() {
    let app = boot("plain", "idem-off").await;

    let opts = ConsumerOpts::default()
        .batch(1)
        .idle_sleep(Duration::from_millis(10));
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let consumer = app.spawn_consumer(ChargePlainConsumer, opts, stop_rx);

    app.enqueue_opts("plain", b"charge", EnqueueOptions::dedup_key("order-4711"))
        .await
        .expect("enqueue");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let _ = stop_tx.send(true);
    let _ = consumer.await;

    // Control: without the guard the redelivery repeats the side effect. This is
    // what makes the assertion above meaningful.
    assert!(
        PLAIN_SIDE_EFFECTS.load(Ordering::SeqCst) >= 2,
        "expected the redelivery to repeat the side effect, got {}",
        PLAIN_SIDE_EFFECTS.load(Ordering::SeqCst)
    );
}
