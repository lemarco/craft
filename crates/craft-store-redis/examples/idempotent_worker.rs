//! Idempotent stateful-actor worker over an [`ActorStateStore`] (ADR 021 G3).
//!
//! The store is the durable, crash-surviving home for *workflow* state (here,
//! per-order progress): if the VPS running this worker crashes, the leader
//! respawns it elsewhere and it resumes from the same keys (ADR 013/018).
//!
//! This example runs against the in-process [`InMemoryStore`] so it needs no
//! Redis. In production, swap in `craft_store_redis::RedisStore` — the worker
//! code is identical because both implement [`ActorStateStore`]:
//!
//! ```ignore
//! let store: Arc<dyn ActorStateStore> =
//!     Arc::new(RedisStore::connect("redis://127.0.0.1:6379").await?.with_prefix("orders:"));
//! ```
//!
//! Run with: `cargo run -p craft-store-redis --example idempotent_worker`

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use craft_actor::{ActorStateStore, InMemoryStore};

/// Process `order_id` exactly once, even if the message is redelivered (retry,
/// migration, at-least-once cluster delivery). `side_effects` counts how many
/// times the *real* work actually ran, to prove the guard holds.
async fn process_order(
    store: &Arc<dyn ActorStateStore>,
    order_id: u64,
    side_effects: &AtomicU32,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = format!("order:{order_id}");

    // Claim the order: CAS from "absent" to "processing" succeeds for exactly
    // one worker; a redelivery or racing worker sees the key already set.
    let claimed = store
        .compare_and_set(&key, None, b"processing", None)
        .await?;
    if !claimed {
        println!(
            "order {order_id}: already handled (state = {:?}), skipping",
            load(store, &key).await
        );
        return Ok(());
    }

    // --- the real, non-idempotent work happens here exactly once ---
    side_effects.fetch_add(1, Ordering::SeqCst);
    println!("order {order_id}: charging card + shipping…");

    // Mark complete so a crash *after* the work still leaves a durable record.
    store.set(&key, b"done", None).await?;
    Ok(())
}

async fn load(store: &Arc<dyn ActorStateStore>, key: &str) -> String {
    store
        .get(key)
        .await
        .ok()
        .flatten()
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .unwrap_or_else(|| "<absent>".into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn ActorStateStore> = Arc::new(InMemoryStore::new());
    let side_effects = AtomicU32::new(0);

    // The same order is delivered three times (e.g. producer retry + migration
    // replay). The work must run only once.
    for _ in 0..3 {
        process_order(&store, 42, &side_effects).await?;
    }
    // A different order runs its work independently.
    process_order(&store, 43, &side_effects).await?;

    let ran = side_effects.load(Ordering::SeqCst);
    println!("\nreal work executed {ran} time(s) for 2 distinct orders");
    assert_eq!(ran, 2, "idempotency guard must collapse redeliveries");
    println!("idempotency guard held ✓");
    Ok(())
}
