//! Integration tests for [`RedisStore`] against a real Redis via
//! `testcontainers` (ADR 029). Gated `#[ignore]` because they need Docker;
//! the heavy CI lane runs them with `--run-ignored all` (or `-- --ignored`).
//!
//! Run locally with:
//!
//! ```text
//! cargo test -p craft-store-redis -- --ignored
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use craft_actor::ActorStateStore;
use craft_store_redis::RedisStore;
use testcontainers_modules::redis::{REDIS_PORT, Redis};
use testcontainers_modules::testcontainers::runners::AsyncRunner;

async fn redis_url() -> (
    testcontainers_modules::testcontainers::ContainerAsync<Redis>,
    String,
) {
    let container = Redis::default()
        .start()
        .await
        .expect("start redis container");
    let host = container.get_host().await.expect("host");
    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .await
        .expect("mapped port");
    let url = format!("redis://{host}:{port}");
    (container, url)
}

async fn connect(url: &str) -> RedisStore {
    RedisStore::connect(url).await.expect("connect redis")
}

/// Idempotent order handler from `examples/idempotent_worker.rs` — shared by
/// the Redis integration tests.
async fn process_order(
    store: &Arc<dyn ActorStateStore>,
    order_id: u64,
    side_effects: &AtomicU32,
) -> Result<(), craft_actor::StoreError> {
    let key = format!("order:{order_id}");
    let claimed = store
        .compare_and_set(&key, None, b"processing", None)
        .await?;
    if !claimed {
        return Ok(());
    }
    side_effects.fetch_add(1, Ordering::SeqCst);
    store.set(&key, b"done", None).await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires Docker; run in heavy CI lane"]
async fn set_get_delete_round_trip() {
    let (_c, url) = redis_url().await;
    let store = connect(&url).await;

    assert_eq!(store.get("k").await.unwrap(), None);
    store.set("k", b"v", None).await.unwrap();
    assert_eq!(store.get("k").await.unwrap(), Some(b"v".to_vec()));
    store.set("k", b"v2", None).await.unwrap();
    assert_eq!(store.get("k").await.unwrap(), Some(b"v2".to_vec()));
    store.delete("k").await.unwrap();
    assert_eq!(store.get("k").await.unwrap(), None);
    // Deleting an absent key is a no-op.
    store.delete("k").await.unwrap();
}

#[tokio::test]
#[ignore = "requires Docker; run in heavy CI lane"]
async fn ttl_expires() {
    let (_c, url) = redis_url().await;
    let store = connect(&url).await;

    store
        .set("k", b"v", Some(Duration::from_millis(100)))
        .await
        .unwrap();
    assert_eq!(store.get("k").await.unwrap(), Some(b"v".to_vec()));
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(store.get("k").await.unwrap(), None);
}

#[tokio::test]
#[ignore = "requires Docker; run in heavy CI lane"]
async fn compare_and_set_is_atomic_and_binary_safe() {
    let (_c, url) = redis_url().await;
    let store = connect(&url).await;

    // Absent key: only a `None` expectation swaps.
    assert!(
        !store
            .compare_and_set("k", Some(b"x"), b"v", None)
            .await
            .unwrap()
    );
    assert!(
        store
            .compare_and_set("k", None, b"\x00\x01\x02", None)
            .await
            .unwrap()
    );
    assert_eq!(store.get("k").await.unwrap(), Some(vec![0, 1, 2]));

    // Present key: `None` expectation fails, wrong value fails, right swaps.
    assert!(!store.compare_and_set("k", None, b"w", None).await.unwrap());
    assert!(
        !store
            .compare_and_set("k", Some(b"nope"), b"w", None)
            .await
            .unwrap()
    );
    assert!(
        store
            .compare_and_set("k", Some(&[0, 1, 2]), b"w", None)
            .await
            .unwrap()
    );
    assert_eq!(store.get("k").await.unwrap(), Some(b"w".to_vec()));

    // CAS honors TTL on the winning branch.
    assert!(
        store
            .compare_and_set("k", Some(b"w"), b"z", Some(Duration::from_millis(100)))
            .await
            .unwrap()
    );
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(store.get("k").await.unwrap(), None);
}

#[tokio::test]
#[ignore = "requires Docker; run in heavy CI lane"]
async fn prefix_namespaces_keys() {
    let (_c, url) = redis_url().await;
    let store = connect(&url).await;
    let orders = store.clone().with_prefix("orders:");
    let users = store.with_prefix("users:");

    orders.set("1", b"order-one", None).await.unwrap();
    users.set("1", b"user-one", None).await.unwrap();

    assert_eq!(orders.get("1").await.unwrap(), Some(b"order-one".to_vec()));
    assert_eq!(users.get("1").await.unwrap(), Some(b"user-one".to_vec()));
}

#[tokio::test]
#[ignore = "requires Docker; run in heavy CI lane"]
async fn two_connections_share_keyspace() {
    let (_c, url) = redis_url().await;
    let writer = connect(&url).await;
    let reader = connect(&url).await;

    writer.set("shared", b"payload", None).await.unwrap();
    assert_eq!(
        reader.get("shared").await.unwrap(),
        Some(b"payload".to_vec())
    );
}

#[tokio::test]
#[ignore = "requires Docker; run in heavy CI lane"]
async fn idempotent_worker_claims_once_per_order() {
    let (_c, url) = redis_url().await;
    let store: Arc<dyn ActorStateStore> = Arc::new(connect(&url).await.with_prefix("orders:"));
    let side_effects = AtomicU32::new(0);

    for _ in 0..3 {
        process_order(&store, 42, &side_effects)
            .await
            .expect("redelivery");
    }
    process_order(&store, 43, &side_effects)
        .await
        .expect("distinct order");

    assert_eq!(
        side_effects.load(Ordering::SeqCst),
        2,
        "CAS guard must collapse redeliveries"
    );
    assert_eq!(store.get("order:42").await.unwrap(), Some(b"done".to_vec()));
}

#[tokio::test]
#[ignore = "requires Docker; run in heavy CI lane"]
async fn connection_manager_recovers_after_brief_redis_pause() {
    let (container, url) = redis_url().await;
    let store = connect(&url).await;
    store.set("k", b"v", None).await.unwrap();

    container.pause().await.expect("pause redis");
    let during_pause = tokio::time::timeout(Duration::from_secs(2), store.get("k")).await;
    assert!(
        during_pause.is_err() || during_pause.unwrap().is_err(),
        "read while Redis is paused should fail or time out"
    );

    container.unpause().await.expect("unpause redis");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        store.get("k").await.expect("get after unpause"),
        Some(b"v".to_vec())
    );
}
