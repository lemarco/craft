//! Integration tests for [`RedisStore`] against a real Redis via
//! `testcontainers` (ADR 029). Gated `#[ignore]` because they need Docker;
//! the heavy CI lane runs them with `--run-ignored all` (or `-- --ignored`).
//!
//! Run locally with:
//!
//! ```text
//! cargo test -p craft-store-redis -- --ignored
//! ```

use std::time::Duration;

use craft_actor::ActorStateStore;
use craft_store_redis::RedisStore;
use testcontainers_modules::redis::{REDIS_PORT, Redis};
use testcontainers_modules::testcontainers::runners::AsyncRunner;

async fn start_redis() -> (impl Sized, RedisStore) {
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
    let store = RedisStore::connect(&url).await.expect("connect redis");
    (container, store)
}

#[tokio::test]
#[ignore = "requires Docker; run in heavy CI lane"]
async fn set_get_delete_round_trip() {
    let (_c, store) = start_redis().await;

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
    let (_c, store) = start_redis().await;

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
    let (_c, store) = start_redis().await;

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
    let (_c, store) = start_redis().await;
    let orders = store.clone().with_prefix("orders:");
    let users = store.with_prefix("users:");

    orders.set("1", b"order-one", None).await.unwrap();
    users.set("1", b"user-one", None).await.unwrap();

    assert_eq!(orders.get("1").await.unwrap(), Some(b"order-one".to_vec()));
    assert_eq!(users.get("1").await.unwrap(), Some(b"user-one".to_vec()));
}
