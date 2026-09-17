//! Integration tests for [`PgCapStore`] against real Postgres via
//! `testcontainers`. Gated `#[ignore]` — heavy CI lane runs with `--ignored`.
//!
//! ```text
//! cargo test -p trembita-capstore-postgres --features docker-tests -- --ignored
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use trembita_capstore::{CapStateStore, StoreError};
use trembita_capstore_postgres::PgCapStore;

async fn pg_url() -> (
    testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    String,
) {
    let container = Postgres::default()
        .start()
        .await
        .expect("start postgres container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
    (container, url)
}

async fn connect(url: &str) -> PgCapStore {
    PgCapStore::connect(url, "trembita_capstore_kv_test")
        .await
        .expect("connect")
}

#[tokio::test]
#[ignore = "requires Docker; run in heavy CI lane"]
async fn round_trip_and_delete() {
    let (_c, url) = pg_url().await;
    let store = connect(&url).await;

    assert_eq!(store.get("missing").await.unwrap(), None);
    store.set("k", b"v", None).await.unwrap();
    assert_eq!(store.get("k").await.unwrap(), Some(b"v".to_vec()));
    store.delete("k").await.unwrap();
    assert_eq!(store.get("k").await.unwrap(), None);
}

#[tokio::test]
#[ignore = "requires Docker; run in heavy CI lane"]
async fn compare_and_set_is_atomic_and_binary_safe() {
    let (_c, url) = pg_url().await;
    let store = connect(&url).await;

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
async fn concurrent_none_expectation_claims_once() {
    let (_c, url) = pg_url().await;
    let store = Arc::new(connect(&url).await);
    let wins = Arc::new(AtomicU32::new(0));

    let mut handles = Vec::with_capacity(48);
    for _ in 0..48 {
        let store = Arc::clone(&store);
        let wins = Arc::clone(&wins);
        handles.push(tokio::spawn(async move {
            if store
                .compare_and_set("claim", None, b"1", None)
                .await
                .unwrap()
            {
                wins.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }
    for handle in handles {
        handle.await.expect("task");
    }

    assert_eq!(wins.load(Ordering::SeqCst), 1);
    assert_eq!(store.get("claim").await.unwrap(), Some(b"1".to_vec()));
}

#[tokio::test]
#[ignore = "requires Docker; run in heavy CI lane"]
async fn idempotent_worker_claims_once_per_order() {
    let (_c, url) = pg_url().await;
    let store: Arc<dyn CapStateStore> = Arc::new(connect(&url).await);
    let side_effects = AtomicU32::new(0);

    for _ in 0..3 {
        process_order(&store, 42, &side_effects)
            .await
            .expect("redelivery");
    }
    process_order(&store, 43, &side_effects)
        .await
        .expect("distinct order");

    assert_eq!(side_effects.load(Ordering::SeqCst), 2);
    assert_eq!(store.get("order:42").await.unwrap(), Some(b"done".to_vec()));
}

async fn process_order(
    store: &Arc<dyn CapStateStore>,
    order_id: u64,
    side_effects: &AtomicU32,
) -> Result<(), StoreError> {
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
