//! Integration contract for [`ActorStateStore`] backends (in-memory baseline).

use std::time::Duration;

use trembita_actor_store::{ActorStateStore, InMemoryStore, StoreError};

#[tokio::test]
async fn in_memory_store_contract_round_trip() {
    let store = InMemoryStore::new();
    assert_eq!(store.get("missing").await.unwrap(), None);

    store.set("k", b"v", None).await.unwrap();
    assert_eq!(store.get("k").await.unwrap(), Some(b"v".to_vec()));

    store.delete("k").await.unwrap();
    assert_eq!(store.get("k").await.unwrap(), None);
}

#[tokio::test]
async fn in_memory_store_contract_compare_and_set() {
    let store = InMemoryStore::new();

    assert!(
        store
            .compare_and_set("lock", None, b"1", None)
            .await
            .unwrap()
    );
    assert!(
        !store
            .compare_and_set("lock", None, b"2", None)
            .await
            .unwrap()
    );
    assert!(
        store
            .compare_and_set("lock", Some(b"1"), b"2", None)
            .await
            .unwrap()
    );
    assert_eq!(store.get("lock").await.unwrap(), Some(b"2".to_vec()));
}

#[tokio::test]
async fn in_memory_store_contract_ttl_expiry() {
    let store = InMemoryStore::new();
    store
        .set("ephemeral", b"x", Some(Duration::from_millis(50)))
        .await
        .unwrap();
    assert_eq!(store.get("ephemeral").await.unwrap(), Some(b"x".to_vec()));

    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(store.get("ephemeral").await.unwrap(), None);
}

#[tokio::test]
async fn in_memory_store_contract_delete_idempotent() {
    let store = InMemoryStore::new();
    store.delete("absent").await.unwrap();
    store.set("k", b"v", None).await.unwrap();
    store.delete("k").await.unwrap();
    store.delete("k").await.unwrap();
    assert_eq!(store.get("k").await.unwrap(), None);
}

#[tokio::test]
async fn store_error_display() {
    let err = StoreError::Backend("redis down".into());
    assert!(err.to_string().contains("redis down"));
}
