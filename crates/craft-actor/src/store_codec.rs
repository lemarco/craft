//! Typed serde helpers at the [`ActorStateStore`] boundary (actor-state-redis, architecture-style).
//!
//! The port stays **opaque bytes** (`get`/`set`/`delete`) so every backend
//! (Redis, in-memory, a fake in tests) implements one trait. Application types
//! cross the boundary through the same encode/decode path as consensus
//! [`Command`](craft_core::Command)s — `craft_proto::encode` / `decode` — without
//! a second trait hierarchy.

use std::time::Duration;

use craft_proto::{decode, encode};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::store::{ActorStateStore, StoreError};

/// Load and decode a value from `key`, or `None` if absent/expired.
///
/// # Errors
/// Returns [`StoreError`] if the backend read fails or bytes cannot be decoded.
pub async fn store_get<T: DeserializeOwned>(
    store: &dyn ActorStateStore,
    key: &str,
) -> Result<Option<T>, StoreError> {
    match store.get(key).await? {
        None => Ok(None),
        Some(bytes) => decode(&bytes)
            .map(Some)
            .map_err(|e| StoreError::Codec(e.to_string())),
    }
}

/// Encode and persist `value` at `key`.
///
/// # Errors
/// Returns [`StoreError`] if encoding or the backend write fails.
pub async fn store_set<T: Serialize>(
    store: &dyn ActorStateStore,
    key: &str,
    value: &T,
    ttl: Option<Duration>,
) -> Result<(), StoreError> {
    let bytes = encode(value).map_err(|e| StoreError::Codec(e.to_string()))?;
    store.set(key, &bytes, ttl).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryStore;

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct OrderState {
        step: u32,
    }

    #[tokio::test]
    async fn typed_round_trip_through_the_store_boundary() {
        let store = InMemoryStore::new();
        store_set(&store, "order:1", &OrderState { step: 2 }, None)
            .await
            .unwrap();
        let loaded: Option<OrderState> = store_get(&store, "order:1").await.unwrap();
        assert_eq!(loaded, Some(OrderState { step: 2 }));
    }

    #[tokio::test]
    async fn corrupt_bytes_surface_as_codec_errors() {
        let store = InMemoryStore::new();
        store.set("bad", b"not-postcard", None).await.unwrap();
        let err = store_get::<OrderState>(&store, "bad").await.unwrap_err();
        assert!(matches!(err, StoreError::Codec(_)));
    }
}
