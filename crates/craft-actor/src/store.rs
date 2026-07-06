//! External state store for stateful actors ([ADR 021](../decisions/021-actor-state-redis.md)).
//!
//! craft splits durable state in two:
//!
//! * **Consensus state** lives in the Raft [`StateMachine`](craft_core::StateMachine)
//!   — linearizable, replicated, the source of truth for balances/config/orders.
//! * **Actor workflow state** (session progress, job steps, idempotency keys,
//!   locks) lives in an *external* store behind [`ActorStateStore`], so it
//!   survives a VPS crash: the leader respawns the worker elsewhere and it
//!   reloads its keys (ADR 013, ADR 018). Keeping this out of the Raft log
//!   avoids log bloat and the wrong abstraction.
//!
//! This module defines the port ([`ActorStateStore`]) plus an in-process
//! [`InMemoryStore`] used for tests and single-node development. Real backends
//! (e.g. `craft-store-redis`) live in their own crates.
//!
//! The trait uses boxed futures rather than `async fn` in trait position so it
//! stays object-safe — actors hold an `Arc<dyn ActorStateStore>` and the
//! concrete backend is chosen at the edge (ADR 010 rationale, matching the
//! transport port).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// A boxed, `Send` future — the return type of the object-safe store trait.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Why a store operation failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The backend (network, server, pool) reported an error.
    #[error("store backend error: {0}")]
    Backend(String),
    /// A value could not be encoded or decoded around the store boundary.
    #[error("store codec error: {0}")]
    Codec(String),
}

/// A key/value store for stateful-actor workflow data (ADR 021).
///
/// Keys are UTF-8 strings (implementations may namespace them); values are
/// opaque bytes. All methods are async and object-safe so an actor can hold an
/// `Arc<dyn ActorStateStore>` regardless of backend.
pub trait ActorStateStore: Send + Sync {
    /// Fetch the value for `key`, or `None` if absent/expired.
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, StoreError>>;

    /// Store `value` under `key`, replacing any existing value. When `ttl` is
    /// `Some`, the key expires after that duration.
    fn set<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Remove `key`. Removing an absent key is not an error.
    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), StoreError>>;

    /// Atomically set `key` to `value` only if its current value equals
    /// `expected` (or the key is absent when `expected` is `None`). Returns
    /// `true` if the swap happened, `false` if the precondition did not hold.
    ///
    /// This is the primitive for optimistic concurrency and single-writer
    /// idempotency guards across distributed workers.
    fn compare_and_set<'a>(
        &'a self,
        key: &'a str,
        expected: Option<&'a [u8]>,
        value: &'a [u8],
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<bool, StoreError>>;
}

struct Entry {
    value: Vec<u8>,
    expires_at: Option<Instant>,
}

impl Entry {
    fn is_expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|deadline| now >= deadline)
    }
}

/// An in-process [`ActorStateStore`] backed by a `HashMap`.
///
/// Intended for tests and single-node development — it is **not** durable
/// across process restarts and does **not** survive a VPS crash, which is the
/// whole point of the external-store pattern. Use `craft-store-redis` (or
/// another backend) in production. TTLs are honored lazily: an expired key is
/// treated as absent on the next access.
#[derive(Default)]
pub struct InMemoryStore {
    map: Mutex<HashMap<String, Entry>>,
}

impl InMemoryStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn read(&self, key: &str) -> Option<Vec<u8>> {
        let now = Instant::now();
        let mut map = self.map.lock().expect("poisoned");
        match map.get(key) {
            Some(entry) if entry.is_expired(now) => {
                map.remove(key);
                None
            }
            Some(entry) => Some(entry.value.clone()),
            None => None,
        }
    }
}

impl ActorStateStore for InMemoryStore {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, StoreError>> {
        Box::pin(async move { Ok(self.read(key)) })
    }

    fn set<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            let entry = Entry {
                value: value.to_vec(),
                expires_at: ttl.map(|d| Instant::now() + d),
            };
            self.map
                .lock()
                .expect("poisoned")
                .insert(key.to_owned(), entry);
            Ok(())
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move {
            self.map.lock().expect("poisoned").remove(key);
            Ok(())
        })
    }

    fn compare_and_set<'a>(
        &'a self,
        key: &'a str,
        expected: Option<&'a [u8]>,
        value: &'a [u8],
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            let now = Instant::now();
            let mut map = self.map.lock().expect("poisoned");
            let current = match map.get(key) {
                Some(entry) if entry.is_expired(now) => {
                    map.remove(key);
                    None
                }
                Some(entry) => Some(entry.value.as_slice()),
                None => None,
            };
            if current != expected {
                return Ok(false);
            }
            map.insert(
                key.to_owned(),
                Entry {
                    value: value.to_vec(),
                    expires_at: ttl.map(|d| now + d),
                },
            );
            Ok(true)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn set_get_delete_round_trip() {
        let store = InMemoryStore::new();
        assert_eq!(store.get("k").await.unwrap(), None);

        store.set("k", b"v", None).await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Some(b"v".to_vec()));

        store.set("k", b"v2", None).await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), Some(b"v2".to_vec()));

        store.delete("k").await.unwrap();
        assert_eq!(store.get("k").await.unwrap(), None);
        // Deleting an absent key is a no-op, not an error.
        store.delete("k").await.unwrap();
    }

    #[tokio::test]
    async fn ttl_expires_lazily() {
        let store = InMemoryStore::new();
        store
            .set("k", b"v", Some(Duration::from_millis(20)))
            .await
            .unwrap();
        assert_eq!(store.get("k").await.unwrap(), Some(b"v".to_vec()));
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(store.get("k").await.unwrap(), None);
    }

    #[tokio::test]
    async fn compare_and_set_enforces_precondition() {
        let store = InMemoryStore::new();

        // Absent → only a `None` expectation succeeds.
        assert!(
            !store
                .compare_and_set("k", Some(b"x"), b"v", None)
                .await
                .unwrap()
        );
        assert!(store.compare_and_set("k", None, b"v", None).await.unwrap());
        assert_eq!(store.get("k").await.unwrap(), Some(b"v".to_vec()));

        // Present → a `None` expectation now fails; the right value swaps.
        assert!(!store.compare_and_set("k", None, b"w", None).await.unwrap());
        assert!(
            !store
                .compare_and_set("k", Some(b"wrong"), b"w", None)
                .await
                .unwrap()
        );
        assert!(
            store
                .compare_and_set("k", Some(b"v"), b"w", None)
                .await
                .unwrap()
        );
        assert_eq!(store.get("k").await.unwrap(), Some(b"w".to_vec()));
    }
}
