//! Durable [`ActorStateStore`](super::store::ActorStateStore) backed by `redb`
//! ([actor-state-store](../../../docs/decisions/actor-state-store.md)).

use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crafty_proto::{StoreReplicateOp, decode, encode};
use redb::{Database, ReadableDatabase, TableDefinition};

use super::store::{ActorStateStore, BoxFuture, StoreError};

const KV: TableDefinition<&str, &[u8]> = TableDefinition::new("actor_store_kv");

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct StoredValue {
    value: Vec<u8>,
    #[serde(default)]
    expires_at_ms: u64,
}

/// Replication batch produced by a leader mutation.
pub type StoreReplicationOps = Vec<StoreReplicateOp>;

fn backend(e: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(e.to_string())
}

fn codec(e: impl std::fmt::Display) -> StoreError {
    StoreError::Codec(e.to_string())
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn ttl_to_expires_at_ms(ttl: Option<Duration>) -> u64 {
    ttl.map_or(0, |d| {
        now_ms().saturating_add(u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    })
}

/// Crash-safe actor workflow store in `{data_dir}/actor-store.redb`.
#[derive(Debug)]
pub struct RedbActorStateStore {
    db: Mutex<Database>,
}

impl RedbActorStateStore {
    /// Open or create the store database at `path`.
    ///
    /// # Errors
    /// Returns [`StoreError::Backend`] when the file cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let db = Mutex::new(Database::create(path).map_err(backend)?);
        let store = Self { db };
        store.bootstrap()?;
        Ok(store)
    }

    fn bootstrap(&self) -> Result<(), StoreError> {
        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_write().map_err(backend)?;
        {
            txn.open_table(KV).map_err(backend)?;
        }
        txn.commit().map_err(backend)?;
        Ok(())
    }

    fn read_value(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let now = now_ms();
        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_read().map_err(backend)?;
        let table = txn.open_table(KV).map_err(backend)?;
        let Some(bytes) = table.get(key).map_err(backend)? else {
            return Ok(None);
        };
        let stored: StoredValue = decode(bytes.value()).map_err(codec)?;
        if stored.expires_at_ms != 0 && stored.expires_at_ms <= now {
            drop(txn);
            self.delete_expired(key)?;
            return Ok(None);
        }
        Ok(Some(stored.value))
    }

    fn delete_expired(&self, key: &str) -> Result<(), StoreError> {
        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_write().map_err(backend)?;
        {
            let mut table = txn.open_table(KV).map_err(backend)?;
            let _ = table.remove(key).map_err(backend)?;
        }
        txn.commit().map_err(backend)?;
        Ok(())
    }

    fn apply_set(&self, key: &str, value: &[u8], expires_at_ms: u64) -> Result<(), StoreError> {
        let stored = StoredValue {
            value: value.to_vec(),
            expires_at_ms,
        };
        let bytes = encode(&stored).map_err(codec)?;
        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_write().map_err(backend)?;
        {
            let mut table = txn.open_table(KV).map_err(backend)?;
            table.insert(key, bytes.as_slice()).map_err(backend)?;
        }
        txn.commit().map_err(backend)?;
        Ok(())
    }

    fn apply_delete(&self, key: &str) -> Result<(), StoreError> {
        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_write().map_err(backend)?;
        {
            let mut table = txn.open_table(KV).map_err(backend)?;
            let _ = table.remove(key).map_err(backend)?;
        }
        txn.commit().map_err(backend)?;
        Ok(())
    }

    /// Apply an idempotent replicated mutation from the store leader.
    ///
    /// # Errors
    /// Returns [`StoreError::Backend`] when the redb transaction fails.
    pub fn apply_replicate(&self, op: &StoreReplicateOp) -> Result<(), StoreError> {
        match op {
            StoreReplicateOp::Set {
                key,
                value,
                expires_at_ms,
            } => self.apply_set(key, value, *expires_at_ms),
            StoreReplicateOp::Delete { key } => self.apply_delete(key),
        }
    }

    /// Like [`ActorStateStore::set`] but returns wire replication ops for followers.
    ///
    /// # Errors
    /// Returns [`StoreError::Backend`] when the redb transaction fails.
    pub fn set_replicated(
        &self,
        key: &str,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<StoreReplicationOps, StoreError> {
        let expires_at_ms = ttl_to_expires_at_ms(ttl);
        self.apply_set(key, value, expires_at_ms)?;
        Ok(vec![StoreReplicateOp::Set {
            key: key.to_string(),
            value: value.to_vec(),
            expires_at_ms,
        }])
    }

    /// Like [`ActorStateStore::delete`] but returns wire replication ops for followers.
    ///
    /// # Errors
    /// Returns [`StoreError::Backend`] when the redb transaction fails.
    pub fn delete_replicated(&self, key: &str) -> Result<StoreReplicationOps, StoreError> {
        self.apply_delete(key)?;
        Ok(vec![StoreReplicateOp::Delete {
            key: key.to_string(),
        }])
    }

    /// Like [`ActorStateStore::compare_and_set`] but returns `(applied, ops)`.
    ///
    /// # Errors
    /// Returns [`StoreError::Backend`] when the redb transaction fails.
    pub fn compare_and_set_replicated(
        &self,
        key: &str,
        expected: Option<&[u8]>,
        value: &[u8],
        ttl: Option<Duration>,
    ) -> Result<(bool, StoreReplicationOps), StoreError> {
        let current = self.read_value(key)?;
        if current.as_deref() != expected {
            return Ok((false, Vec::new()));
        }
        let ops = self.set_replicated(key, value, ttl)?;
        Ok((true, ops))
    }
}

impl ActorStateStore for RedbActorStateStore {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, StoreError>> {
        Box::pin(async move { self.read_value(key) })
    }

    fn set<'a>(
        &'a self,
        key: &'a str,
        value: &'a [u8],
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move { self.set_replicated(key, value, ttl).map(|_| ()) })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), StoreError>> {
        Box::pin(async move { self.delete_replicated(key).map(|_| ()) })
    }

    fn compare_and_set<'a>(
        &'a self,
        key: &'a str,
        expected: Option<&'a [u8]>,
        value: &'a [u8],
        ttl: Option<Duration>,
    ) -> BoxFuture<'a, Result<bool, StoreError>> {
        Box::pin(async move {
            self.compare_and_set_replicated(key, expected, value, ttl)
                .map(|(applied, _)| applied)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn redb_store_round_trip_and_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("actor-store.redb");
        {
            let store = RedbActorStateStore::open(&path).expect("open");
            store.set("k", b"v", None).await.expect("set");
            assert_eq!(store.get("k").await.unwrap(), Some(b"v".to_vec()));
        }
        let store = RedbActorStateStore::open(&path).expect("reopen");
        assert_eq!(store.get("k").await.unwrap(), Some(b"v".to_vec()));
    }

    #[tokio::test]
    async fn apply_replicate_matches_local_mutations() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = RedbActorStateStore::open(dir.path().join("actor-store.redb")).expect("open");
        let ops = store.set_replicated("a", b"1", None).expect("set");
        for op in &ops {
            store.apply_replicate(op).expect("apply");
        }
        assert_eq!(store.get("a").await.unwrap(), Some(b"1".to_vec()));
        let ops = store.delete_replicated("a").expect("delete");
        for op in &ops {
            store.apply_replicate(op).expect("apply");
        }
        assert_eq!(store.get("a").await.unwrap(), None);
    }

    #[tokio::test]
    async fn compare_and_set_replicated() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = RedbActorStateStore::open(dir.path().join("actor-store.redb")).expect("open");
        let (applied, _) = store
            .compare_and_set_replicated("k", None, b"v", None)
            .expect("cas");
        assert!(applied);
        let (applied, _) = store
            .compare_and_set_replicated("k", None, b"w", None)
            .expect("cas");
        assert!(!applied);
    }
}
