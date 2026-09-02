//! Durable outbox for [`ExternalBacklog::settle`](super::external_backlog::ExternalBacklog)
//! — at-least-once delivery after job queue ack/nack/reclaim
//! ([external-backlog](../../../docs/decisions/external-backlog.md)).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use crafty_proto::{decode, encode};
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};

use super::external_backlog::BacklogSettleEvent;

const ENTRIES: TableDefinition<u64, &[u8]> = TableDefinition::new("backlog_settle_entries");
const KEY_INDEX: TableDefinition<&[u8], u64> = TableDefinition::new("backlog_settle_key_index");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("backlog_settle_meta");
const K_NEXT_ID: &str = "next_id";

/// Stable row id in the settle outbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BacklogSettleOutboxId(pub u64);

/// Why a settle outbox read or write failed.
#[derive(Debug, thiserror::Error)]
pub enum BacklogSettleOutboxError {
    /// Disk / redb backend failure.
    #[error("backend: {0}")]
    Backend(String),
    /// Record encode/decode failure.
    #[error("codec: {0}")]
    Codec(String),
}

fn backend(e: impl std::fmt::Display) -> BacklogSettleOutboxError {
    BacklogSettleOutboxError::Backend(e.to_string())
}

fn codec(e: impl std::fmt::Display) -> BacklogSettleOutboxError {
    BacklogSettleOutboxError::Codec(e.to_string())
}

/// Tunables for [`crate::run_backlog_settle_drainer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacklogSettleOutboxOpts {
    /// Leader poll interval.
    pub poll_interval: Duration,
    /// Max pending rows drained per tick.
    pub max_batch: usize,
}

impl Default for BacklogSettleOutboxOpts {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(500),
            max_batch: 64,
        }
    }
}

impl BacklogSettleOutboxOpts {
    /// Drainer poll interval.
    #[must_use]
    pub fn poll(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Max rows processed per leader tick.
    #[must_use]
    pub fn max_batch(mut self, n: usize) -> Self {
        self.max_batch = n.max(1);
        self
    }
}

/// Durable pending [`BacklogSettleEvent`] rows awaiting external `settle`.
pub trait BacklogSettleOutbox: Send + Sync {
    /// Enqueue or replace a pending settle for `(stream, dedup_key)`.
    ///
    /// # Errors
    /// Returns [`BacklogSettleOutboxError`] when the backend write fails.
    fn push(
        &self,
        event: BacklogSettleEvent,
    ) -> Result<BacklogSettleOutboxId, BacklogSettleOutboxError>;

    /// List up to `max` pending rows in insertion order.
    ///
    /// # Errors
    /// Returns [`BacklogSettleOutboxError`] when the backend read fails.
    fn list_pending(
        &self,
        max: usize,
    ) -> Result<Vec<(BacklogSettleOutboxId, BacklogSettleEvent)>, BacklogSettleOutboxError>;

    /// Remove a delivered row.
    ///
    /// # Errors
    /// Returns [`BacklogSettleOutboxError`] when the backend delete fails.
    fn remove(&self, id: BacklogSettleOutboxId) -> Result<(), BacklogSettleOutboxError>;

    /// Count of pending rows (observability / tests).
    ///
    /// # Errors
    /// Returns [`BacklogSettleOutboxError`] when the backend read fails.
    fn pending_count(&self) -> Result<u64, BacklogSettleOutboxError>;
}

fn composite_key(stream: &str, dedup_key: &[u8]) -> Vec<u8> {
    let mut key = stream.as_bytes().to_vec();
    key.push(0);
    key.extend_from_slice(dedup_key);
    key
}

/// In-process outbox for unit tests.
#[derive(Default)]
pub struct InMemoryBacklogSettleOutbox {
    inner: Mutex<InMemoryInner>,
}

#[derive(Default)]
struct InMemoryInner {
    next_id: u64,
    entries: HashMap<u64, BacklogSettleEvent>,
    key_index: HashMap<Vec<u8>, u64>,
}

impl InMemoryBacklogSettleOutbox {
    /// Empty outbox.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl BacklogSettleOutbox for InMemoryBacklogSettleOutbox {
    fn push(
        &self,
        event: BacklogSettleEvent,
    ) -> Result<BacklogSettleOutboxId, BacklogSettleOutboxError> {
        let mut inner = self.inner.lock().map_err(|_| backend("poisoned"))?;
        let Some(dedup_key) = event.dedup_key.as_ref() else {
            return Err(backend("settle outbox requires dedup_key"));
        };
        let ck = composite_key(&event.stream, dedup_key);
        if let Some(old) = inner.key_index.remove(&ck) {
            inner.entries.remove(&old);
        }
        let id = inner.next_id;
        inner.next_id = inner.next_id.saturating_add(1);
        inner.entries.insert(id, event);
        inner.key_index.insert(ck, id);
        Ok(BacklogSettleOutboxId(id))
    }

    fn list_pending(
        &self,
        max: usize,
    ) -> Result<Vec<(BacklogSettleOutboxId, BacklogSettleEvent)>, BacklogSettleOutboxError> {
        let inner = self.inner.lock().map_err(|_| backend("poisoned"))?;
        let mut ids: Vec<u64> = inner.entries.keys().copied().collect();
        ids.sort_unstable();
        Ok(ids
            .into_iter()
            .take(max)
            .filter_map(|id| {
                inner
                    .entries
                    .get(&id)
                    .cloned()
                    .map(|ev| (BacklogSettleOutboxId(id), ev))
            })
            .collect())
    }

    fn remove(&self, id: BacklogSettleOutboxId) -> Result<(), BacklogSettleOutboxError> {
        let mut inner = self.inner.lock().map_err(|_| backend("poisoned"))?;
        if let Some(ev) = inner.entries.remove(&id.0)
            && let Some(dedup) = ev.dedup_key
        {
            inner.key_index.remove(&composite_key(&ev.stream, &dedup));
        }
        Ok(())
    }

    fn pending_count(&self) -> Result<u64, BacklogSettleOutboxError> {
        let inner = self.inner.lock().map_err(|_| backend("poisoned"))?;
        Ok(u64::try_from(inner.entries.len()).unwrap_or(u64::MAX))
    }
}

/// Crash-safe settle outbox in `{data_dir}/backlog-settle-outbox.redb`.
#[derive(Debug)]
pub struct RedbBacklogSettleOutbox {
    db: Mutex<Database>,
}

impl RedbBacklogSettleOutbox {
    /// Open or create the outbox database at `path`.
    ///
    /// # Errors
    /// Returns [`BacklogSettleOutboxError::Backend`] when the file cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, BacklogSettleOutboxError> {
        let db = Mutex::new(Database::create(path).map_err(backend)?);
        let outbox = Self { db };
        outbox.bootstrap()?;
        Ok(outbox)
    }

    fn bootstrap(&self) -> Result<(), BacklogSettleOutboxError> {
        let db = self.db.lock().map_err(|_| backend("poisoned"))?;
        let txn = db.begin_write().map_err(backend)?;
        {
            txn.open_table(ENTRIES).map_err(backend)?;
            txn.open_table(KEY_INDEX).map_err(backend)?;
            let mut meta = txn.open_table(META).map_err(backend)?;
            if meta.get(K_NEXT_ID).map_err(backend)?.is_none() {
                meta.insert(K_NEXT_ID, encode(&1u64).map_err(codec)?.as_slice())
                    .map_err(backend)?;
            }
        }
        txn.commit().map_err(backend)?;
        Ok(())
    }

    fn alloc_id(&self) -> Result<BacklogSettleOutboxId, BacklogSettleOutboxError> {
        let db = self.db.lock().map_err(|_| backend("poisoned"))?;
        let txn = db.begin_write().map_err(backend)?;
        let id = {
            let mut meta = txn.open_table(META).map_err(backend)?;
            let current: u64 = match meta.get(K_NEXT_ID).map_err(backend)? {
                Some(v) => decode(v.value()).map_err(codec)?,
                None => 1,
            };
            meta.insert(K_NEXT_ID, encode(&(current + 1)).map_err(codec)?.as_slice())
                .map_err(backend)?;
            BacklogSettleOutboxId(current)
        };
        txn.commit().map_err(backend)?;
        Ok(id)
    }
}

impl BacklogSettleOutbox for RedbBacklogSettleOutbox {
    fn push(
        &self,
        event: BacklogSettleEvent,
    ) -> Result<BacklogSettleOutboxId, BacklogSettleOutboxError> {
        let Some(dedup_key) = event.dedup_key.as_ref() else {
            return Err(backend("settle outbox requires dedup_key"));
        };
        let ck = composite_key(&event.stream, dedup_key);
        let bytes = encode(&event).map_err(codec)?;
        let id = self.alloc_id()?;
        let db = self.db.lock().map_err(|_| backend("poisoned"))?;
        let txn = db.begin_write().map_err(backend)?;
        {
            let mut entries = txn.open_table(ENTRIES).map_err(backend)?;
            let mut key_index = txn.open_table(KEY_INDEX).map_err(backend)?;
            if let Some(old) = key_index.get(ck.as_slice()).map_err(backend)? {
                entries.remove(old.value()).map_err(backend)?;
            }
            entries.insert(id.0, bytes.as_slice()).map_err(backend)?;
            key_index.insert(ck.as_slice(), id.0).map_err(backend)?;
        }
        txn.commit().map_err(backend)?;
        Ok(id)
    }

    fn list_pending(
        &self,
        max: usize,
    ) -> Result<Vec<(BacklogSettleOutboxId, BacklogSettleEvent)>, BacklogSettleOutboxError> {
        let db = self.db.lock().map_err(|_| backend("poisoned"))?;
        let txn = db.begin_read().map_err(backend)?;
        let entries = txn.open_table(ENTRIES).map_err(backend)?;
        let mut out = Vec::new();
        for row in entries.iter().map_err(backend)? {
            let (id, bytes) = row.map_err(backend)?;
            let event: BacklogSettleEvent = decode(bytes.value()).map_err(codec)?;
            out.push((BacklogSettleOutboxId(id.value()), event));
            if out.len() >= max {
                break;
            }
        }
        out.sort_by_key(|(id, _)| id.0);
        Ok(out)
    }

    fn remove(&self, id: BacklogSettleOutboxId) -> Result<(), BacklogSettleOutboxError> {
        let db = self.db.lock().map_err(|_| backend("poisoned"))?;
        let txn = db.begin_write().map_err(backend)?;
        {
            let mut entries = txn.open_table(ENTRIES).map_err(backend)?;
            let mut key_index = txn.open_table(KEY_INDEX).map_err(backend)?;
            if let Some(bytes) = entries.get(id.0).map_err(backend)? {
                let event: BacklogSettleEvent = decode(bytes.value()).map_err(codec)?;
                if let Some(dedup) = event.dedup_key {
                    key_index
                        .remove(composite_key(&event.stream, &dedup).as_slice())
                        .map_err(backend)?;
                }
            }
            entries.remove(id.0).map_err(backend)?;
        }
        txn.commit().map_err(backend)?;
        Ok(())
    }

    fn pending_count(&self) -> Result<u64, BacklogSettleOutboxError> {
        let db = self.db.lock().map_err(|_| backend("poisoned"))?;
        let txn = db.begin_read().map_err(backend)?;
        let entries = txn.open_table(ENTRIES).map_err(backend)?;
        entries.len().map_err(backend)
    }
}

/// Push `event` to the outbox when present (ignores missing dedup key).
pub fn push_backlog_settle(outbox: Option<&dyn BacklogSettleOutbox>, event: BacklogSettleEvent) {
    if event.dedup_key.is_none() {
        return;
    }
    if let Some(outbox) = outbox {
        let _ = outbox.push(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::external_backlog::BacklogSettleOutcome;

    #[test]
    fn in_memory_upserts_same_dedup_key() {
        let outbox = InMemoryBacklogSettleOutbox::new();
        outbox
            .push(BacklogSettleEvent {
                stream: "imports".into(),
                dedup_key: Some(b"k1".to_vec()),
                outcome: BacklogSettleOutcome::Failed {
                    attempts: 1,
                    error: "nack".into(),
                },
            })
            .unwrap();
        outbox
            .push(BacklogSettleEvent {
                stream: "imports".into(),
                dedup_key: Some(b"k1".to_vec()),
                outcome: BacklogSettleOutcome::Done,
            })
            .unwrap();
        assert_eq!(outbox.pending_count().unwrap(), 1);
        let pending = outbox.list_pending(8).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].1.outcome, BacklogSettleOutcome::Done);
    }

    #[test]
    fn redb_outbox_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settle-outbox.redb");
        let id = {
            let outbox = RedbBacklogSettleOutbox::open(&path).unwrap();
            outbox
                .push(BacklogSettleEvent {
                    stream: "jobs".into(),
                    dedup_key: Some(b"x".to_vec()),
                    outcome: BacklogSettleOutcome::Done,
                })
                .unwrap()
        };
        let outbox = RedbBacklogSettleOutbox::open(&path).unwrap();
        assert_eq!(outbox.pending_count().unwrap(), 1);
        outbox.remove(id).unwrap();
        assert_eq!(outbox.pending_count().unwrap(), 0);
    }
}
