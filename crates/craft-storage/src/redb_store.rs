//! Durable [`redb`] adapter — the production backend (backlog Track B3).
//!
//! Layout: one table keyed by log index holds `postcard`-encoded [`LogEntry`]
//! values; a small metadata table holds the hard state, the latest snapshot,
//! and the compaction (purge) boundary. Every mutating method commits its own
//! `redb` write transaction, so a crash leaves the store at a consistent,
//! fsync'd point.

use std::path::Path;

use craft_proto::{LogEntry, LogIndex, decode, encode};
use redb::{Database, ReadableTable, TableDefinition};

use crate::{HardState, HardStateStore, LogStore, Snapshot, SnapshotStore, StorageError};

/// Log entries, keyed by 1-based index.
const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
/// Single-row metadata (hard state, snapshot, purge boundary).
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

const K_HARD_STATE: &str = "hard_state";
const K_SNAPSHOT: &str = "snapshot";
const K_PURGED: &str = "purged";

fn backend(e: impl std::fmt::Display) -> StorageError {
    StorageError::Backend(e.to_string())
}

/// A crash-safe [`LogStore`] + [`HardStateStore`] + [`SnapshotStore`] backed by
/// a single `redb` database file.
#[derive(Debug)]
pub struct RedbStorage {
    db: Database,
}

impl RedbStorage {
    /// Open (creating if absent) the database at `path` and ensure both tables
    /// exist so later read transactions never hit a missing-table error.
    ///
    /// # Errors
    /// Returns [`StorageError::Backend`] if the file cannot be opened or the
    /// bootstrap transaction fails.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let db = Database::create(path).map_err(backend)?;
        let txn = db.begin_write().map_err(backend)?;
        {
            txn.open_table(LOG).map_err(backend)?;
            txn.open_table(META).map_err(backend)?;
        }
        txn.commit().map_err(backend)?;
        Ok(Self { db })
    }

    fn read_meta(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let txn = self.db.begin_read().map_err(backend)?;
        let table = txn.open_table(META).map_err(backend)?;
        let value = table.get(key).map_err(backend)?;
        Ok(value.map(|guard| guard.value().to_vec()))
    }

    fn write_meta(&self, key: &str, bytes: &[u8]) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(backend)?;
        {
            let mut table = txn.open_table(META).map_err(backend)?;
            table.insert(key, bytes).map_err(backend)?;
        }
        txn.commit().map_err(backend)?;
        Ok(())
    }

    fn purged(&self) -> Result<u64, StorageError> {
        match self.read_meta(K_PURGED)? {
            Some(bytes) => Ok(decode::<u64>(&bytes)?),
            None => Ok(0),
        }
    }
}

impl HardStateStore for RedbStorage {
    fn load_hard_state(&self) -> Result<HardState, StorageError> {
        match self.read_meta(K_HARD_STATE)? {
            Some(bytes) => Ok(decode(&bytes)?),
            None => Ok(HardState::default()),
        }
    }

    fn save_hard_state(&mut self, state: &HardState) -> Result<(), StorageError> {
        self.write_meta(K_HARD_STATE, &encode(state)?)
    }
}

impl LogStore for RedbStorage {
    fn first_index(&self) -> Result<LogIndex, StorageError> {
        let txn = self.db.begin_read().map_err(backend)?;
        let table = txn.open_table(LOG).map_err(backend)?;
        match table.first().map_err(backend)? {
            Some((key, _)) => Ok(LogIndex(key.value())),
            None => Ok(LogIndex(self.purged()? + 1)),
        }
    }

    fn last_index(&self) -> Result<LogIndex, StorageError> {
        let txn = self.db.begin_read().map_err(backend)?;
        let table = txn.open_table(LOG).map_err(backend)?;
        match table.last().map_err(backend)? {
            Some((key, _)) => Ok(LogIndex(key.value())),
            None => Ok(LogIndex(self.purged()?)),
        }
    }

    fn read(&self, index: LogIndex) -> Result<Option<LogEntry>, StorageError> {
        let txn = self.db.begin_read().map_err(backend)?;
        let table = txn.open_table(LOG).map_err(backend)?;
        match table.get(index.0).map_err(backend)? {
            Some(guard) => Ok(Some(decode(guard.value())?)),
            None => Ok(None),
        }
    }

    fn read_from(&self, from: LogIndex) -> Result<Vec<LogEntry>, StorageError> {
        let txn = self.db.begin_read().map_err(backend)?;
        let table = txn.open_table(LOG).map_err(backend)?;
        let mut out = Vec::new();
        for row in table.range(from.0..).map_err(backend)? {
            let (_, value) = row.map_err(backend)?;
            out.push(decode(value.value())?);
        }
        Ok(out)
    }

    fn append(&mut self, entries: &[LogEntry]) -> Result<(), StorageError> {
        if entries.is_empty() {
            return Ok(());
        }
        let expected = self.last_index()?.0 + 1;
        if entries[0].index.0 != expected {
            return Err(StorageError::NonContiguous {
                expected,
                got: entries[0].index.0,
            });
        }
        for pair in entries.windows(2) {
            if pair[1].index.0 != pair[0].index.0 + 1 {
                return Err(StorageError::NonContiguous {
                    expected: pair[0].index.0 + 1,
                    got: pair[1].index.0,
                });
            }
        }
        let txn = self.db.begin_write().map_err(backend)?;
        {
            let mut table = txn.open_table(LOG).map_err(backend)?;
            for entry in entries {
                table
                    .insert(entry.index.0, encode(entry)?.as_slice())
                    .map_err(backend)?;
            }
        }
        txn.commit().map_err(backend)?;
        Ok(())
    }

    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(backend)?;
        {
            let mut table = txn.open_table(LOG).map_err(backend)?;
            table.retain(|key, _| key < from.0).map_err(backend)?;
        }
        txn.commit().map_err(backend)?;
        Ok(())
    }

    fn purge_prefix(&mut self, through: LogIndex) -> Result<(), StorageError> {
        let txn = self.db.begin_write().map_err(backend)?;
        {
            let mut table = txn.open_table(LOG).map_err(backend)?;
            table.retain(|key, _| key > through.0).map_err(backend)?;
        }
        txn.commit().map_err(backend)?;
        let new_purged = self.purged()?.max(through.0);
        self.write_meta(K_PURGED, &encode(&new_purged)?)
    }
}

impl SnapshotStore for RedbStorage {
    fn save_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), StorageError> {
        self.write_meta(K_SNAPSHOT, &encode(snapshot)?)
    }

    fn load_snapshot(&self) -> Result<Option<Snapshot>, StorageError> {
        match self.read_meta(K_SNAPSHOT)? {
            Some(bytes) => Ok(Some(decode(&bytes)?)),
            None => Ok(None),
        }
    }
}
