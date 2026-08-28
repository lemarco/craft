//! In-memory storage — the test double used by the deterministic simulator and
//! unit tests. It enforces the exact same contract as [`RedbStorage`], so tests
//! that pass against it also describe the durable backend's behaviour.

use std::collections::BTreeMap;

use crafty_proto::{LogEntry, LogIndex};

use crate::{HardState, HardStateStore, LogStore, Snapshot, SnapshotStore, StorageError};

/// A volatile [`LogStore`] + [`HardStateStore`] + [`SnapshotStore`] backed by
/// in-process maps. Nothing survives a drop.
#[derive(Debug, Default)]
pub struct MemoryStorage {
    hard_state: HardState,
    entries: BTreeMap<u64, LogEntry>,
    /// Highest index removed by compaction (the snapshot boundary).
    purged: u64,
    snapshot: Option<Snapshot>,
}

impl MemoryStorage {
    /// Create an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl HardStateStore for MemoryStorage {
    fn load_hard_state(&self) -> Result<HardState, StorageError> {
        Ok(self.hard_state.clone())
    }

    fn save_hard_state(&mut self, state: &HardState) -> Result<(), StorageError> {
        self.hard_state = state.clone();
        Ok(())
    }
}

impl LogStore for MemoryStorage {
    fn first_index(&self) -> Result<LogIndex, StorageError> {
        Ok(LogIndex(
            self.entries
                .keys()
                .next()
                .copied()
                .unwrap_or(self.purged + 1),
        ))
    }

    fn last_index(&self) -> Result<LogIndex, StorageError> {
        Ok(LogIndex(
            self.entries
                .keys()
                .next_back()
                .copied()
                .unwrap_or(self.purged),
        ))
    }

    fn read(&self, index: LogIndex) -> Result<Option<LogEntry>, StorageError> {
        Ok(self.entries.get(&index.0).cloned())
    }

    fn read_from(&self, from: LogIndex) -> Result<Vec<LogEntry>, StorageError> {
        Ok(self
            .entries
            .range(from.0..)
            .map(|(_, e)| e.clone())
            .collect())
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
        for entry in entries {
            self.entries.insert(entry.index.0, entry.clone());
        }
        Ok(())
    }

    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), StorageError> {
        self.entries.retain(|&idx, _| idx < from.0);
        Ok(())
    }

    fn purge_prefix(&mut self, through: LogIndex) -> Result<(), StorageError> {
        self.entries.retain(|&idx, _| idx > through.0);
        self.purged = self.purged.max(through.0);
        Ok(())
    }
}

impl SnapshotStore for MemoryStorage {
    fn save_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), StorageError> {
        self.snapshot = Some(snapshot.clone());
        Ok(())
    }

    fn load_snapshot(&self) -> Result<Option<Snapshot>, StorageError> {
        Ok(self.snapshot.clone())
    }
}
