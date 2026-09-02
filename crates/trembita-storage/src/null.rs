//! A no-op storage backend that discards every write and reads back empty.
//!
//! This is the default for a consensus node that opts out of durability (for
//! example the deterministic simulator or a purely in-memory test). It honours
//! the [`LogStore`]/[`HardStateStore`]/[`SnapshotStore`] contracts for an empty
//! log, so a driver wired to it behaves exactly as it did before persistence
//! existed — nothing is ever recovered because nothing is ever stored.

use trembita_proto::{LogEntry, LogIndex};

use crate::{HardState, HardStateStore, LogStore, Snapshot, SnapshotStore, StorageError};

/// A storage backend that drops all writes. Reads always report an empty log,
/// the default hard state, and no snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct NullStorage;

impl HardStateStore for NullStorage {
    fn load_hard_state(&self) -> Result<HardState, StorageError> {
        Ok(HardState::default())
    }

    fn save_hard_state(&mut self, _state: &HardState) -> Result<(), StorageError> {
        Ok(())
    }
}

impl LogStore for NullStorage {
    fn first_index(&self) -> Result<LogIndex, StorageError> {
        Ok(LogIndex(1))
    }

    fn last_index(&self) -> Result<LogIndex, StorageError> {
        Ok(LogIndex(0))
    }

    fn read(&self, _index: LogIndex) -> Result<Option<LogEntry>, StorageError> {
        Ok(None)
    }

    fn read_from(&self, _from: LogIndex) -> Result<Vec<LogEntry>, StorageError> {
        Ok(Vec::new())
    }

    fn append(&mut self, _entries: &[LogEntry]) -> Result<(), StorageError> {
        Ok(())
    }

    fn truncate_suffix(&mut self, _from: LogIndex) -> Result<(), StorageError> {
        Ok(())
    }

    fn purge_prefix(&mut self, _through: LogIndex) -> Result<(), StorageError> {
        Ok(())
    }
}

impl SnapshotStore for NullStorage {
    fn save_snapshot(&mut self, _snapshot: &Snapshot) -> Result<(), StorageError> {
        Ok(())
    }

    fn load_snapshot(&self) -> Result<Option<Snapshot>, StorageError> {
        Ok(None)
    }
}
