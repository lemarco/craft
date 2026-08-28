//! Test-only storage helpers (not part of the production API).

use crafty_proto::{LogEntry, LogIndex};

use crate::{
    HardState, HardStateStore, LogStore, MemoryStorage, RaftStorage, Snapshot, SnapshotStore,
    StorageError,
};

/// An isolated in-memory store for one Raft group.
#[derive(Debug, Default)]
pub struct GroupMemoryStorage {
    group: u32,
    inner: MemoryStorage,
}

impl GroupMemoryStorage {
    /// A fresh in-memory store tagged with `group` (for debugging only).
    #[must_use]
    pub fn new(group: u32) -> Self {
        Self {
            group,
            inner: MemoryStorage::default(),
        }
    }

    /// The group id.
    #[must_use]
    pub fn group(&self) -> u32 {
        self.group
    }

    /// Box as a [`RaftStorage`] trait object.
    #[must_use]
    pub fn boxed(self) -> Box<dyn RaftStorage> {
        Box::new(self)
    }
}

impl HardStateStore for GroupMemoryStorage {
    fn load_hard_state(&self) -> Result<HardState, StorageError> {
        self.inner.load_hard_state()
    }

    fn save_hard_state(&mut self, state: &HardState) -> Result<(), StorageError> {
        self.inner.save_hard_state(state)
    }
}

impl LogStore for GroupMemoryStorage {
    fn first_index(&self) -> Result<LogIndex, StorageError> {
        self.inner.first_index()
    }

    fn last_index(&self) -> Result<LogIndex, StorageError> {
        self.inner.last_index()
    }

    fn read(&self, index: LogIndex) -> Result<Option<LogEntry>, StorageError> {
        self.inner.read(index)
    }

    fn read_from(&self, from: LogIndex) -> Result<Vec<LogEntry>, StorageError> {
        self.inner.read_from(from)
    }

    fn append(&mut self, entries: &[LogEntry]) -> Result<(), StorageError> {
        self.inner.append(entries)
    }

    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), StorageError> {
        self.inner.truncate_suffix(from)
    }

    fn purge_prefix(&mut self, through: LogIndex) -> Result<(), StorageError> {
        self.inner.purge_prefix(through)
    }
}

impl SnapshotStore for GroupMemoryStorage {
    fn save_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), StorageError> {
        self.inner.save_snapshot(snapshot)
    }

    fn load_snapshot(&self) -> Result<Option<Snapshot>, StorageError> {
        self.inner.load_snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HardState, MemoryStorage};

    #[test]
    fn group_memory_storage_matches_memory_contract() {
        let mut a = GroupMemoryStorage::new(0);
        let b = GroupMemoryStorage::new(1);
        a.save_hard_state(&HardState {
            current_term: crafty_proto::Term(2),
            voted_for: None,
        })
        .unwrap();
        assert_eq!(b.load_hard_state().unwrap(), HardState::default());

        let mut plain = MemoryStorage::default();
        plain
            .save_hard_state(&HardState {
                current_term: crafty_proto::Term(2),
                voted_for: None,
            })
            .unwrap();
        assert_eq!(
            a.load_hard_state().unwrap(),
            plain.load_hard_state().unwrap()
        );
    }
}
