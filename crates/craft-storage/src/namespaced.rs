//! Per-group storage isolation for multi-Raft (ADR 031).
//!
//! Each Raft group gets its own backend so logs, hard state, and snapshots
//! never collide. In production this is one `redb` file per group under a
//! shared data directory; tests use [`GroupMemoryStorage`].

use std::path::{Path, PathBuf};

use craft_proto::{LogEntry, LogIndex};

use crate::{
    HardState, HardStateStore, LogStore, MemoryStorage, RaftStorage, RedbStorage, Snapshot,
    SnapshotStore, StorageError,
};

/// Path to the `redb` file for Raft group `group` under directory `base`.
#[must_use]
pub fn group_redb_path(base: impl AsRef<Path>, group: u32) -> PathBuf {
    base.as_ref().join(format!("group-{group}.redb"))
}

/// Opens one [`RedbStorage`] file per Raft group under a shared directory.
#[derive(Debug, Clone)]
pub struct GroupRedbLayout {
    base: PathBuf,
}

impl GroupRedbLayout {
    /// Use `base` as the parent directory for per-group `group-<id>.redb` files.
    #[must_use]
    pub fn new(base: impl Into<PathBuf>) -> Self {
        Self { base: base.into() }
    }

    /// The configured data directory.
    #[must_use]
    pub fn base(&self) -> &Path {
        &self.base
    }

    /// Open (creating if absent) durable storage for `group`.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the directory cannot be created or the
    /// database file cannot be opened.
    pub fn open_group(&self, group: u32) -> Result<RedbStorage, StorageError> {
        std::fs::create_dir_all(&self.base).map_err(|e| StorageError::Backend(e.to_string()))?;
        RedbStorage::open(group_redb_path(&self.base, group))
    }

    /// Open every group `0..group_count` and return them in order.
    ///
    /// # Errors
    /// Returns [`StorageError`] if any group file cannot be opened.
    pub fn open_groups(&self, group_count: u32) -> Result<Vec<RedbStorage>, StorageError> {
        (0..group_count).map(|g| self.open_group(g)).collect()
    }
}

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
