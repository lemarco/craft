//! Per-group storage isolation for multi-Raft (ADR 031).
//!
//! Each Raft group gets its own backend so logs, hard state, and snapshots
//! never collide. In production this is one `redb` file per group under a
//! shared data directory; tests use [`crate::test_util::GroupMemoryStorage`].

use std::path::{Path, PathBuf};

use crate::{RedbStorage, StorageError};

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
