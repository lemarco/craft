//! Per-group storage isolation for multi-Raft (ADR 031).
//!
//! Production nodes give each Raft group its own storage backend (separate
//! files or tables). This module provides a thin helper for tests.

use crate::{MemoryStorage, RaftStorage};

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
        Box::new(self.inner)
    }
}
