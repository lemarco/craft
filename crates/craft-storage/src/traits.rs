//! The storage ports (ADR 030). The consensus runtime depends only on these
//! traits, never on a concrete backend.

use craft_proto::{LogEntry, LogIndex};

use crate::{HardState, Snapshot, StorageError};

/// Durable storage for the [`HardState`] (term + vote).
pub trait HardStateStore {
    /// Load the persisted hard state, or the default (`term 0`, no vote) if
    /// nothing has been written yet.
    ///
    /// # Errors
    /// Returns [`StorageError`] if the backend read or a decode fails.
    fn load_hard_state(&self) -> Result<HardState, StorageError>;

    /// Persist the hard state. Implementations must make the write durable
    /// before returning (Raft correctness depends on this).
    ///
    /// # Errors
    /// Returns [`StorageError`] if the backend write or an encode fails.
    fn save_hard_state(&mut self, state: &HardState) -> Result<(), StorageError>;
}

/// Durable storage for the Raft log.
///
/// The log is identified by 1-based indices. It is append-only except for two
/// operations: [`truncate_suffix`](LogStore::truncate_suffix) removes a
/// conflicting tail during replication (Raft §5.3), and
/// [`purge_prefix`](LogStore::purge_prefix) discards a compacted prefix after a
/// snapshot (Raft §7).
pub trait LogStore {
    /// The first index still present in the log. For a fresh log this is `1`;
    /// after a purge through index `k` it is `k + 1`.
    ///
    /// # Errors
    /// Returns [`StorageError`] on a backend read failure.
    fn first_index(&self) -> Result<LogIndex, StorageError>;

    /// The highest index present in the log, or the purge boundary (which may
    /// be `0`) when the live log is empty.
    ///
    /// # Errors
    /// Returns [`StorageError`] on a backend read failure.
    fn last_index(&self) -> Result<LogIndex, StorageError>;

    /// Read a single entry, or `None` if that index is absent (never written,
    /// truncated away, or purged by compaction).
    ///
    /// # Errors
    /// Returns [`StorageError`] on a backend read or decode failure.
    fn read(&self, index: LogIndex) -> Result<Option<LogEntry>, StorageError>;

    /// Read all entries with index `>= from`, in ascending order.
    ///
    /// # Errors
    /// Returns [`StorageError`] on a backend read or decode failure.
    fn read_from(&self, from: LogIndex) -> Result<Vec<LogEntry>, StorageError>;

    /// Append a contiguous batch of entries. The batch must start at
    /// `last_index + 1` and be internally consecutive.
    ///
    /// # Errors
    /// Returns [`StorageError::NonContiguous`] if the batch would leave a hole,
    /// or [`StorageError`] on a backend write / encode failure.
    fn append(&mut self, entries: &[LogEntry]) -> Result<(), StorageError>;

    /// Remove every entry with index `>= from` (conflict resolution).
    ///
    /// # Errors
    /// Returns [`StorageError`] on a backend write failure.
    fn truncate_suffix(&mut self, from: LogIndex) -> Result<(), StorageError>;

    /// Remove every entry with index `<= through` (log compaction). Advances
    /// [`first_index`](LogStore::first_index) past `through`.
    ///
    /// # Errors
    /// Returns [`StorageError`] on a backend write failure.
    fn purge_prefix(&mut self, through: LogIndex) -> Result<(), StorageError>;
}

/// Convenience bundle of all three Raft storage ports (plus `Send`) so the
/// consensus runtime can hold a single `Box<dyn RaftStorage>` regardless of the
/// concrete backend (backlog B4). Blanket-implemented for every type that
/// provides the [`HardStateStore`], [`LogStore`], and [`SnapshotStore`] ports.
pub trait RaftStorage: HardStateStore + LogStore + SnapshotStore + Send {}

impl<T: HardStateStore + LogStore + SnapshotStore + Send> RaftStorage for T {}

/// Durable storage for the most recent [`Snapshot`].
pub trait SnapshotStore {
    /// Persist a snapshot, replacing any previously stored one.
    ///
    /// # Errors
    /// Returns [`StorageError`] on a backend write or encode failure.
    fn save_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), StorageError>;

    /// Load the stored snapshot, or `None` if none has been saved.
    ///
    /// # Errors
    /// Returns [`StorageError`] on a backend read or decode failure.
    fn load_snapshot(&self) -> Result<Option<Snapshot>, StorageError>;
}
