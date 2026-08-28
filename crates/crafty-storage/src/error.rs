//! Storage error type shared by every store implementation.

use crafty_proto::CodecError;

/// An error raised by a [`LogStore`](crate::LogStore),
/// [`HardStateStore`](crate::HardStateStore), or
/// [`SnapshotStore`](crate::SnapshotStore) implementation.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// A stored value could not be encoded or decoded.
    #[error("codec: {0}")]
    Codec(#[from] CodecError),

    /// The underlying storage backend (e.g. `redb`) failed.
    #[error("backend: {0}")]
    Backend(String),

    /// An append would leave a hole in the log. The log must stay contiguous:
    /// the first appended index has to equal `last_index + 1`, and entries
    /// within a batch must be consecutive.
    #[error("non-contiguous append: expected index {expected}, got {got}")]
    NonContiguous {
        /// The index the store expected the batch to start at.
        expected: u64,
        /// The index the batch actually started at.
        got: u64,
    },
}
