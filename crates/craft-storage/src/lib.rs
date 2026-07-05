//! `craft-storage` — durable log, hard-state, and snapshot stores.
//!
//! Raft requires three pieces of durable state (Raft §5.3, §7):
//!
//! * **Hard state** — `current_term` and `voted_for`, fsync'd before a node
//!   replies to any RPC that depends on them.
//! * **Log** — the replicated entries, append-only except for suffix
//!   truncation (conflict resolution) and prefix purge (compaction).
//! * **Snapshot** — the compacted state-machine image plus the configuration
//!   in effect at its boundary.
//!
//! These are expressed as the ports [`HardStateStore`], [`LogStore`], and
//! [`SnapshotStore`] (ADR 030) so the runtime can swap the [`MemoryStorage`]
//! test double for the durable [`RedbStorage`] adapter without touching the
//! consensus core.

pub use craft_proto as proto;

mod error;
mod memory;
mod redb_store;
mod traits;
mod types;

pub use error::StorageError;
pub use memory::MemoryStorage;
pub use redb_store::RedbStorage;
pub use traits::{HardStateStore, LogStore, SnapshotStore};
pub use types::{HardState, Snapshot, SnapshotMeta};
