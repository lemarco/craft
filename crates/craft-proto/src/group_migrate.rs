//! Cross-node Raft group migration wire types (ADR 031).

use serde::{Deserialize, Serialize};

use crate::{LogEntry, LogId, LogIndex, Membership, NodeId, Term};

/// Durable hard state exported with a group migration bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupMigrationHardState {
    /// Latest term seen by the exporting replica.
    pub current_term: Term,
    /// Vote cast in `current_term`, if any.
    pub voted_for: Option<NodeId>,
}

/// Snapshot metadata bundled for group migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupMigrationSnapshotMeta {
    /// Last log entry included in the snapshot.
    pub last_included: LogId,
    /// Membership in effect at the snapshot boundary.
    pub membership: Membership,
}

/// Application snapshot bytes bundled for group migration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupMigrationSnapshot {
    /// Snapshot boundary metadata.
    pub meta: GroupMigrationSnapshotMeta,
    /// Opaque state-machine bytes.
    pub data: Vec<u8>,
}

/// Full durable state for one Raft group replica (log + snapshot + hard state).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupMigrationBundle {
    /// Persisted term/vote.
    pub hard_state: GroupMigrationHardState,
    /// Highest log index removed by compaction (`0` if none).
    pub purged_through: LogIndex,
    /// Latest stored snapshot, if any.
    pub snapshot: Option<GroupMigrationSnapshot>,
    /// Live log suffix retained after compaction.
    pub log: Vec<LogEntry>,
}

/// Request to adopt a Raft group replica on the target node
/// (`POST /raft/v1/cluster/group/migrate`, ADR 031).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupMigrateRequest {
    /// Raft group id being transferred.
    pub group: u32,
    /// Physical node that exported the bundle.
    pub from: NodeId,
    /// Exported durable state for the group.
    pub bundle: GroupMigrationBundle,
}

/// Response to a [`GroupMigrateRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupMigrateReply {
    /// Whether the target now hosts the group (`true` includes idempotent acks).
    pub adopted: bool,
    /// Human-readable failure when `adopted` is `false`.
    pub error: Option<String>,
}
