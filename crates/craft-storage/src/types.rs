//! Value types persisted by the stores.

use craft_proto::{LogId, Membership, NodeId, Term};
use serde::{Deserialize, Serialize};

/// The durable per-node Raft state that must be fsync'd before a node acts on
/// it (Raft §5.1, §5.2): the current term and the candidate this node voted for
/// in that term.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardState {
    /// Latest term this node has seen.
    pub current_term: Term,
    /// Candidate that received this node's vote in `current_term`, if any.
    pub voted_for: Option<NodeId>,
}

/// Metadata describing a snapshot boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMeta {
    /// The `(term, index)` of the last log entry included in the snapshot. The
    /// log is compacted through this index.
    pub last_included: LogId,
    /// The cluster configuration in effect at the snapshot boundary, so a node
    /// restoring from the snapshot recovers its membership (membership-early).
    pub membership: Membership,
}

/// A compacted state-machine image plus its [`SnapshotMeta`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    /// Snapshot boundary metadata.
    pub meta: SnapshotMeta,
    /// Opaque, application-encoded state-machine bytes.
    pub data: Vec<u8>,
}
