//! Dynamic multi-Raft catalog replication over group 0 (dynamic catalog / stable shards).

use serde::{Deserialize, Serialize};

use crate::NodeId;

/// Catalog metadata replicated through group 0's Raft log (not the user SM).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogCommand {
    /// Append contiguous group ids after `from_len`.
    AddGroups {
        /// Catalog length before this entry (idempotency check).
        from_len: u32,
        /// Contiguous new group ids appended in order.
        new_groups: Vec<u32>,
    },
}

/// Request to grow the multi-Raft group catalog (`/cluster/catalog/add`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogAddRequest {
    /// Wire/protocol version of the caller.
    pub protocol_version: u32,
    /// Number of contiguous Raft groups to append.
    pub add_groups: u32,
}

/// Response to a [`CatalogAddRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogAddResponse {
    /// Catalog change committed by the group 0 leader.
    Accepted {
        /// Current group 0 leader.
        leader: NodeId,
        /// Catalog length after expansion.
        catalog_len: u32,
        /// New group ids appended by this request.
        new_groups: Vec<u32>,
    },
    /// Contacted node is not the group 0 leader; retry against `leader`.
    Redirect {
        /// Best-known current leader, if any.
        leader: Option<NodeId>,
    },
    /// Request refused.
    Rejected {
        /// Why the request was refused.
        reason: CatalogRejection,
    },
}

/// Reason a [`CatalogAddRequest`] was rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogRejection {
    /// Protocol version mismatch.
    VersionSkew {
        /// Version the cluster expects.
        expected: u32,
        /// Version the caller offered.
        got: u32,
    },
    /// Single-group clusters cannot expand the catalog.
    NotMultiRaft,
    /// Planner rejected the expansion (invalid count or catalog state).
    InvalidExpansion(String),
    /// Any other refusal, human-readable.
    Other(String),
}
