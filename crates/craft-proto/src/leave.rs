//! Cluster leave handshake wire types over `/cluster/leave` (symmetric to join-rpc).

use serde::{Deserialize, Serialize};

use crate::{Membership, NodeId};

/// A request to remove a node from the cluster registry (group 0).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaveRequest {
    /// Wire/protocol version (join-version-skew).
    pub protocol_version: u32,
    /// Node id to remove from the committed voter set.
    pub node_id: NodeId,
}

/// The response to a [`LeaveRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LeaveResponse {
    /// Leave accepted; membership change committed by the leader.
    Accepted {
        /// Current leader.
        leader: NodeId,
        /// Resulting cluster membership.
        membership: Membership,
    },
    /// Contacted node is not the leader; retry against `leader`.
    Redirect {
        /// Best-known current leader, if any.
        leader: Option<NodeId>,
    },
    /// Leave refused.
    Rejected {
        /// Why the leave was refused.
        reason: LeaveRejection,
    },
}

/// Reason a [`LeaveRequest`] was rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LeaveRejection {
    /// Protocol version mismatch — hard reject (join-version-skew).
    VersionSkew {
        /// Version the cluster expects.
        expected: u32,
        /// Version the requester offered.
        got: u32,
    },
    /// The cluster is not currently accepting leaves (`--allow-leave` off).
    LeavesDisabled,
    /// `node_id` is not in the committed voter set.
    NotMember,
    /// Removing `node_id` would leave an empty voter set.
    LastMember,
    /// Any other refusal, human-readable.
    Other(String),
}
