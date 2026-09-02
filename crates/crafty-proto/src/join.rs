//! Cluster join handshake wire types over `/cluster/join` (join-rpc, join-version-skew).

use serde::{Deserialize, Serialize};

use crate::{Membership, NodeId};

/// Whether a joining node enters the committed voter set or the learner set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum JoinRole {
    /// Non-voting cluster member — full peer for traffic and workers; default
    /// for elastic scale-out.
    #[default]
    Learner,
    /// Voting member — increases quorum size and queue replication fan-out.
    /// Requires explicit opt-in on the leader (`allow_voter_join`).
    Voter,
}

/// A request from a new node asking to join the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinRequest {
    /// Wire/protocol version of the joining node (join-version-skew).
    pub protocol_version: u32,
    /// Desired node id, or `None` to have the leader assign the next free id.
    #[serde(default)]
    pub node_id: Option<NodeId>,
    /// Address peers should use to reach the joining node.
    pub advertise_addr: String,
    /// Voter (rare, seed expansion) or learner (default elastic join).
    #[serde(default)]
    pub role: JoinRole,
}

/// The response to a [`JoinRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinResponse {
    /// Join accepted; membership change committed by the leader.
    Accepted {
        /// Current leader.
        leader: NodeId,
        /// Id assigned to this node (matches the request when one was given).
        node_id: NodeId,
        /// Resulting cluster membership.
        membership: Membership,
    },
    /// Contacted node is not the leader; retry against `leader`.
    Redirect {
        /// Best-known current leader, if any.
        leader: Option<NodeId>,
    },
    /// Join refused.
    Rejected {
        /// Why the join was refused.
        reason: JoinRejection,
    },
}

/// One node's advertised address, as gossiped in a [`PeerBook`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerEntry {
    /// The node this address belongs to.
    pub node: NodeId,
    /// The address peers should dial to reach it (`host:port`).
    pub addr: String,
}

/// A snapshot of a node's known peer addresses, served over `/cluster/peers`
/// (discovery) so a newly joined node — and existing members — can learn how to
/// reach every peer without static, cluster-wide address configuration. This is
/// the address-plane counterpart to the Raft-replicated membership (which
/// carries only [`NodeId`]s, not addresses).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PeerBook {
    /// Known `(node, addr)` pairs, ascending by id.
    pub entries: Vec<PeerEntry>,
}

/// Reason a [`JoinRequest`] was rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinRejection {
    /// Protocol version mismatch — hard reject (join-version-skew).
    VersionSkew {
        /// Version the cluster expects.
        expected: u32,
        /// Version the joiner offered.
        got: u32,
    },
    /// The cluster is not currently accepting joins (`--allow-join` off).
    JoinsDisabled,
    /// A node with this id is already a member.
    Duplicate,
    /// Voter join requested but the cluster rejects voter expansion.
    VoterJoinDisabled,
    /// Any other refusal, human-readable.
    Other(String),
}
