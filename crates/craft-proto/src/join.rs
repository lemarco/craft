//! Cluster join handshake wire types over `/cluster/join` (ADR 017, ADR 020).

use serde::{Deserialize, Serialize};

use crate::{Membership, NodeId};

/// A request from a new node asking to join the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JoinRequest {
    /// Wire/protocol version of the joining node (ADR 020).
    pub protocol_version: u32,
    /// Desired node id.
    pub node_id: NodeId,
    /// Address peers should use to reach the joining node.
    pub advertise_addr: String,
}

/// The response to a [`JoinRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinResponse {
    /// Join accepted; membership change committed by the leader.
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
/// (ADR 007) so a newly joined node — and existing members — can learn how to
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
    /// Protocol version mismatch — hard reject (ADR 020).
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
    /// Any other refusal, human-readable.
    Other(String),
}
