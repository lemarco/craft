//! Multi-Raft wire envelopes (ADR 031).

use serde::{Deserialize, Serialize};

use crate::RaftRpc;

/// A peer RPC scoped to one Raft group on a multi-group node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupPeerEnvelope {
    /// Target Raft group id on the receiving node.
    pub group: u32,
    /// The inner consensus RPC.
    pub rpc: RaftRpc,
}
