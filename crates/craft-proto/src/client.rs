//! Client API wire types sent over `/client/wire` (ADR 002, ADR 003, ADR 005).

use serde::{Deserialize, Serialize};

use crate::NodeId;

/// A request from a client to the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientRequest {
    /// A write: application-encoded command replicated through the Raft log.
    Propose(Vec<u8>),
    /// A linearizable read: application-encoded query answered via ReadIndex.
    Query(Vec<u8>),
}

/// The cluster's response to a [`ClientRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientResponse {
    /// Success with an application-encoded result body.
    Ok(Vec<u8>),
    /// The contacted node is not the leader (transparent forward usually
    /// hides this; the hint aids clients that route themselves).
    NotLeader {
        /// Best-known current leader, if any.
        leader: Option<NodeId>,
    },
    /// A processing error, human-readable.
    Error(String),
}
