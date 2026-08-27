//! Client API wire types sent over `/client/wire` (ADR 002, ADR 003, ADR 005).

use serde::{Deserialize, Serialize};

use crate::{LogIndex, NodeId, Term};

/// A request from a client to the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientRequest {
    /// A write: application-encoded command replicated through the Raft log.
    Propose(Vec<u8>),
    /// A linearizable read: application-encoded query answered via ReadIndex.
    Query(Vec<u8>),
    /// A write routed to the Raft group owning `key` (multi-Raft, ADR 031).
    ProposeKeyed {
        /// Shard routing key (typically the same key the command mutates).
        key: Vec<u8>,
        /// Application-encoded command body.
        command: Vec<u8>,
    },
    /// A linearizable read routed to the Raft group owning `key`.
    QueryKeyed {
        /// Shard routing key.
        key: Vec<u8>,
        /// Application-encoded query body.
        query: Vec<u8>,
    },
    /// Ask the leader to confirm a linearizable read index without executing a
    /// query (etcd-style follower read setup, ADR 005).
    ReadIndexConfirm,
}

/// The cluster's response to a [`ClientRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientResponse {
    /// Success with an application-encoded result body.
    Ok(Vec<u8>),
    /// ReadIndex confirmed at `index` in `term` (response to
    /// [`ClientRequest::ReadIndexConfirm`]).
    ReadIndexConfirmed {
        /// The linearizable read barrier index.
        index: LogIndex,
        /// The leader term that confirmed the read.
        term: Term,
    },
    /// The contacted node is not the leader (transparent forward usually
    /// hides this; the hint aids clients that route themselves).
    NotLeader {
        /// Best-known current leader, if any.
        leader: Option<NodeId>,
    },
    /// A processing error, human-readable.
    Error(String),
}
