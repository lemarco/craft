//! Client API wire types sent over `/client/wire` (client-api, client-routing, read-consistency).

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{LogIndex, NodeId, Term};

/// A request from a client to the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientRequest {
    /// A write: application-encoded command replicated through the Raft log.
    Propose(Vec<u8>),
    /// A linearizable read: application-encoded query answered via `ReadIndex`.
    Query(Vec<u8>),
    /// A write routed to the Raft group owning `key` (multi-Raft, write-sharding-multi-raft).
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
    /// query (etcd-style follower read setup, read-consistency).
    ReadIndexConfirm {
        /// Shard routing key for multi-Raft; `None` targets group 0.
        route_key: Option<Vec<u8>>,
    },
    /// Stage a command in leader memory for cross-shard 2PC (optional).
    TwoPhasePrepare {
        /// Shared transaction id.
        tx_id: Vec<u8>,
        /// Shard routing key.
        key: Vec<u8>,
        /// Application-encoded command to commit later.
        command: Vec<u8>,
    },
    /// Commit a previously prepared command through the normal Raft log.
    TwoPhaseCommit {
        /// Shared transaction id.
        tx_id: Vec<u8>,
        /// Shard routing key.
        key: Vec<u8>,
    },
    /// Drop a previously prepared command without committing.
    TwoPhaseAbort {
        /// Shared transaction id.
        tx_id: Vec<u8>,
        /// Shard routing key.
        key: Vec<u8>,
    },
}

/// Typed failure codes for [`ClientResponse::Err`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientWireError {
    /// No Raft leader is currently elected.
    NoLeaderElected,
    /// The node runtime stopped before completing the request.
    Stopped,
    /// Application command body could not be decoded.
    DecodeCommand(String),
    /// Application query body could not be decoded.
    DecodeQuery(String),
    /// Successful response could not be encoded for the wire.
    EncodeResponse(String),
    /// Core driver or state machine rejected the request.
    Driver(String),
    /// Follower could not complete a delegated linearizable read.
    FollowerRead(String),
    /// Proxy to the leader failed (transport or wire error).
    ForwardFailed {
        /// Target leader node.
        leader: NodeId,
        /// Transport or decode failure detail.
        reason: String,
    },
    /// Proxy to the leader exceeded the forward deadline.
    ForwardTimeout {
        /// Target leader node.
        leader: NodeId,
    },
    /// Two-phase client request reached the generic propose/query path.
    TwoPhaseMisrouted,
    /// Routing key is outside the active shard range.
    KeyOutsideShardRange,
}

impl fmt::Display for ClientWireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoLeaderElected => write!(f, "no leader elected"),
            Self::Stopped => write!(f, "node runtime stopped"),
            Self::DecodeCommand(e) => write!(f, "decode command: {e}"),
            Self::DecodeQuery(e) => write!(f, "decode query: {e}"),
            Self::EncodeResponse(e) => write!(f, "encode response: {e}"),
            Self::Driver(e) => f.write_str(e),
            Self::FollowerRead(e) => write!(f, "follower read: {e}"),
            Self::ForwardFailed { leader, reason } => {
                write!(f, "forward to leader {leader:?} failed: {reason}")
            }
            Self::ForwardTimeout { leader } => {
                write!(f, "forward to leader {leader:?} timed out")
            }
            Self::TwoPhaseMisrouted => write!(f, "two-phase request misrouted"),
            Self::KeyOutsideShardRange => write!(f, "key outside active shard range"),
        }
    }
}

/// The cluster's response to a [`ClientRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientResponse {
    /// Success with an application-encoded result body.
    Ok(Vec<u8>),
    /// `ReadIndex` confirmed at `index` in `term` (response to
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
    /// A typed processing error ([`ClientWireError`]).
    Err(ClientWireError),
}

impl ClientResponse {
    /// Returns `true` when the response is a retryable cluster-side failure
    /// (no leader yet, forward timeout, runtime stopped mid-election).
    #[must_use]
    pub fn is_retryable_cluster_error(&self) -> bool {
        matches!(
            self,
            Self::Err(err) if err.is_retryable()
        )
    }
}

impl ClientWireError {
    /// Returns `true` when a client may retry against another node or after a
    /// short backoff (election in flight, stale leader hint, forward timeout).
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::NoLeaderElected | Self::ForwardTimeout { .. } | Self::Stopped
        )
    }
}
