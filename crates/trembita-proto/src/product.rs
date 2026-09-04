//! Typed failure codes for queue, topic, and actor-store wire replies.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::NodeId;

/// Shared product-service wire error (queue / topic / actor-store).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProductWireError {
    /// No Raft leader is currently elected.
    NoLeaderElected,
    /// The contacted node is not the Raft leader (follower should forward).
    NotLeader,
    /// Replication RPC rejected: declared leader does not match the receiver's hint.
    ReplicateNotLeader,
    /// Other voters exist in membership but none are reachable for replication.
    NoReachableVoters,
    /// Proxy to the leader failed (transport or wire error).
    ForwardFailed {
        /// Target leader node.
        leader: NodeId,
        /// Transport or decode failure detail.
        reason: String,
    },
    /// Queue stream name is not registered on this node.
    UnknownStream {
        /// Requested stream name.
        stream: String,
    },
    /// Topic name is not registered on this node.
    UnknownTopic {
        /// Requested topic name.
        topic: String,
    },
    /// Actor-store key operation referred to an unknown resource.
    UnknownKey {
        /// Store key or identifier from the request.
        key: String,
    },
    /// Local backend or application logic rejected the operation.
    Backend(String),
    /// Follower failed to apply a replication op from the leader.
    ReplicateApply(String),
}

impl fmt::Display for ProductWireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoLeaderElected => write!(f, "no raft leader elected"),
            Self::NotLeader => write!(f, "not leader"),
            Self::ReplicateNotLeader => {
                write!(f, "replicate rejected: caller is not raft leader")
            }
            Self::NoReachableVoters => write!(
                f,
                "replication failed: other voters exist but none are reachable"
            ),
            Self::ForwardFailed { leader, reason } => {
                write!(f, "forward to leader {leader:?} failed: {reason}")
            }
            Self::UnknownStream { stream } => write!(f, "unknown queue stream {stream:?}"),
            Self::UnknownTopic { topic } => write!(f, "unknown topic {topic:?}"),
            Self::UnknownKey { key } => write!(f, "unknown key {key:?}"),
            Self::Backend(msg) | Self::ReplicateApply(msg) => f.write_str(msg),
        }
    }
}

impl ProductWireError {
    /// Wrap a backend/application message.
    #[must_use]
    pub fn backend(msg: impl fmt::Display) -> Self {
        Self::Backend(msg.to_string())
    }

    /// Map legacy string errors from call sites to typed variants when possible.
    #[must_use]
    pub fn classify(msg: impl Into<String>) -> Self {
        let msg = msg.into();
        if msg == "no raft leader elected" {
            return Self::NoLeaderElected;
        }
        if msg == "not leader" {
            return Self::NotLeader;
        }
        if msg.contains("replicate rejected") {
            return Self::ReplicateNotLeader;
        }
        if msg.contains("other voters exist but none are reachable") {
            return Self::NoReachableVoters;
        }
        if let Some(stream) = msg.strip_prefix("unknown queue stream ") {
            return Self::UnknownStream {
                stream: stream.trim_matches('"').to_string(),
            };
        }
        if let Some(topic) = msg.strip_prefix("unknown topic ") {
            return Self::UnknownTopic {
                topic: topic.trim_matches('"').to_string(),
            };
        }
        if let Some(rest) = msg.strip_prefix("forward to leader ")
            && let Some((leader_part, reason)) = rest.split_once(" failed: ")
            && let Ok(id) = leader_part
                .trim_start_matches("NodeId(")
                .trim_end_matches(')')
                .parse()
        {
            return Self::ForwardFailed {
                leader: NodeId(id),
                reason: reason.to_string(),
            };
        }
        Self::Backend(msg)
    }
}

impl From<String> for ProductWireError {
    fn from(value: String) -> Self {
        Self::classify(value)
    }
}

impl From<&str> for ProductWireError {
    fn from(value: &str) -> Self {
        Self::classify(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_known_messages() {
        assert_eq!(
            ProductWireError::classify("no raft leader elected"),
            ProductWireError::NoLeaderElected
        );
        assert!(matches!(
            ProductWireError::classify(r#"unknown queue stream "jobs""#),
            ProductWireError::UnknownStream { .. }
        ));
    }
}
