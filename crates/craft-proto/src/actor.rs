//! Cross-node actor messaging wire types (ADR 013, ADR 019).

use serde::{Deserialize, Serialize};

use crate::NodeId;

/// A reference used to route a message to an actor or actor group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorRef {
    /// Logical group / pool name (e.g. `"workers"`).
    pub group: String,
    /// Optional routing key for consistent-hash routing (ADR 019); when
    /// `None`, round-robin routing is used.
    pub key: Option<String>,
    /// Pin to a specific node; when `None`, the registry chooses placement.
    pub node: Option<NodeId>,
}

/// An actor message crossing a node boundary via `/actor/deliver`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorEnvelope {
    /// Where the message should be delivered.
    pub target: ActorRef,
    /// Application-encoded message body.
    pub payload: Vec<u8>,
    /// Whether the sender awaits a reply (`ask`) versus fire-and-forget (`cast`).
    pub reply_expected: bool,
}
