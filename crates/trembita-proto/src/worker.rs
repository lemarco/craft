//! Shared worker identity for queue and event topic consumers.

use serde::{Deserialize, Serialize};

use crate::NodeId;

/// Identifies a queue or topic consumer (actor instance on a node).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerId {
    /// Hosting cluster node.
    pub node: NodeId,
    /// Worker actor instance id on that node.
    pub instance: u32,
}
