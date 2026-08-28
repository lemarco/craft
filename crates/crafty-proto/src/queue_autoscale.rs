//! Queue autoscale policy metadata replicated through Meta-Raft / group 0.

use serde::{Deserialize, Serialize};

/// Serializable worker autoscale tunables (durations as millis).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoscalePolicyWire {
    pub worker_group: String,
    pub target_pending_per_worker: u64,
    pub min_workers: usize,
    pub max_workers: usize,
    pub cooldown_ms: u64,
    pub poll_interval_ms: u64,
}

/// Serializable membership autoscale tunables (durations as millis).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipAutoscalePolicyWire {
    pub pending_per_node_threshold: u64,
    pub max_nodes: usize,
    pub cooldown_ms: u64,
    pub poll_interval_ms: u64,
}

/// Upsert queue autoscale policy for `stream` (Meta-Raft metadata entry).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueAutoscalePolicyCommand {
    pub stream: String,
    pub worker: Option<AutoscalePolicyWire>,
    pub membership: Option<MembershipAutoscalePolicyWire>,
}
