//! Queue autoscale policy metadata replicated through Meta-Raft / group 0.

use serde::{Deserialize, Serialize};

/// Serializable worker autoscale tunables (durations as millis).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoscalePolicyWire {
    /// Actor group name to scale (registered worker type).
    pub worker_group: String,
    /// Target `(pending + leased) / worker` before scaling up.
    pub target_pending_per_worker: u64,
    /// Floor on worker instance count.
    pub min_workers: usize,
    /// Ceiling on worker instance count (also capped by live node count).
    pub max_workers: usize,
    /// Minimum time between scale decisions (millis).
    pub cooldown_ms: u64,
    /// Metrics sampling interval (millis).
    pub poll_interval_ms: u64,
}

/// Serializable membership autoscale tunables (durations as millis).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipAutoscalePolicyWire {
    /// Add a node when `(pending + leased) / live_nodes` exceeds this.
    pub pending_per_node_threshold: u64,
    /// Maximum cluster size this policy may grow to.
    pub max_nodes: usize,
    /// Minimum time between join attempts (millis).
    pub cooldown_ms: u64,
    /// Depth sampling interval (millis).
    pub poll_interval_ms: u64,
}

/// Upsert queue autoscale policy for `stream` (Meta-Raft metadata entry).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueAutoscalePolicyCommand {
    /// Queue stream these policies apply to.
    pub stream: String,
    /// Worker-group scaling policy (`None` leaves prior worker policy unchanged).
    pub worker: Option<AutoscalePolicyWire>,
    /// Cluster membership scaling policy (`None` leaves prior membership policy unchanged).
    pub membership: Option<MembershipAutoscalePolicyWire>,
}
