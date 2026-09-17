//! B-44 — coordination growth ceilings and operator-visible auto-shard state.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::Serialize;

/// Hard caps on closed-loop coordination growth (env / [`TrembitaConfigure`](../../crates/trembita/src/configure.rs)).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct CoordinationCeilings {
    /// Cap physical queue shards per logical auto-shard stream (`None` = policy only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_queue_shards: Option<usize>,
    /// Cap coordination Raft groups for runtime [`add_raft_groups`](../../crates/trembita-assembly/src/cluster_handle/cluster.rs) (`None` = no extra cap).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_raft_groups: Option<u32>,
}

/// Live auto-shard row for one logical queue stream.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct AutoShardClosedLoopLine {
    /// Logical stream name.
    pub stream: String,
    /// Last sampled logical `pending` depth.
    pub pending: u64,
    /// Consecutive leader ticks at/above threshold.
    pub hot_ticks: u32,
    /// Physical shards currently open.
    pub shard_count: usize,
    /// Effective ceiling (`min(policy.max_shards, coordination max)`).
    pub effective_max_shards: usize,
    /// When expansion is blocked or idle, a stable machine reason (see runbook B-44).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub why_not_scaling: Option<String>,
}

/// Point-in-time closed-loop snapshot for introspect (B-43 [`OpsSummary`](../../crates/trembita/src/app/ops_summary.rs)).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct CoordinationClosedLoopSnapshot {
    /// Active ceilings on this node.
    pub ceilings: CoordinationCeilings,
    /// Coordination Raft groups at boot / last catalog size.
    pub coordination_raft_groups: u32,
    /// Raft growth blocker when automatic expansion is not in progress.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raft_why_not_scaling: Option<String>,
    /// Per auto-shard logical stream (leader-updated).
    pub auto_shard_streams: Vec<AutoShardClosedLoopLine>,
}

#[derive(Debug, Default)]
struct StreamState {
    pending: u64,
    hot_ticks: u32,
    shard_count: usize,
    effective_max_shards: usize,
    why_not_scaling: Option<String>,
}

/// Leader auto-shard coordinator writes; ops introspect reads.
#[derive(Debug, Default)]
pub struct CoordinationClosedLoopRegistry {
    ceilings: CoordinationCeilings,
    streams: Mutex<BTreeMap<String, StreamState>>,
}

impl CoordinationClosedLoopRegistry {
    /// Shared registry for one node process.
    #[must_use]
    pub fn new(ceilings: CoordinationCeilings) -> Arc<Self> {
        Arc::new(Self {
            ceilings,
            streams: Mutex::new(BTreeMap::new()),
        })
    }

    /// Configured ceilings.
    #[must_use]
    pub fn ceilings(&self) -> CoordinationCeilings {
        self.ceilings
    }

    /// Register a logical auto-shard stream before the leader loop starts.
    pub fn register_stream(&self, logical: &str, policy_max_shards: usize) {
        let effective = effective_max_shards(policy_max_shards, self.ceilings.max_queue_shards);
        let mut map = self.streams.lock().expect("poisoned");
        map.entry(logical.to_string())
            .or_insert_with(|| StreamState {
                effective_max_shards: effective,
                why_not_scaling: Some("pending_below_threshold".into()),
                ..StreamState::default()
            });
    }

    /// Update after a leader coordinator tick.
    pub fn record_auto_shard_tick(
        &self,
        logical: &str,
        pending: u64,
        hot_ticks: u32,
        shard_count: usize,
        policy_max_shards: usize,
        expand_result: AutoShardExpandResult,
    ) {
        let effective = effective_max_shards(policy_max_shards, self.ceilings.max_queue_shards);
        let why = why_not_after_tick(pending, hot_ticks, shard_count, effective, &expand_result);
        let mut map = self.streams.lock().expect("poisoned");
        let entry = map
            .entry(logical.to_string())
            .or_insert_with(|| StreamState {
                effective_max_shards: effective,
                ..StreamState::default()
            });
        entry.pending = pending;
        entry.hot_ticks = hot_ticks;
        entry.shard_count = shard_count;
        entry.effective_max_shards = effective;
        entry.why_not_scaling = why;
    }

    /// JSON snapshot merged with live Raft layout.
    #[must_use]
    pub fn snapshot(&self, coordination_raft_groups: u32) -> CoordinationClosedLoopSnapshot {
        let streams = self.streams.lock().expect("poisoned");
        CoordinationClosedLoopSnapshot {
            ceilings: self.ceilings,
            coordination_raft_groups,
            raft_why_not_scaling: raft_why_not_scaling(
                coordination_raft_groups,
                self.ceilings.max_raft_groups,
            ),
            auto_shard_streams: streams
                .iter()
                .map(|(stream, s)| AutoShardClosedLoopLine {
                    stream: stream.clone(),
                    pending: s.pending,
                    hot_ticks: s.hot_ticks,
                    shard_count: s.shard_count,
                    effective_max_shards: s.effective_max_shards,
                    why_not_scaling: s.why_not_scaling.clone(),
                })
                .collect(),
        }
    }
}

/// Outcome of one expand attempt on the leader tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutoShardExpandResult {
    /// Depth below threshold or still warming ticks.
    Idle,
    /// Expanded to another physical shard.
    Expanded,
    /// At effective shard ceiling.
    AtCeiling,
    /// Backend error from [`QueueService::try_expand_sharded_stream`](crate::queue_service::QueueService).
    ExpandFailed(String),
}

/// Minimum of policy `max_shards` and optional B-44 env ceiling (at least 1).
#[must_use]
pub fn effective_max_shards(policy_max: usize, ceiling: Option<usize>) -> usize {
    match ceiling {
        Some(cap) => policy_max.min(cap.max(1)),
        None => policy_max,
    }
}

#[must_use]
fn why_not_after_tick(
    pending: u64,
    hot_ticks: u32,
    shard_count: usize,
    effective_max: usize,
    expand: &AutoShardExpandResult,
) -> Option<String> {
    match expand {
        AutoShardExpandResult::Expanded => None,
        AutoShardExpandResult::AtCeiling => Some("at_max_queue_shards_ceiling".into()),
        AutoShardExpandResult::ExpandFailed(msg) => Some(format!("expand_failed:{msg}")),
        AutoShardExpandResult::Idle if shard_count >= effective_max => {
            Some("at_max_queue_shards_ceiling".into())
        }
        AutoShardExpandResult::Idle if hot_ticks > 0 => Some("accumulating_hot_ticks".into()),
        AutoShardExpandResult::Idle if pending == 0 => Some("queue_empty".into()),
        AutoShardExpandResult::Idle => Some("pending_below_threshold".into()),
    }
}

/// Stable machine reason when automatic Raft catalog growth is idle or blocked.
#[must_use]
pub fn raft_why_not_scaling(current_groups: u32, max_groups: Option<u32>) -> Option<String> {
    if let Some(max) = max_groups
        && current_groups >= max
    {
        return Some("at_max_raft_groups_ceiling".into());
    }
    Some("automatic_raft_group_growth_not_active".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b44_effective_max_shards_respects_ceiling() {
        assert_eq!(effective_max_shards(16, Some(8)), 8);
        assert_eq!(effective_max_shards(4, Some(8)), 4);
        assert_eq!(effective_max_shards(16, None), 16);
    }

    #[test]
    fn b44_why_not_after_tick_scenarios_table() {
        struct Row {
            pending: u64,
            hot_ticks: u32,
            shard_count: usize,
            effective_max: usize,
            expand: AutoShardExpandResult,
            want: Option<&'static str>,
        }
        let rows = [
            Row {
                pending: 0,
                hot_ticks: 0,
                shard_count: 1,
                effective_max: 8,
                expand: AutoShardExpandResult::Idle,
                want: Some("queue_empty"),
            },
            Row {
                pending: 100,
                hot_ticks: 1,
                shard_count: 1,
                effective_max: 8,
                expand: AutoShardExpandResult::Idle,
                want: Some("accumulating_hot_ticks"),
            },
            Row {
                pending: 900,
                hot_ticks: 0,
                shard_count: 1,
                effective_max: 8,
                expand: AutoShardExpandResult::Idle,
                want: Some("pending_below_threshold"),
            },
            Row {
                pending: 900,
                hot_ticks: 3,
                shard_count: 2,
                effective_max: 8,
                expand: AutoShardExpandResult::Expanded,
                want: None,
            },
            Row {
                pending: 900,
                hot_ticks: 3,
                shard_count: 8,
                effective_max: 8,
                expand: AutoShardExpandResult::AtCeiling,
                want: Some("at_max_queue_shards_ceiling"),
            },
        ];
        for row in rows {
            let got = why_not_after_tick(
                row.pending,
                row.hot_ticks,
                row.shard_count,
                row.effective_max,
                &row.expand,
            );
            assert_eq!(got.as_deref(), row.want, "pending={}", row.pending);
        }
    }

    #[test]
    fn b44_raft_why_not_scaling_at_ceiling() {
        assert_eq!(
            raft_why_not_scaling(4, Some(4)).as_deref(),
            Some("at_max_raft_groups_ceiling")
        );
        assert_eq!(
            raft_why_not_scaling(2, Some(4)).as_deref(),
            Some("automatic_raft_group_growth_not_active")
        );
    }

    #[test]
    fn b44_registry_snapshot_lists_registered_streams() {
        let reg = CoordinationClosedLoopRegistry::new(CoordinationCeilings {
            max_queue_shards: Some(8),
            max_raft_groups: Some(4),
        });
        reg.register_stream("imports", 16);
        reg.record_auto_shard_tick("imports", 512, 2, 1, 16, AutoShardExpandResult::Idle);
        let snap = reg.snapshot(2);
        assert_eq!(snap.auto_shard_streams.len(), 1);
        assert_eq!(snap.auto_shard_streams[0].stream, "imports");
        assert_eq!(snap.auto_shard_streams[0].effective_max_shards, 8);
        assert_eq!(snap.coordination_raft_groups, 2);
    }
}
