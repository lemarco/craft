//! Adaptive physical shard expansion for hot logical queues ([job-queue](../../../docs/decisions/job-queue.md)).

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use trembita_runtime::{LeaderLoopOpts, run_leader_loop};

use crate::JobQueue;
use crate::coordination_closed_loop::{
    AutoShardExpandResult, CoordinationClosedLoopRegistry, effective_max_shards,
};
use crate::queue_service::QueueService;

/// When sustained `pending` exceeds this, the leader may add a physical shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoShardPolicy {
    /// Minimum logical `pending` before counting pressure ticks.
    pub pending_threshold: u64,
    /// Consecutive leader ticks above threshold before expanding.
    pub ticks_above_threshold: u32,
    /// Hard cap on physical shards (`1` = no expansion beyond boot layout).
    pub max_shards: usize,
    /// Leader sampling interval.
    pub poll_interval: Duration,
}

impl Default for AutoShardPolicy {
    fn default() -> Self {
        Self {
            pending_threshold: 512,
            ticks_above_threshold: 3,
            max_shards: 8,
            poll_interval: Duration::from_secs(5),
        }
    }
}

impl AutoShardPolicy {
    /// B-37 **`jobs_backlog`** profile — expand sooner, allow more physical shards.
    #[must_use]
    pub fn jobs_backlog_growth() -> Self {
        Self {
            pending_threshold: 256,
            ticks_above_threshold: 2,
            max_shards: 16,
            poll_interval: Duration::from_secs(5),
        }
    }

    /// B-37 **`full`** profile — default depth with a higher shard ceiling.
    #[must_use]
    pub fn full_growth() -> Self {
        Self {
            pending_threshold: 512,
            ticks_above_threshold: 3,
            max_shards: 12,
            poll_interval: Duration::from_secs(5),
        }
    }
}

/// Physical stream template for lazy follower open (`{logical}~{n}`).
#[derive(Debug, Clone)]
pub struct AutoShardStreamSpec {
    /// Logical queue name (also registered in [`crate::ShardedJobQueue`]).
    pub logical: String,
    /// Node data directory (`queue-{logical}~{n}.redb` files).
    pub data_dir: PathBuf,
    /// Lease timeout for lazily opened physical shards.
    pub lease_timeout: Duration,
    /// Leader prefetch depth for new physical shards.
    pub prefetch: usize,
    /// Default max attempts on lazily opened shards.
    pub default_max_attempts: u32,
    /// Expansion thresholds and caps.
    pub policy: AutoShardPolicy,
}

/// Leader loop: sample depth and expand sharded queues under sustained pressure.
pub async fn run_queue_auto_shard_coordinator(
    state: Arc<dyn trembita_runtime::ClusterState>,
    queue: Arc<dyn JobQueue>,
    service: Arc<QueueService>,
    spec: AutoShardStreamSpec,
    closed_loop: Arc<CoordinationClosedLoopRegistry>,
    stop: tokio::sync::watch::Receiver<bool>,
) {
    let max_queue_shards = closed_loop.ceilings().max_queue_shards;
    closed_loop.register_stream(&spec.logical, spec.policy.max_shards);
    let hot_ticks = Arc::new(Mutex::new(0u32));
    let opts = LeaderLoopOpts::new(spec.policy.poll_interval).with_name("queue_auto_shard");
    let _ = run_leader_loop(state, opts, stop, move |gate| {
        let queue = Arc::clone(&queue);
        let service = Arc::clone(&service);
        let spec = spec.clone();
        let hot_ticks = Arc::clone(&hot_ticks);
        let closed_loop = Arc::clone(&closed_loop);
        async move {
            if !gate.is_active() {
                return;
            }
            let Ok(metrics) = queue.metrics().await else {
                return;
            };
            let shard_count = service.sharded_logical_shard_count(&spec.logical);
            let expand = tick_auto_shard(
                &service,
                &spec,
                metrics.pending,
                &hot_ticks,
                max_queue_shards,
            )
            .await;
            let ticks = *hot_ticks.lock().expect("poisoned");
            closed_loop.record_auto_shard_tick(
                &spec.logical,
                metrics.pending,
                ticks,
                shard_count,
                spec.policy.max_shards,
                expand,
            );
        }
    })
    .await;
}

async fn tick_auto_shard(
    service: &QueueService,
    spec: &AutoShardStreamSpec,
    pending: u64,
    hot_ticks: &Mutex<u32>,
    max_queue_shards: Option<usize>,
) -> AutoShardExpandResult {
    let effective_max = effective_max_shards(spec.policy.max_shards, max_queue_shards);
    let mut ticks = hot_ticks.lock().expect("poisoned");
    if pending >= spec.policy.pending_threshold {
        *ticks = ticks.saturating_add(1);
    } else {
        *ticks = 0;
        return AutoShardExpandResult::Idle;
    }
    if *ticks < spec.policy.ticks_above_threshold {
        return AutoShardExpandResult::Idle;
    }
    *ticks = 0;
    drop(ticks);
    match service.try_expand_sharded_stream(
        &spec.logical,
        &spec.data_dir,
        spec.lease_timeout,
        spec.prefetch,
        spec.default_max_attempts,
        effective_max,
    ) {
        Ok(()) => AutoShardExpandResult::Expanded,
        Err(e) if e.to_string().contains("max_shards") => AutoShardExpandResult::AtCeiling,
        Err(e) => AutoShardExpandResult::ExpandFailed(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_shard_policy_defaults_are_sane() {
        let p = AutoShardPolicy::default();
        assert!(p.max_shards >= 2);
        assert!(p.pending_threshold > 0);
    }

    /// B-37 — leader thresholds documented in [capabilities § B-37](../../../docs/scenarios/capabilities.md#coordination-growth-presets-b-37).
    #[test]
    fn b37_growth_preset_auto_shard_policies_table() {
        struct Row {
            label: &'static str,
            policy: AutoShardPolicy,
            pending: u64,
            max_shards: usize,
        }
        let rows = [
            Row {
                label: "jobs_backlog",
                policy: AutoShardPolicy::jobs_backlog_growth(),
                pending: 256,
                max_shards: 16,
            },
            Row {
                label: "full",
                policy: AutoShardPolicy::full_growth(),
                pending: 512,
                max_shards: 12,
            },
        ];
        for row in rows {
            assert_eq!(row.policy.pending_threshold, row.pending, "{}", row.label);
            assert_eq!(row.policy.max_shards, row.max_shards, "{}", row.label);
        }
    }
}
