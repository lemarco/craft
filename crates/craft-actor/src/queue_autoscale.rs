//! Queue-depth-driven worker scaling ([job-queue](../../../docs/decisions/job-queue.md)).

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::supervisor::ClusterState;
use crate::{ActorDirectory, ClusterScaleError, JobQueue};

/// Tunables for [`run_queue_autoscaler`].
#[derive(Debug, Clone)]
pub struct AutoscalePolicy {
    /// Worker actor group to scale (must be registered on the control plane).
    pub worker_group: String,
    /// Target pending jobs per worker instance.
    pub target_pending_per_worker: u64,
    pub min_workers: usize,
    pub max_workers: usize,
    /// Minimum time between scale decisions.
    pub cooldown: Duration,
    /// How often depth is sampled.
    pub poll_interval: Duration,
}

/// Leader-only loop: read queue metrics → scale worker group.
///
/// `scale` performs the actual placement (typically `ClusterControl::scale_cluster`).
pub async fn run_queue_autoscaler<F, Fut>(
    queue: Arc<dyn JobQueue>,
    directory: Arc<ActorDirectory>,
    state: Arc<dyn ClusterState>,
    policy: AutoscalePolicy,
    mut scale: F,
) where
    F: FnMut(usize) -> Fut + Send,
    Fut: std::future::Future<Output = Result<(), ClusterScaleError>> + Send,
{
    let mut last_scale = Instant::now()
        .checked_sub(policy.cooldown)
        .unwrap_or_else(Instant::now);
    let mut interval = tokio::time::interval(policy.poll_interval);
    loop {
        interval.tick().await;
        if !state.is_leader() {
            continue;
        }
        let Ok(metrics) = queue.metrics().await else {
            continue;
        };
        let reachable = state.reachable_nodes().len();
        if reachable == 0 {
            continue;
        }
        let current = directory.lookup(&policy.worker_group).len();
        let desired_raw = if policy.target_pending_per_worker == 0 {
            policy.min_workers
        } else {
            (metrics.pending + metrics.leased).div_ceil(policy.target_pending_per_worker) as usize
        };
        let desired = desired_raw
            .clamp(policy.min_workers, policy.max_workers)
            .min(reachable);
        if desired == current {
            continue;
        }
        if last_scale.elapsed() < policy.cooldown {
            continue;
        }
        if scale(desired).await.is_ok() {
            last_scale = Instant::now();
        }
    }
}
