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

/// Tunables for [`run_queue_membership_autoscaler`] — add cluster nodes when worker
/// scaling is capped by live VPS count ([job-queue](../../../docs/decisions/job-queue.md)).
#[derive(Debug, Clone)]
pub struct MembershipAutoscalePolicy {
    /// Request a new node when `(pending + leased) / live_nodes` exceeds this.
    pub pending_per_node_threshold: u64,
    pub max_nodes: usize,
    pub cooldown: Duration,
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

/// Leader-only loop: when queue depth per live node exceeds a threshold and
/// worker autoscale is capped at `live_nodes`, invoke `join` to add a VPS.
pub async fn run_queue_membership_autoscaler<F, Fut>(
    queue: Arc<dyn JobQueue>,
    state: Arc<dyn ClusterState>,
    policy: MembershipAutoscalePolicy,
    mut join: F,
) where
    F: FnMut() -> Fut + Send,
    Fut: std::future::Future<Output = Result<(), ClusterScaleError>> + Send,
{
    let mut last_join = Instant::now()
        .checked_sub(policy.cooldown)
        .unwrap_or_else(Instant::now);
    let mut interval = tokio::time::interval(policy.poll_interval);
    loop {
        interval.tick().await;
        if !state.is_leader() {
            continue;
        }
        let reachable = state.reachable_nodes().len();
        if reachable == 0 || reachable >= policy.max_nodes {
            continue;
        }
        let Ok(metrics) = queue.metrics().await else {
            continue;
        };
        let depth = metrics.pending + metrics.leased;
        let per_node = depth / reachable as u64;
        if per_node <= policy.pending_per_node_threshold {
            continue;
        }
        if last_join.elapsed() < policy.cooldown {
            continue;
        }
        if join().await.is_ok() {
            last_join = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::InMemoryJobQueue;

    struct MockState {
        leader: bool,
        nodes: Vec<craft_proto::NodeId>,
    }

    impl ClusterState for MockState {
        fn is_leader(&self) -> bool {
            self.leader
        }

        fn live_nodes(&self) -> Vec<craft_proto::NodeId> {
            self.nodes.clone()
        }

        fn leader_id(&self) -> Option<craft_proto::NodeId> {
            self.leader.then_some(craft_proto::NodeId(1))
        }

        fn reachable_nodes(&self) -> Vec<craft_proto::NodeId> {
            self.nodes.clone()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn membership_autoscale_invokes_join_when_depth_per_node_high() {
        let queue: Arc<dyn JobQueue> = Arc::new(InMemoryJobQueue::new(Duration::from_secs(30)));
        for i in 0..20u8 {
            queue.enqueue(&[i]).await.unwrap();
        }
        let state = Arc::new(MockState {
            leader: true,
            nodes: vec![craft_proto::NodeId(1), craft_proto::NodeId(2)],
        });
        let joins = Arc::new(AtomicUsize::new(0));
        let joins_task = Arc::clone(&joins);
        let policy = MembershipAutoscalePolicy {
            pending_per_node_threshold: 5,
            max_nodes: 4,
            cooldown: Duration::from_millis(10),
            poll_interval: Duration::from_millis(5),
        };
        let task = tokio::spawn(async move {
            run_queue_membership_autoscaler(queue, state, policy, move || {
                let joins_task = Arc::clone(&joins_task);
                async move {
                    joins_task.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await;
        });
        for _ in 0..10 {
            tokio::time::advance(Duration::from_millis(10)).await;
            tokio::task::yield_now().await;
        }
        task.abort();
        let _ = task.await;
        assert!(joins.load(Ordering::SeqCst) >= 1);
    }
}
