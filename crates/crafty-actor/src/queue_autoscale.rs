//! Queue-depth-driven worker scaling ([job-queue](../../../docs/decisions/job-queue.md)).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crafty_proto::{
    AutoscalePolicyWire, MembershipAutoscalePolicyWire, QueueAutoscalePolicyCommand,
};

use crate::supervisor::ClusterState;
use crate::{ActorDirectory, ClusterScaleError, ExternalBacklog, JobQueue, effective_queue_depth};

/// Tunables for [`run_queue_autoscaler`].
#[derive(Debug, Clone)]
pub struct AutoscalePolicy {
    /// Worker actor group to scale (must be registered on the control plane).
    pub worker_group: String,
    /// Target pending jobs per worker instance.
    pub target_pending_per_worker: u64,
    /// Minimum worker instances for this group.
    pub min_workers: usize,
    /// Maximum worker instances (also capped by reachable nodes).
    pub max_workers: usize,
    /// Minimum time between scale decisions.
    pub cooldown: Duration,
    /// How often depth is sampled.
    pub poll_interval: Duration,
}

impl AutoscalePolicy {
    /// Encode for Meta-Raft / wire replication.
    #[must_use]
    pub fn to_wire(&self) -> AutoscalePolicyWire {
        AutoscalePolicyWire {
            worker_group: self.worker_group.clone(),
            target_pending_per_worker: self.target_pending_per_worker,
            min_workers: self.min_workers,
            max_workers: self.max_workers,
            cooldown_ms: u64::try_from(self.cooldown.as_millis()).unwrap_or(u64::MAX),
            poll_interval_ms: u64::try_from(self.poll_interval.as_millis()).unwrap_or(u64::MAX),
        }
    }

    /// Decode from wire / Meta-Raft metadata.
    #[must_use]
    pub fn from_wire(w: &AutoscalePolicyWire) -> Self {
        Self {
            worker_group: w.worker_group.clone(),
            target_pending_per_worker: w.target_pending_per_worker,
            min_workers: w.min_workers,
            max_workers: w.max_workers,
            cooldown: Duration::from_millis(w.cooldown_ms),
            poll_interval: Duration::from_millis(w.poll_interval_ms),
        }
    }
}

impl MembershipAutoscalePolicy {
    /// Encode for Meta-Raft / wire replication.
    #[must_use]
    pub fn to_wire(&self) -> MembershipAutoscalePolicyWire {
        MembershipAutoscalePolicyWire {
            pending_per_node_threshold: self.pending_per_node_threshold,
            max_nodes: self.max_nodes,
            cooldown_ms: u64::try_from(self.cooldown.as_millis()).unwrap_or(u64::MAX),
            poll_interval_ms: u64::try_from(self.poll_interval.as_millis()).unwrap_or(u64::MAX),
        }
    }

    /// Decode from wire / Meta-Raft metadata.
    #[must_use]
    pub fn from_wire(w: &MembershipAutoscalePolicyWire) -> Self {
        Self {
            pending_per_node_threshold: w.pending_per_node_threshold,
            max_nodes: w.max_nodes,
            cooldown: Duration::from_millis(w.cooldown_ms),
            poll_interval: Duration::from_millis(w.poll_interval_ms),
        }
    }
}

/// Live + Meta-Raft-backed autoscale policies keyed by queue stream name.
#[derive(Debug, Default)]
pub struct QueueAutoscaleRegistry {
    worker: Mutex<BTreeMap<String, AutoscalePolicy>>,
    membership: Mutex<BTreeMap<String, MembershipAutoscalePolicy>>,
}

impl QueueAutoscaleRegistry {
    /// Empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a committed Meta-Raft policy upsert.
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub fn apply(&self, command: &QueueAutoscalePolicyCommand) {
        if let Some(w) = &command.worker {
            self.worker
                .lock()
                .expect("poisoned")
                .insert(command.stream.clone(), AutoscalePolicy::from_wire(w));
        }
        if let Some(m) = &command.membership {
            self.membership.lock().expect("poisoned").insert(
                command.stream.clone(),
                MembershipAutoscalePolicy::from_wire(m),
            );
        }
    }

    /// Latest worker policy for `stream`, if any.
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    #[must_use]
    pub fn worker_policy(&self, stream: &str) -> Option<AutoscalePolicy> {
        self.worker.lock().expect("poisoned").get(stream).cloned()
    }

    /// Latest membership policy for `stream`, if any.
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    #[must_use]
    pub fn membership_policy(&self, stream: &str) -> Option<MembershipAutoscalePolicy> {
        self.membership
            .lock()
            .expect("poisoned")
            .get(stream)
            .cloned()
    }
}

/// Tunables for [`run_queue_membership_autoscaler`] — add cluster nodes when worker
/// scaling is capped by live VPS count ([job-queue](../../../docs/decisions/job-queue.md)).
#[derive(Debug, Clone)]
pub struct MembershipAutoscalePolicy {
    /// Request a new node when `(pending + leased) / live_nodes` exceeds this.
    pub pending_per_node_threshold: u64,
    /// Maximum cluster size this policy may grow to.
    pub max_nodes: usize,
    /// Minimum time between join attempts.
    pub cooldown: Duration,
    /// How often queue depth is sampled.
    pub poll_interval: Duration,
}

/// Leader-only loop: read queue metrics → scale worker group.
///
/// `scale` performs the actual placement (typically `ClusterControl::scale_cluster`).
#[allow(clippy::too_many_arguments)]
pub async fn run_queue_autoscaler<F, Fut>(
    queue: Arc<dyn JobQueue>,
    directory: Arc<ActorDirectory>,
    state: Arc<dyn ClusterState>,
    registry: Arc<QueueAutoscaleRegistry>,
    stream: String,
    fallback: AutoscalePolicy,
    backlog: Option<Arc<dyn ExternalBacklog>>,
    mut scale: F,
) where
    F: FnMut(usize) -> Fut + Send,
    Fut: std::future::Future<Output = Result<(), ClusterScaleError>> + Send,
{
    let fallback0 = fallback.clone();
    let mut last_scale = Instant::now()
        .checked_sub(fallback0.cooldown)
        .unwrap_or_else(Instant::now);
    let mut interval = tokio::time::interval(fallback0.poll_interval);
    loop {
        interval.tick().await;
        if !state.is_leader() {
            continue;
        }
        let policy = registry
            .worker_policy(&stream)
            .unwrap_or_else(|| fallback.clone());
        let depth = effective_queue_depth(queue.as_ref(), backlog.as_deref()).await;
        let reachable = state.reachable_nodes().len();
        if reachable == 0 {
            continue;
        }
        let current = directory.lookup(&policy.worker_group).len();
        let desired_raw = if policy.target_pending_per_worker == 0 {
            policy.min_workers
        } else {
            usize::try_from(depth.div_ceil(policy.target_pending_per_worker)).unwrap_or(usize::MAX)
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
    registry: Arc<QueueAutoscaleRegistry>,
    stream: String,
    fallback: MembershipAutoscalePolicy,
    backlog: Option<Arc<dyn ExternalBacklog>>,
    mut join: F,
) where
    F: FnMut() -> Fut + Send,
    Fut: std::future::Future<Output = Result<(), ClusterScaleError>> + Send,
{
    let fallback0 = fallback.clone();
    let mut last_join = Instant::now()
        .checked_sub(fallback0.cooldown)
        .unwrap_or_else(Instant::now);
    let mut interval = tokio::time::interval(fallback0.poll_interval);
    loop {
        interval.tick().await;
        if !state.is_leader() {
            continue;
        }
        let policy = registry
            .membership_policy(&stream)
            .unwrap_or_else(|| fallback.clone());
        let reachable = state.reachable_nodes().len();
        if reachable == 0 || reachable >= policy.max_nodes {
            continue;
        }
        let depth = effective_queue_depth(queue.as_ref(), backlog.as_deref()).await;
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
        nodes: Vec<crafty_proto::NodeId>,
    }

    impl ClusterState for MockState {
        fn is_leader(&self) -> bool {
            self.leader
        }

        fn live_nodes(&self) -> Vec<crafty_proto::NodeId> {
            self.nodes.clone()
        }

        fn leader_id(&self) -> Option<crafty_proto::NodeId> {
            self.leader.then_some(crafty_proto::NodeId(1))
        }

        fn reachable_nodes(&self) -> Vec<crafty_proto::NodeId> {
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
            nodes: vec![crafty_proto::NodeId(1), crafty_proto::NodeId(2)],
        });
        let joins = Arc::new(AtomicUsize::new(0));
        let joins_task = Arc::clone(&joins);
        let registry = Arc::new(QueueAutoscaleRegistry::new());
        let policy = MembershipAutoscalePolicy {
            pending_per_node_threshold: 5,
            max_nodes: 4,
            cooldown: Duration::from_millis(10),
            poll_interval: Duration::from_millis(5),
        };
        let task = tokio::spawn(async move {
            run_queue_membership_autoscaler(
                queue,
                state,
                registry,
                "jobs".to_string(),
                policy,
                None,
                move || {
                    let joins_task = Arc::clone(&joins_task);
                    async move {
                        joins_task.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
            )
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
