//! External backlog port — claim from a source of truth, top up the job queue,
//! settle outcomes back ([external-backlog](../../../docs/decisions/external-backlog.md)).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::backlog_settle_outbox::{
    BacklogSettleOutbox, BacklogSettleOutboxOpts, push_backlog_settle,
};
use crate::{EnqueueOptions, JobId, JobQueue};
use trembita_actor_store::BoxFuture;
use trembita_proto::QueueReplicateOp;
use trembita_runtime::ClusterState;

/// Why an external backlog operation failed.
#[derive(Debug, thiserror::Error)]
pub enum BacklogError {
    /// Backend (database, network) error.
    #[error("backlog backend error: {0}")]
    Backend(String),
}

/// Terminal outcome reported to the external source after job queue processing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Settlement {
    /// Handler acked successfully.
    ///
    /// `attempts` is the queue job's stored attempt counter at ack time (prior
    /// failed deliveries only; `0` on first-try success). Adapters should
    /// ignore stale `Done` when the counter does not match the in-flight
    /// generation (see [external-backlog](../../../docs/decisions/external-backlog.md)).
    Done {
        /// Queue attempt counter at successful ack ([`JobQueue::peek_lease_meta`]).
        attempts: u32,
    },
    /// Failed but will retry (nack / lease timeout, not yet dead-letter).
    Failed {
        /// Attempt count after this failure.
        attempts: u32,
        /// Human-readable error when available.
        error: String,
    },
    /// Moved to dead letter — no more retries.
    DeadLettered {
        /// Final attempt count.
        attempts: u32,
        /// Human-readable error when available.
        error: String,
    },
}

/// One item claimed from an external backlog for enqueue into the job queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacklogItem {
    /// Stable idempotency key — becomes [`EnqueueOptions::dedup_key`].
    pub key: Vec<u8>,
    /// Opaque job body for the consumer.
    pub payload: Vec<u8>,
    /// Lease priority (higher first).
    pub priority: u8,
}

/// How the backlog feeder sizes the in-flight window against consumer capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerCount {
    /// `reachable_nodes × per_node` — recomputed each poll from [`ClusterState`].
    ///
    /// When registered via `JobOpts::backlog`, `per_node` is taken from local
    /// `.instances()` at boot.
    Live {
        /// Consumer loops registered on each node for this stream.
        per_node: u64,
    },
    /// Fixed cluster-wide consumer count (explicit opt-out of live sizing).
    Fixed(u64),
}

impl Default for ConsumerCount {
    fn default() -> Self {
        Self::Live { per_node: 1 }
    }
}

impl ConsumerCount {
    /// Resolve the effective cluster-wide consumer count for window sizing.
    #[must_use]
    pub fn resolve(self, state: &dyn ClusterState) -> u64 {
        match self {
            Self::Live { per_node } => {
                let nodes = u64::try_from(state.reachable_nodes().len())
                    .unwrap_or(u64::MAX)
                    .max(1);
                nodes.saturating_mul(per_node.max(1))
            }
            Self::Fixed(n) => n.max(1),
        }
    }
}

/// Tunables for [`run_backlog_feeder`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacklogFeedOpts {
    /// Target in-flight jobs per consumer instance (`pending + leased` window).
    pub pending_target_per_consumer: u64,
    /// How often the leader re-evaluates the in-flight window.
    pub poll_interval: Duration,
    /// Upper bound on items claimed per poll.
    pub max_claim_batch: usize,
    /// Consumer capacity for window sizing — live by default.
    pub consumer_instances: ConsumerCount,
}

impl Default for BacklogFeedOpts {
    fn default() -> Self {
        Self {
            pending_target_per_consumer: 2,
            poll_interval: Duration::from_secs(1),
            max_claim_batch: 64,
            consumer_instances: ConsumerCount::default(),
        }
    }
}

impl BacklogFeedOpts {
    /// Target pending+leased jobs per consumer loop.
    #[must_use]
    pub fn pending_target_per_consumer(mut self, n: u64) -> Self {
        self.pending_target_per_consumer = n.max(1);
        self
    }

    /// Leader poll interval.
    #[must_use]
    pub fn poll(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Max items to claim in one poll.
    #[must_use]
    pub fn max_claim_batch(mut self, n: usize) -> Self {
        self.max_claim_batch = n.max(1);
        self
    }

    /// Consumer capacity for in-flight window sizing.
    #[must_use]
    pub fn consumer_instances(mut self, count: ConsumerCount) -> Self {
        self.consumer_instances = count;
        self
    }
}

/// Source-of-truth backlog outside the job queue (Postgres table, existing job store, …).
pub trait ExternalBacklog: Send + Sync {
    /// Outstanding demand in the external store — feeds autoscaling when wired.
    fn depth(&self) -> BoxFuture<'_, Result<u64, BacklogError>>;

    /// Claim up to `max` items. The implementation owns exclusion (`SKIP LOCKED`, CAS, …).
    fn claim(&self, max: usize) -> BoxFuture<'_, Result<Vec<BacklogItem>, BacklogError>>;

    /// Terminal outcome — called after ack or dead-letter before the job queue row is dropped.
    fn settle(&self, key: &[u8], outcome: Settlement) -> BoxFuture<'_, Result<(), BacklogError>>;
}

/// Maps queue stream names to external backlogs (settlement + autoscale wiring).
#[derive(Default)]
pub struct BacklogRegistry {
    inner: Mutex<HashMap<String, Arc<dyn ExternalBacklog>>>,
}

impl BacklogRegistry {
    /// Empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `stream` → `backlog`.
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub fn register(&self, stream: impl Into<String>, backlog: Arc<dyn ExternalBacklog>) {
        self.inner
            .lock()
            .expect("poisoned")
            .insert(stream.into(), backlog);
    }

    /// Lookup backlog for a stream.
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    #[must_use]
    pub fn get(&self, stream: &str) -> Option<Arc<dyn ExternalBacklog>> {
        self.inner.lock().expect("poisoned").get(stream).cloned()
    }
}

/// Effective queue depth for autoscaling: external `depth()` when registered, else job queue metrics.
pub async fn effective_queue_depth(
    queue: &dyn JobQueue,
    backlog: Option<&dyn ExternalBacklog>,
) -> u64 {
    if let Some(b) = backlog {
        return b.depth().await.unwrap_or(0);
    }
    queue
        .metrics()
        .await
        .map_or(0, |m| m.pending.saturating_add(m.leased))
}

/// Map job queue terminal attempt metadata to an external settle outcome.
#[must_use]
pub fn terminal_backlog_outcome(
    attempts: u32,
    dead_letter: bool,
    error: impl Into<String>,
) -> BacklogSettleOutcome {
    let error = error.into();
    if dead_letter {
        BacklogSettleOutcome::DeadLettered { attempts, error }
    } else {
        BacklogSettleOutcome::Failed { attempts, error }
    }
}

/// Emit settle events for `Nack` / `Reclaim` replication ops (lease timeout, nack, …).
pub async fn emit_backlog_settle_for_terminal_ops(
    stream: &str,
    queue: &dyn JobQueue,
    outbox: Option<&dyn BacklogSettleOutbox>,
    ops: &[QueueReplicateOp],
    error: &str,
) {
    for op in ops {
        let (job_id, attempts, dead_letter) = match op {
            QueueReplicateOp::Nack {
                job_id,
                attempts,
                dead_letter,
                ..
            }
            | QueueReplicateOp::Reclaim {
                job_id,
                attempts,
                dead_letter,
                ..
            } => (*job_id, *attempts, *dead_letter),
            _ => continue,
        };
        let dedup_key = queue
            .job_status(JobId(job_id))
            .await
            .ok()
            .flatten()
            .and_then(|status| status.dedup_key);
        let Some(dedup_key) = dedup_key else {
            continue;
        };
        push_backlog_settle(
            outbox,
            BacklogSettleEvent {
                stream: stream.to_owned(),
                dedup_key: Some(dedup_key),
                outcome: terminal_backlog_outcome(attempts, dead_letter, error),
            },
        );
    }
}

/// Leader-only loop: keep job queue `(pending + leased)` near `target × consumers` by claiming
/// from [`ExternalBacklog`] and enqueueing with `dedup_key = item.key`.
pub async fn run_backlog_feeder(
    stream: String,
    queue: Arc<dyn JobQueue>,
    backlog: Arc<dyn ExternalBacklog>,
    state: Arc<dyn ClusterState>,
    opts: BacklogFeedOpts,
    settle_outbox: Option<Arc<dyn BacklogSettleOutbox>>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut session = trembita_runtime::LeaderSession::new();
    let mut interval = tokio::time::interval(opts.poll_interval);
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = stop.changed() => {
                if *stop.borrow() {
                    break;
                }
            }
        }
        if *stop.borrow() {
            break;
        }
        if !session.gate(state.as_ref()).is_active() {
            continue;
        }
        let consumer_instances = opts.consumer_instances.resolve(state.as_ref());
        let target_in_flight = opts
            .pending_target_per_consumer
            .saturating_mul(consumer_instances);
        let Ok(metrics) = queue.metrics().await else {
            continue;
        };
        let in_flight = metrics.pending.saturating_add(metrics.leased);
        if in_flight >= target_in_flight {
            continue;
        }
        let need = usize::try_from(target_in_flight.saturating_sub(in_flight))
            .unwrap_or(usize::MAX)
            .min(opts.max_claim_batch);
        if need == 0 {
            continue;
        }
        let Ok(items) = backlog.claim(need).await else {
            continue;
        };
        for item in items {
            let options = EnqueueOptions {
                priority: item.priority,
                dedup_key: Some(item.key),
                ..EnqueueOptions::default()
            };
            let Ok((_, replicate_ops)) =
                queue.enqueue_opts_replicated(&item.payload, options).await
            else {
                continue;
            };
            if let Some(outbox) = settle_outbox.as_deref() {
                emit_backlog_settle_for_terminal_ops(
                    &stream,
                    queue.as_ref(),
                    Some(outbox),
                    &replicate_ops,
                    "reclaim",
                )
                .await;
            }
        }
    }
}

/// Leader-only loop: drain the settle outbox into [`ExternalBacklog::settle`] with retry.
pub async fn run_backlog_settle_drainer(
    registry: Arc<BacklogRegistry>,
    outbox: Arc<dyn BacklogSettleOutbox>,
    state: Arc<dyn ClusterState>,
    opts: BacklogSettleOutboxOpts,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut session = trembita_runtime::LeaderSession::new();
    let mut interval = tokio::time::interval(opts.poll_interval);
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = stop.changed() => {
                if *stop.borrow() {
                    break;
                }
            }
        }
        if *stop.borrow() || !session.gate(state.as_ref()).is_active() {
            continue;
        }
        let Ok(batch) = outbox.list_pending(opts.max_batch) else {
            continue;
        };
        for (id, ev) in batch {
            let Some(backlog) = registry.get(&ev.stream) else {
                let _ = outbox.remove(id);
                continue;
            };
            let Some(key) = ev.dedup_key else {
                let _ = outbox.remove(id);
                continue;
            };
            let settlement = match ev.outcome {
                BacklogSettleOutcome::Done { attempts } => Settlement::Done { attempts },
                BacklogSettleOutcome::Failed { attempts, error } => {
                    Settlement::Failed { attempts, error }
                }
                BacklogSettleOutcome::DeadLettered { attempts, error } => {
                    Settlement::DeadLettered { attempts, error }
                }
            };
            if let Ok(()) = backlog.settle(&key, settlement).await {
                let _ = outbox.remove(id);
            }
        }
    }
}

/// Terminal job queue outcome forwarded to [`ExternalBacklog::settle`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BacklogSettleOutcome {
    /// Successful ack.
    Done {
        /// Queue attempt counter at ack ([`JobQueue::peek_lease_meta`]).
        attempts: u32,
    },
    /// Nack / reclaim with retries remaining.
    Failed {
        /// Attempt count after this failure.
        attempts: u32,
        /// Error summary.
        error: String,
    },
    /// Dead letter — no more retries.
    DeadLettered {
        /// Final attempt count.
        attempts: u32,
        /// Error summary.
        error: String,
    },
}

/// Event emitted by the queue leader when a job with a dedup key reaches a terminal state.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BacklogSettleEvent {
    /// Queue stream.
    pub stream: String,
    /// Idempotency key from enqueue (`ExternalBacklog` claim key).
    pub dedup_key: Option<Vec<u8>>,
    /// Terminal outcome.
    pub outcome: BacklogSettleOutcome,
}

/// In-memory external backlog for tests and single-node dev.
#[derive(Default)]
pub struct InMemoryExternalBacklog {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    pending: VecDeque<BacklogItem>,
    /// Attempt counter for each claimed key (matches external row at claim time).
    claimed: BTreeMap<Vec<u8>, u32>,
    settled: BTreeMap<Vec<u8>, Settlement>,
}

impl InMemoryExternalBacklog {
    /// Empty backlog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed pending work (test helper).
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub fn push(&self, item: BacklogItem) {
        self.inner.lock().expect("poisoned").pending.push_back(item);
    }

    /// Settled outcomes keyed by item key (test helper).
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    #[must_use]
    pub fn settled(&self) -> BTreeMap<Vec<u8>, Settlement> {
        self.inner.lock().expect("poisoned").settled.clone()
    }
}

impl ExternalBacklog for InMemoryExternalBacklog {
    fn depth(&self) -> BoxFuture<'_, Result<u64, BacklogError>> {
        Box::pin(async move {
            let inner = self
                .inner
                .lock()
                .map_err(|_| BacklogError::Backend("poisoned InMemoryExternalBacklog".into()))?;
            let n = u64::try_from(inner.pending.len().saturating_add(inner.claimed.len()))
                .unwrap_or(u64::MAX);
            Ok(n)
        })
    }

    fn claim(&self, max: usize) -> BoxFuture<'_, Result<Vec<BacklogItem>, BacklogError>> {
        Box::pin(async move {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| BacklogError::Backend("poisoned InMemoryExternalBacklog".into()))?;
            let mut out = Vec::new();
            for _ in 0..max {
                let Some(item) = inner.pending.pop_front() else {
                    break;
                };
                inner.claimed.insert(item.key.clone(), 0);
                out.push(item);
            }
            Ok(out)
        })
    }

    fn settle(&self, key: &[u8], outcome: Settlement) -> BoxFuture<'_, Result<(), BacklogError>> {
        let key = key.to_vec();
        Box::pin(async move {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| BacklogError::Backend("poisoned InMemoryExternalBacklog".into()))?;
            match &outcome {
                Settlement::Done { attempts } => {
                    if let Some(claimed_attempts) = inner.claimed.get(&key) {
                        if *claimed_attempts != *attempts {
                            return Ok(());
                        }
                        inner.claimed.remove(&key);
                        inner.settled.insert(key, outcome);
                        return Ok(());
                    }
                    if matches!(inner.settled.get(&key), Some(Settlement::Done { .. })) {
                        return Ok(());
                    }
                    inner.settled.insert(key, outcome);
                }
                Settlement::Failed { attempts, .. } | Settlement::DeadLettered { attempts, .. } => {
                    if let Some(claimed_attempts) = inner.claimed.get_mut(&key) {
                        *claimed_attempts = *attempts;
                    }
                    inner.claimed.remove(&key);
                    inner.settled.insert(key, outcome);
                }
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use trembita_proto::NodeId;

    use super::*;
    use crate::InMemoryBacklogSettleOutbox;
    use crate::{EnqueueOptions, InMemoryJobQueue, JobQueue};
    use trembita_runtime::ClusterState;

    struct MockState {
        leader: bool,
        reachable: Vec<NodeId>,
    }

    impl ClusterState for MockState {
        fn is_leader(&self) -> bool {
            self.leader
        }

        fn live_nodes(&self) -> Vec<NodeId> {
            self.reachable.clone()
        }

        fn leader_id(&self) -> Option<NodeId> {
            self.leader.then_some(NodeId(1))
        }

        fn reachable_nodes(&self) -> Vec<NodeId> {
            self.reachable.clone()
        }
    }

    #[tokio::test]
    async fn feeder_tops_up_in_flight_window() {
        let backlog = Arc::new(InMemoryExternalBacklog::new());
        for i in 0..5 {
            backlog.push(BacklogItem {
                key: format!("k{i}").into_bytes(),
                payload: format!("p{i}").into_bytes(),
                priority: 0,
            });
        }
        let queue = Arc::new(InMemoryJobQueue::new(Duration::from_secs(30)));
        let state = Arc::new(MockState {
            leader: true,
            reachable: vec![NodeId(1)],
        });
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let feeder = tokio::spawn(run_backlog_feeder(
            "jobs".into(),
            Arc::clone(&queue) as Arc<dyn JobQueue>,
            backlog.clone(),
            state,
            BacklogFeedOpts::default()
                .pending_target_per_consumer(2)
                .consumer_instances(ConsumerCount::Fixed(1))
                .poll(Duration::from_millis(20)),
            None,
            stop_rx,
        ));
        tokio::time::sleep(Duration::from_millis(150)).await;
        stop_tx.send(true).unwrap();
        feeder.await.unwrap();
        let metrics = queue.metrics().await.unwrap();
        assert!(metrics.pending + metrics.leased <= 2);
        assert!(metrics.pending + metrics.leased >= 1);
    }

    #[tokio::test]
    async fn feeder_live_consumer_count_tracks_reachable_nodes() {
        let backlog = Arc::new(InMemoryExternalBacklog::new());
        for i in 0..12 {
            backlog.push(BacklogItem {
                key: format!("k{i}").into_bytes(),
                payload: format!("p{i}").into_bytes(),
                priority: 0,
            });
        }
        let queue = Arc::new(InMemoryJobQueue::new(Duration::from_secs(30)));
        let state = Arc::new(MockState {
            leader: true,
            reachable: vec![NodeId(1), NodeId(2), NodeId(3)],
        });
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let feeder = tokio::spawn(run_backlog_feeder(
            "jobs".into(),
            Arc::clone(&queue) as Arc<dyn JobQueue>,
            backlog.clone(),
            state,
            BacklogFeedOpts::default()
                .pending_target_per_consumer(2)
                .consumer_instances(ConsumerCount::Live { per_node: 2 })
                .poll(Duration::from_millis(20)),
            None,
            stop_rx,
        ));
        tokio::time::sleep(Duration::from_millis(150)).await;
        stop_tx.send(true).unwrap();
        feeder.await.unwrap();
        let metrics = queue.metrics().await.unwrap();
        // 3 reachable nodes × 2 instances × 2 pending target = 12
        assert!(metrics.pending + metrics.leased <= 12);
        assert!(metrics.pending + metrics.leased >= 6);
    }

    #[tokio::test]
    async fn in_memory_settle_tracks_outcomes() {
        let backlog = Arc::new(InMemoryExternalBacklog::new());
        backlog.push(BacklogItem {
            key: b"order-1".to_vec(),
            payload: b"work".to_vec(),
            priority: 0,
        });
        let claimed = backlog.claim(1).await.unwrap();
        assert_eq!(claimed.len(), 1);
        backlog
            .settle(b"order-1", Settlement::Done { attempts: 0 })
            .await
            .unwrap();
        assert_eq!(backlog.depth().await.unwrap(), 0);
        assert_eq!(
            backlog.settled().get(b"order-1".as_slice()),
            Some(&Settlement::Done { attempts: 0 })
        );
    }

    #[tokio::test]
    async fn terminal_ops_settles_from_reclaim_op() {
        let queue = Arc::new(InMemoryJobQueue::new(Duration::from_secs(30)));
        let key = b"row-1".to_vec();
        let (job_id, _) = queue
            .enqueue_opts_replicated(
                b"payload",
                EnqueueOptions {
                    dedup_key: Some(key.clone()),
                    ..EnqueueOptions::default()
                },
            )
            .await
            .unwrap();

        let reclaim_ops = [QueueReplicateOp::Reclaim {
            lease_id: 1,
            job_id: job_id.0,
            attempts: 1,
            dead_letter: false,
            not_before_ms: 0,
        }];

        let backlog = Arc::new(InMemoryExternalBacklog::new());
        let registry = Arc::new(BacklogRegistry::new());
        registry.register("imports", backlog.clone());
        let outbox: Arc<dyn BacklogSettleOutbox> = Arc::new(InMemoryBacklogSettleOutbox::new());
        let state = Arc::new(MockState {
            leader: true,
            reachable: vec![NodeId(1)],
        });
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let drainer = tokio::spawn(run_backlog_settle_drainer(
            registry,
            Arc::clone(&outbox),
            state,
            BacklogSettleOutboxOpts::default().poll(Duration::from_millis(20)),
            stop_rx,
        ));
        emit_backlog_settle_for_terminal_ops(
            "imports",
            queue.as_ref(),
            Some(outbox.as_ref()),
            &reclaim_ops,
            "reclaim",
        )
        .await;

        for _ in 0..50 {
            if backlog.settled().contains_key(key.as_slice()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        stop_tx.send(true).unwrap();
        drainer.await.unwrap();

        assert_eq!(
            backlog.settled().get(key.as_slice()),
            Some(&Settlement::Failed {
                attempts: 1,
                error: "reclaim".into(),
            })
        );
        assert_eq!(outbox.pending_count().unwrap(), 0);
    }

    #[tokio::test]
    async fn stale_done_settle_ignored_when_attempts_mismatch() {
        let backlog = Arc::new(InMemoryExternalBacklog::new());
        backlog.push(BacklogItem {
            key: b"row-1".to_vec(),
            payload: b"work".to_vec(),
            priority: 0,
        });
        backlog.claim(1).await.unwrap();
        backlog
            .settle(b"row-1", Settlement::Done { attempts: 1 })
            .await
            .unwrap();
        assert!(backlog.settled().is_empty());
        assert_eq!(backlog.depth().await.unwrap(), 1);
        backlog
            .settle(b"row-1", Settlement::Done { attempts: 0 })
            .await
            .unwrap();
        assert_eq!(
            backlog.settled().get(b"row-1".as_slice()),
            Some(&Settlement::Done { attempts: 0 })
        );
    }

    #[tokio::test]
    async fn outbox_drainer_retries_after_settle_failure() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct FlakyBacklog {
            inner: Arc<InMemoryExternalBacklog>,
            remaining_failures: AtomicUsize,
        }

        impl ExternalBacklog for FlakyBacklog {
            fn depth(&self) -> BoxFuture<'_, Result<u64, BacklogError>> {
                self.inner.depth()
            }

            fn claim(&self, max: usize) -> BoxFuture<'_, Result<Vec<BacklogItem>, BacklogError>> {
                self.inner.claim(max)
            }

            fn settle(
                &self,
                key: &[u8],
                outcome: Settlement,
            ) -> BoxFuture<'_, Result<(), BacklogError>> {
                if self.remaining_failures.fetch_sub(1, Ordering::SeqCst) > 0 {
                    return Box::pin(async { Err(BacklogError::Backend("transient".into())) });
                }
                self.inner.settle(key, outcome)
            }
        }

        let backlog = Arc::new(FlakyBacklog {
            inner: Arc::new(InMemoryExternalBacklog::new()),
            remaining_failures: AtomicUsize::new(2),
        });
        let registry = Arc::new(BacklogRegistry::new());
        registry.register("imports", backlog.clone());
        let outbox: Arc<dyn BacklogSettleOutbox> = Arc::new(InMemoryBacklogSettleOutbox::new());
        outbox
            .push(BacklogSettleEvent {
                stream: "imports".into(),
                dedup_key: Some(b"k".to_vec()),
                outcome: BacklogSettleOutcome::Done { attempts: 0 },
            })
            .unwrap();
        assert_eq!(outbox.pending_count().unwrap(), 1);

        let state = Arc::new(MockState {
            leader: true,
            reachable: vec![NodeId(1)],
        });
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let drainer = tokio::spawn(run_backlog_settle_drainer(
            registry,
            Arc::clone(&outbox),
            state,
            BacklogSettleOutboxOpts::default().poll(Duration::from_millis(10)),
            stop_rx,
        ));

        for _ in 0..100 {
            if backlog.inner.settled().contains_key(b"k".as_slice()) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        stop_tx.send(true).unwrap();
        drainer.await.unwrap();

        assert_eq!(
            backlog.inner.settled().get(b"k".as_slice()),
            Some(&Settlement::Done { attempts: 0 })
        );
        assert_eq!(outbox.pending_count().unwrap(), 0);
    }
}
