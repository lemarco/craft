//! Durable job backlog port ([job-queue](../../../docs/decisions/job-queue.md)).
//!
//! [`JobQueue`] is tier C messaging: shared async work with `lease` / `ack`,
//! distinct from actor mailboxes (tier B) and Raft (tier A). [`InMemoryJobQueue`]
//! backs tests and single-node dev; production uses [`RedbJobQueue`](super::redb_queue::RedbJobQueue).

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use craft_proto::NodeId;
pub use craft_proto::QueueReplicateOp;

pub use crate::store::BoxFuture;

/// Opaque job identifier (monotonic per queue stream).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobId(pub u64);

/// Opaque lease token returned by [`JobQueue::lease`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeaseId(pub u64);

/// Identifies a queue consumer (actor instance on a node).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkerId {
    pub node: NodeId,
    pub instance: u32,
}

/// A job handed to a worker under lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeasedJob {
    pub lease_id: LeaseId,
    pub job_id: JobId,
    pub payload: Vec<u8>,
}

/// Options for [`JobQueue::enqueue_opts`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EnqueueOptions {
    /// Higher priority jobs are leased first (default `0`).
    pub priority: u8,
    /// Do not lease before this unix timestamp in milliseconds (`None` = now).
    pub not_before_ms: Option<u64>,
    /// Routes to a shard when using [`ShardedJobQueue`](super::sharded_queue::ShardedJobQueue).
    pub shard_key: Option<Vec<u8>>,
    /// Client-supplied idempotency token; duplicate enqueues return the same [`JobId`].
    pub dedup_key: Option<Vec<u8>>,
}

impl EnqueueOptions {
    /// Job with elevated priority.
    #[must_use]
    pub fn priority(priority: u8) -> Self {
        Self {
            priority,
            ..Self::default()
        }
    }

    /// Job that becomes visible after `delay` from enqueue time.
    #[must_use]
    pub fn delayed(delay: Duration) -> Self {
        let not_before_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX)
            + u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);
        Self {
            not_before_ms: Some(not_before_ms),
            ..Self::default()
        }
    }

    /// Job with a client idempotency key (safe enqueue retries).
    #[must_use]
    pub fn dedup_key(key: impl Into<Vec<u8>>) -> Self {
        Self {
            dedup_key: Some(key.into()),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueueMetrics {
    pub pending: u64,
    pub leased: u64,
    pub oldest_pending_age: Duration,
}

/// Why a queue operation failed.
#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    /// The backend (disk, network) reported an error.
    #[error("queue backend error: {0}")]
    Backend(String),
    /// `lease_id` is unknown or not owned by this worker.
    #[error("invalid or expired lease")]
    InvalidLease,
    /// Payload could not be encoded/decoded at the store boundary.
    #[error("queue codec error: {0}")]
    Codec(String),
}

/// Replication batch produced by a leader mutation.
pub type QueueReplicationOps = Vec<QueueReplicateOp>;

/// Shared async work buffer — at-least-once with lease + visibility timeout.
///
/// Object-safe (`BoxFuture`) so runtime code can hold `Arc<dyn JobQueue>`.
pub trait JobQueue: Send + Sync {
    /// Append a job; returns the assigned [`JobId`].
    fn enqueue<'a>(&'a self, payload: &'a [u8]) -> BoxFuture<'a, Result<JobId, QueueError>> {
        Box::pin(async move { self.enqueue_opts(payload, EnqueueOptions::default()).await })
    }

    /// Append with priority, delay, and optional shard routing key.
    fn enqueue_opts<'a>(
        &'a self,
        payload: &'a [u8],
        options: EnqueueOptions,
    ) -> BoxFuture<'a, Result<JobId, QueueError>>;

    /// Pull up to `max` pending jobs exclusively for `worker`.
    fn lease(
        &self,
        worker: WorkerId,
        max: usize,
    ) -> BoxFuture<'_, Result<Vec<LeasedJob>, QueueError>>;

    /// Mark a leased job complete (idempotent if already acked).
    fn ack(&self, worker: WorkerId, lease_id: LeaseId) -> BoxFuture<'_, Result<(), QueueError>>;

    /// Return a leased job to the pending set immediately.
    fn nack(&self, worker: WorkerId, lease_id: LeaseId) -> BoxFuture<'_, Result<(), QueueError>>;

    /// Depth gauges for observability and autoscale.
    fn metrics(&self) -> BoxFuture<'_, Result<QueueMetrics, QueueError>>;

    /// Apply an idempotent replicated mutation from the queue leader.
    fn apply_replicate<'a>(
        &'a self,
        op: &'a QueueReplicateOp,
    ) -> BoxFuture<'a, Result<(), QueueError>>;

    /// Like [`enqueue_opts`](Self::enqueue_opts) but returns wire replication ops for followers.
    fn enqueue_opts_replicated<'a>(
        &'a self,
        payload: &'a [u8],
        options: EnqueueOptions,
    ) -> BoxFuture<'a, Result<(JobId, QueueReplicationOps), QueueError>>;

    /// Like [`enqueue`](Self::enqueue) but returns wire replication ops for followers.
    fn enqueue_replicated<'a>(
        &'a self,
        payload: &'a [u8],
    ) -> BoxFuture<'a, Result<(JobId, QueueReplicationOps), QueueError>> {
        Box::pin(async move {
            self.enqueue_opts_replicated(payload, EnqueueOptions::default())
                .await
        })
    }

    /// Like [`lease`](Self::lease) but includes reclaim + lease replication ops.
    fn lease_replicated(
        &self,
        worker: WorkerId,
        max: usize,
    ) -> BoxFuture<'_, Result<(Vec<LeasedJob>, QueueReplicationOps), QueueError>>;

    /// Like [`ack`](Self::ack) but returns a replication op on success.
    fn ack_replicated(
        &self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'_, Result<QueueReplicationOps, QueueError>>;

    /// Like [`nack`](Self::nack) but returns a replication op on success.
    fn nack_replicated(
        &self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'_, Result<QueueReplicationOps, QueueError>>;
}

#[derive(Debug)]
struct JobEntry {
    payload: Vec<u8>,
    enqueued_at: Instant,
    priority: u8,
    not_before_ms: u64,
    dedup_key: Option<Vec<u8>>,
}

#[derive(Debug)]
struct LeaseEntry {
    job_id: JobId,
    worker: WorkerId,
    expires_at: Instant,
}

/// In-process [`JobQueue`] — not durable across restarts.
#[derive(Debug)]
pub struct InMemoryJobQueue {
    lease_timeout: Duration,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    next_job_id: u64,
    next_lease_id: u64,
    pending: VecDeque<JobId>,
    jobs: BTreeMap<JobId, JobEntry>,
    leases: BTreeMap<LeaseId, LeaseEntry>,
    dedup: BTreeMap<Vec<u8>, JobId>,
}

impl InMemoryJobQueue {
    /// Empty queue with the given visibility timeout for leases.
    #[must_use]
    pub fn new(lease_timeout: Duration) -> Self {
        Self {
            lease_timeout,
            inner: Mutex::new(Inner {
                next_job_id: 1,
                next_lease_id: 1,
                pending: VecDeque::new(),
                jobs: BTreeMap::new(),
                leases: BTreeMap::new(),
                dedup: BTreeMap::new(),
            }),
        }
    }

    fn with_inner<R>(&self, f: impl FnOnce(&mut Inner) -> R) -> R {
        let mut inner = self.inner.lock().expect("poisoned");
        inner.reclaim_expired();
        f(&mut inner)
    }
}

impl Inner {
    fn dedup_lookup(&self, key: &[u8]) -> Option<JobId> {
        self.dedup
            .get(key)
            .copied()
            .filter(|id| self.jobs.contains_key(id))
    }

    fn remove_job(&mut self, job_id: JobId) {
        if let Some(entry) = self.jobs.remove(&job_id)
            && let Some(key) = entry.dedup_key
        {
            self.dedup.remove(&key);
        }
    }

    fn reclaim_expired(&mut self) {
        let now = Instant::now();
        let expired: Vec<LeaseId> = self
            .leases
            .iter()
            .filter(|(_, lease)| now >= lease.expires_at)
            .map(|(id, _)| *id)
            .collect();
        for lease_id in expired {
            if let Some(lease) = self.leases.remove(&lease_id) {
                self.pending.push_back(lease.job_id);
            }
        }
    }

    fn metrics(&self) -> QueueMetrics {
        let now_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX);
        let oldest = self
            .pending
            .iter()
            .filter_map(|id| self.jobs.get(id))
            .filter(|entry| entry.not_before_ms <= now_ms)
            .map(|entry| entry.enqueued_at.elapsed())
            .max()
            .unwrap_or_default();
        let ready_pending = self
            .pending
            .iter()
            .filter(|id| self.jobs.get(id).is_some_and(|e| e.not_before_ms <= now_ms))
            .count();
        QueueMetrics {
            pending: ready_pending as u64,
            leased: self.leases.len() as u64,
            oldest_pending_age: oldest,
        }
    }

    fn select_pending(&self, max: usize, now_ms: u64) -> Vec<JobId> {
        let mut ready: Vec<JobId> = self
            .pending
            .iter()
            .filter(|id| self.jobs.get(id).is_some_and(|e| e.not_before_ms <= now_ms))
            .copied()
            .collect();
        ready.sort_by(|a, b| {
            let ea = self.jobs.get(a).expect("pending job");
            let eb = self.jobs.get(b).expect("pending job");
            eb.priority.cmp(&ea.priority).then_with(|| a.cmp(b))
        });
        ready.truncate(max);
        ready
    }
}

impl JobQueue for InMemoryJobQueue {
    fn enqueue_opts<'a>(
        &'a self,
        payload: &'a [u8],
        options: EnqueueOptions,
    ) -> BoxFuture<'a, Result<JobId, QueueError>> {
        Box::pin(async move {
            self.enqueue_opts_replicated(payload, options)
                .await
                .map(|(id, _)| id)
        })
    }

    fn enqueue_opts_replicated<'a>(
        &'a self,
        payload: &'a [u8],
        options: EnqueueOptions,
    ) -> BoxFuture<'a, Result<(JobId, QueueReplicationOps), QueueError>> {
        Box::pin(async move {
            let enqueued_at_ms = u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
            )
            .unwrap_or(u64::MAX);
            let not_before_ms = options.not_before_ms.unwrap_or(enqueued_at_ms);
            let (job_id, ops) = self.with_inner(|inner| {
                if let Some(key) = &options.dedup_key
                    && let Some(existing) = inner.dedup_lookup(key)
                {
                    return (existing, Vec::new());
                }
                let job_id = inner.next_job_id;
                inner.next_job_id += 1;
                let dedup_key = options.dedup_key.clone();
                inner.jobs.insert(
                    JobId(job_id),
                    JobEntry {
                        payload: payload.to_vec(),
                        enqueued_at: Instant::now(),
                        priority: options.priority,
                        not_before_ms,
                        dedup_key: dedup_key.clone(),
                    },
                );
                inner.pending.push_back(JobId(job_id));
                if let Some(key) = dedup_key {
                    inner.dedup.insert(key, JobId(job_id));
                }
                (
                    JobId(job_id),
                    vec![QueueReplicateOp::Enqueue {
                        job_id,
                        payload: payload.to_vec(),
                        enqueued_at_ms,
                        next_job_id: inner.next_job_id,
                        priority: options.priority,
                        not_before_ms,
                        dedup_key: options.dedup_key.clone(),
                    }],
                )
            });
            Ok((job_id, ops))
        })
    }

    fn apply_replicate<'a>(
        &'a self,
        op: &'a QueueReplicateOp,
    ) -> BoxFuture<'a, Result<(), QueueError>> {
        Box::pin(async move {
            self.with_inner(|inner| match op {
                QueueReplicateOp::Enqueue {
                    job_id,
                    payload,
                    enqueued_at_ms: _,
                    next_job_id,
                    priority,
                    not_before_ms,
                    dedup_key,
                } => {
                    if let Some(key) = dedup_key
                        && inner.dedup_lookup(key).is_some()
                    {
                        inner.next_job_id = inner.next_job_id.max(*next_job_id);
                        return Ok(());
                    }
                    if let std::collections::btree_map::Entry::Vacant(entry) =
                        inner.jobs.entry(JobId(*job_id))
                    {
                        entry.insert(JobEntry {
                            payload: payload.clone(),
                            enqueued_at: Instant::now(),
                            priority: *priority,
                            not_before_ms: *not_before_ms,
                            dedup_key: dedup_key.clone(),
                        });
                        inner.pending.push_back(JobId(*job_id));
                        if let Some(key) = dedup_key {
                            inner.dedup.insert(key.clone(), JobId(*job_id));
                        }
                    }
                    inner.next_job_id = inner.next_job_id.max(*next_job_id);
                    Ok(())
                }
                QueueReplicateOp::Lease {
                    lease_id,
                    job_id,
                    worker_node,
                    worker_instance,
                    expires_at_ms: _,
                    next_lease_id,
                } => {
                    inner.pending.retain(|id| id.0 != *job_id);
                    inner
                        .leases
                        .entry(LeaseId(*lease_id))
                        .or_insert(LeaseEntry {
                            job_id: JobId(*job_id),
                            worker: WorkerId {
                                node: NodeId(*worker_node),
                                instance: *worker_instance,
                            },
                            expires_at: Instant::now() + Duration::from_secs(3600),
                        });
                    inner.next_lease_id = inner.next_lease_id.max(*next_lease_id);
                    Ok(())
                }
                QueueReplicateOp::Ack { lease_id, job_id } => {
                    inner.leases.remove(&LeaseId(*lease_id));
                    inner.remove_job(JobId(*job_id));
                    Ok(())
                }
                QueueReplicateOp::Nack { lease_id, job_id }
                | QueueReplicateOp::Reclaim { lease_id, job_id } => {
                    inner.leases.remove(&LeaseId(*lease_id));
                    if inner.jobs.contains_key(&JobId(*job_id)) {
                        inner.pending.push_back(JobId(*job_id));
                    }
                    Ok(())
                }
            })
        })
    }

    fn lease(
        &self,
        worker: WorkerId,
        max: usize,
    ) -> BoxFuture<'_, Result<Vec<LeasedJob>, QueueError>> {
        Box::pin(async move {
            self.lease_replicated(worker, max)
                .await
                .map(|(jobs, _)| jobs)
        })
    }

    fn lease_replicated(
        &self,
        worker: WorkerId,
        max: usize,
    ) -> BoxFuture<'_, Result<(Vec<LeasedJob>, QueueReplicationOps), QueueError>> {
        Box::pin(async move {
            let mut ops = Vec::new();
            let now = Instant::now();
            let expired: Vec<(LeaseId, JobId)> = {
                let inner = self.inner.lock().expect("poisoned");
                inner
                    .leases
                    .iter()
                    .filter(|(_, lease)| now >= lease.expires_at)
                    .map(|(id, lease)| (*id, lease.job_id))
                    .collect()
            };
            for (lease_id, job_id) in expired {
                self.with_inner(|inner| {
                    inner.leases.remove(&lease_id);
                    inner.pending.push_back(job_id);
                });
                ops.push(QueueReplicateOp::Reclaim {
                    lease_id: lease_id.0,
                    job_id: job_id.0,
                });
            }

            let (jobs, lease_ops) = self.with_inner(|inner| {
                let mut out = Vec::new();
                let mut lease_ops = Vec::new();
                let deadline = Instant::now() + self.lease_timeout;
                let now_ms = u64::try_from(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis(),
                )
                .unwrap_or(u64::MAX);
                for job_id in inner.select_pending(max, now_ms) {
                    inner.pending.retain(|id| *id != job_id);
                    let Some(entry) = inner.jobs.get(&job_id) else {
                        continue;
                    };
                    let lease_id = inner.next_lease_id;
                    inner.next_lease_id += 1;
                    inner.leases.insert(
                        LeaseId(lease_id),
                        LeaseEntry {
                            job_id,
                            worker,
                            expires_at: deadline,
                        },
                    );
                    lease_ops.push(QueueReplicateOp::Lease {
                        lease_id,
                        job_id: job_id.0,
                        worker_node: worker.node.0,
                        worker_instance: worker.instance,
                        expires_at_ms: 0,
                        next_lease_id: inner.next_lease_id,
                    });
                    out.push(LeasedJob {
                        lease_id: LeaseId(lease_id),
                        job_id,
                        payload: entry.payload.clone(),
                    });
                }
                (out, lease_ops)
            });
            ops.extend(lease_ops);
            Ok((jobs, ops))
        })
    }

    fn ack(&self, worker: WorkerId, lease_id: LeaseId) -> BoxFuture<'_, Result<(), QueueError>> {
        Box::pin(async move { self.ack_replicated(worker, lease_id).await.map(|_| ()) })
    }

    fn ack_replicated(
        &self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'_, Result<QueueReplicationOps, QueueError>> {
        Box::pin(async move {
            let job_id = self.with_inner(|inner| {
                let lease = inner
                    .leases
                    .remove(&lease_id)
                    .ok_or(QueueError::InvalidLease)?;
                if lease.worker != worker {
                    inner.leases.insert(lease_id, lease);
                    return Err(QueueError::InvalidLease);
                }
                inner.remove_job(lease.job_id);
                Ok(lease.job_id.0)
            })?;
            Ok(vec![QueueReplicateOp::Ack {
                lease_id: lease_id.0,
                job_id,
            }])
        })
    }

    fn nack(&self, worker: WorkerId, lease_id: LeaseId) -> BoxFuture<'_, Result<(), QueueError>> {
        Box::pin(async move { self.nack_replicated(worker, lease_id).await.map(|_| ()) })
    }

    fn nack_replicated(
        &self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'_, Result<QueueReplicationOps, QueueError>> {
        Box::pin(async move {
            let job_id = self.with_inner(|inner| {
                let lease = inner
                    .leases
                    .remove(&lease_id)
                    .ok_or(QueueError::InvalidLease)?;
                if lease.worker != worker {
                    inner.leases.insert(lease_id, lease);
                    return Err(QueueError::InvalidLease);
                }
                inner.pending.push_back(lease.job_id);
                Ok(lease.job_id.0)
            })?;
            Ok(vec![QueueReplicateOp::Nack {
                lease_id: lease_id.0,
                job_id,
            }])
        })
    }

    fn metrics(&self) -> BoxFuture<'_, Result<QueueMetrics, QueueError>> {
        Box::pin(async move { Ok(self.with_inner(|inner| inner.metrics())) })
    }
}

/// Poll a [`JobQueue`], invoke `handle` on each payload, then ack or nack.
///
/// Runs until `stop` is set. When the queue is empty, sleeps `idle_sleep` between polls.
pub async fn run_queue_consumer<Q, F, Fut, E>(
    queue: std::sync::Arc<Q>,
    worker: WorkerId,
    batch: usize,
    idle_sleep: Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
    mut handle: F,
) where
    Q: JobQueue + ?Sized,
    F: FnMut(&[u8]) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    loop {
        if *stop.borrow() {
            break;
        }
        let Ok(jobs) = queue.lease(worker, batch).await else {
            tokio::time::sleep(idle_sleep).await;
            continue;
        };
        if jobs.is_empty() {
            tokio::select! {
                () = tokio::time::sleep(idle_sleep) => {}
                _ = stop.changed() => {
                    if *stop.borrow() {
                        break;
                    }
                }
            }
            continue;
        }
        for job in jobs {
            if *stop.borrow() {
                let _ = queue.nack(worker, job.lease_id).await;
                return;
            }
            match handle(&job.payload).await {
                Ok(()) => {
                    let _ = queue.ack(worker, job.lease_id).await;
                }
                Err(_) => {
                    let _ = queue.nack(worker, job.lease_id).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker(instance: u32) -> WorkerId {
        WorkerId {
            node: NodeId(1),
            instance,
        }
    }

    #[tokio::test]
    async fn enqueue_lease_ack_round_trip() {
        let q = InMemoryJobQueue::new(Duration::from_secs(30));
        let id = q.enqueue(b"job").await.unwrap();
        assert_eq!(id, JobId(1));

        let leased = q.lease(worker(0), 8).await.unwrap();
        assert_eq!(leased.len(), 1);
        assert_eq!(leased[0].payload, b"job");

        q.ack(worker(0), leased[0].lease_id).await.unwrap();
        let m = q.metrics().await.unwrap();
        assert_eq!(m.pending, 0);
        assert_eq!(m.leased, 0);
    }

    #[tokio::test]
    async fn two_workers_get_distinct_jobs() {
        let q = InMemoryJobQueue::new(Duration::from_secs(30));
        q.enqueue(b"a").await.unwrap();
        q.enqueue(b"b").await.unwrap();

        let a = q.lease(worker(0), 1).await.unwrap();
        let b = q.lease(worker(1), 1).await.unwrap();
        assert_eq!(a[0].payload, b"a");
        assert_eq!(b[0].payload, b"b");
    }

    #[tokio::test]
    async fn nack_requeues() {
        let q = InMemoryJobQueue::new(Duration::from_secs(30));
        q.enqueue(b"x").await.unwrap();
        let leased = q.lease(worker(0), 1).await.unwrap();
        q.nack(worker(0), leased[0].lease_id).await.unwrap();

        let again = q.lease(worker(1), 1).await.unwrap();
        assert_eq!(again[0].payload, b"x");
    }

    #[tokio::test]
    async fn expired_lease_returns_to_pending() {
        let q = InMemoryJobQueue::new(Duration::from_millis(20));
        q.enqueue(b"z").await.unwrap();
        let leased = q.lease(worker(0), 1).await.unwrap();
        assert_eq!(leased.len(), 1);

        tokio::time::sleep(Duration::from_millis(40)).await;
        let m = q.metrics().await.unwrap();
        assert_eq!(m.pending, 1);
        assert_eq!(m.leased, 0);

        let again = q.lease(worker(1), 1).await.unwrap();
        assert_eq!(again[0].payload, b"z");
    }

    #[tokio::test]
    async fn dedup_key_returns_existing_job_id() {
        let q = InMemoryJobQueue::new(Duration::from_secs(30));
        let id1 = q
            .enqueue_opts(b"first", EnqueueOptions::dedup_key("order-1"))
            .await
            .unwrap();
        let id2 = q
            .enqueue_opts(b"retry", EnqueueOptions::dedup_key("order-1"))
            .await
            .unwrap();
        assert_eq!(id1, id2);
        assert_eq!(q.metrics().await.unwrap().pending, 1);
    }

    #[tokio::test]
    async fn priority_jobs_leased_first() {
        let q = InMemoryJobQueue::new(Duration::from_secs(30));
        q.enqueue_opts(b"low", EnqueueOptions::default())
            .await
            .unwrap();
        q.enqueue_opts(b"high", EnqueueOptions::priority(10))
            .await
            .unwrap();

        let leased = q.lease(worker(0), 1).await.unwrap();
        assert_eq!(leased[0].payload, b"high");
    }

    #[tokio::test]
    async fn delayed_job_not_leased_before_not_before() {
        let q = InMemoryJobQueue::new(Duration::from_secs(30));
        let far_future = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap_or(u64::MAX)
            + 3_600_000;
        q.enqueue_opts(
            b"later",
            EnqueueOptions {
                not_before_ms: Some(far_future),
                ..EnqueueOptions::default()
            },
        )
        .await
        .unwrap();

        let empty = q.lease(worker(0), 1).await.unwrap();
        assert!(empty.is_empty());
        assert_eq!(q.metrics().await.unwrap().pending, 0);
    }

    #[tokio::test]
    async fn ack_rejects_wrong_worker() {
        let q = InMemoryJobQueue::new(Duration::from_secs(30));
        q.enqueue(b"j").await.unwrap();
        let leased = q.lease(worker(0), 1).await.unwrap();
        assert!(matches!(
            q.ack(worker(1), leased[0].lease_id).await,
            Err(QueueError::InvalidLease)
        ));
    }
}
