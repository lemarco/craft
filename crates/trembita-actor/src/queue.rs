//! Durable job backlog port ([job-queue](../../../docs/decisions/job-queue.md)).
//!
//! [`JobQueue`] is the job queue layer: shared async work with `lease` / `ack`,
//! distinct from actor mailboxes and Raft consensus. [`InMemoryJobQueue`]
//! backs tests and single-node dev; production uses [`RedbJobQueue`](super::redb_queue::RedbJobQueue).

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use trembita_proto::NodeId;
pub use trembita_proto::QueueReplicateOp;

pub use crate::store::BoxFuture;

/// Opaque job identifier (monotonic per queue stream).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobId(pub u64);

/// Lease token returned by [`JobQueue::lease`], required for ack/nack/extend.
///
/// Within a stream, ids are **monotonically increasing** and the counter survives
/// leader failover (replicated via [`QueueReplicateOp::Lease`] with a `max()` bump).
/// Redelivery (nack, timeout, worker loss) issues a **new** id; ack/nack with a
/// stale token returns [`QueueError::InvalidLease`].
///
/// This is a queue ownership token, not an application fencing token for external
/// side effects — see [background-jobs § Delivery
/// semantics](../../../docs/scenarios/background-jobs.md#delivery-semantics).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeaseId(pub u64);

/// Identifies a queue consumer (actor instance on a node).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkerId {
    /// Hosting cluster node.
    pub node: NodeId,
    /// Worker actor instance id on that node.
    pub instance: u32,
}

/// A job handed to a worker under lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeasedJob {
    /// Token required for ack/nack.
    pub lease_id: LeaseId,
    /// Job id within the stream.
    pub job_id: JobId,
    /// Opaque payload from enqueue.
    pub payload: Vec<u8>,
    /// Delivery attempts including this one (`1` on first delivery).
    ///
    /// Greater than `1` means this job was redelivered — a previous attempt was
    /// nacked, timed out, or lost its worker. Handlers must assume the earlier
    /// attempt may have partially applied its side effect.
    pub attempts: u32,
    /// Client idempotency token from [`EnqueueOptions::dedup_key`], when set.
    pub dedup_key: Option<Vec<u8>>,
}

/// What a consumer knows about the delivery it is currently handling.
///
/// Passed to `#[consumer]` handlers that declare a second argument. See
/// [background-jobs](../../../docs/scenarios/background-jobs.md#delivery-semantics)
/// for what is and is not guaranteed.
#[derive(Debug, Clone)]
pub struct JobContext<'a> {
    /// Job id within the stream.
    pub job_id: JobId,
    /// Lease token backing this delivery.
    pub lease_id: LeaseId,
    /// Stream the job was leased from.
    pub stream: &'a str,
    /// Delivery attempts including this one (`1` on first delivery).
    pub attempts: u32,
    /// Client idempotency token from enqueue, when set.
    pub dedup_key: Option<&'a [u8]>,
    keep_alive: Option<JobKeepAlive>,
}

/// Leader-backed lease extension wired by the consumer runtime.
#[derive(Clone)]
struct JobKeepAlive {
    queue: std::sync::Arc<dyn JobQueue>,
    worker: WorkerId,
    lease_id: LeaseId,
}

impl fmt::Debug for JobKeepAlive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JobKeepAlive")
            .field("worker", &self.worker)
            .field("lease_id", &self.lease_id)
            .finish_non_exhaustive()
    }
}

impl JobContext<'_> {
    /// Build a delivery context for tests and manual handler invocation.
    #[must_use]
    pub fn new<'a>(
        job_id: JobId,
        lease_id: LeaseId,
        stream: &'a str,
        attempts: u32,
        dedup_key: Option<&'a [u8]>,
    ) -> JobContext<'a> {
        JobContext {
            job_id,
            lease_id,
            stream,
            attempts,
            dedup_key,
            keep_alive: None,
        }
    }

    /// `true` when this is not the first delivery of the job.
    ///
    /// A redelivery does not mean the previous attempt did nothing — only that it
    /// did not ack.
    #[must_use]
    pub fn is_redelivery(&self) -> bool {
        self.attempts > 1
    }

    /// Reset the visibility timeout to the stream's lease duration.
    ///
    /// Call periodically from long-running handlers so the queue does not reclaim
    /// the job while work is still in progress. No-op when the context was not
    /// created by the consumer runtime (e.g. unit tests).
    ///
    /// # Errors
    /// Returns [`QueueError::InvalidLease`] when the lease expired or belongs to
    /// another worker.
    pub async fn keep_alive(&self) -> Result<(), QueueError> {
        match &self.keep_alive {
            Some(ext) => ext.queue.extend_lease(ext.worker, ext.lease_id).await,
            None => Ok(()),
        }
    }

    /// Attach lease extension for consumer runtime wiring.
    #[must_use]
    pub fn attach_keep_alive(
        mut self,
        queue: std::sync::Arc<dyn JobQueue>,
        worker: WorkerId,
    ) -> Self {
        self.keep_alive = Some(JobKeepAlive {
            queue,
            worker,
            lease_id: self.lease_id,
        });
        self
    }
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
    /// Maximum delivery attempts before dead letter.
    ///
    /// `None` inherits the stream default set via
    /// [`InMemoryJobQueue::default_max_attempts`] (or `QueueOpts`/`JobOpts` at the
    /// app layer). `Some(0)` is an explicit request for unlimited retries and
    /// overrides the stream default.
    pub max_attempts: Option<u32>,
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

    /// Cap delivery attempts; after the limit the job moves to dead letter.
    ///
    /// Overrides the stream default. `max_attempts(0)` means unlimited retries.
    #[must_use]
    pub fn max_attempts(max: u32) -> Self {
        Self {
            max_attempts: Some(max),
            ..Self::default()
        }
    }
}

/// Lifecycle phase of a job in the queue (observability / HTTP lookup).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobLifecycle {
    /// Waiting in the pending set (eligible to lease now).
    Pending,
    /// Leased to a worker (not yet acked).
    Leased,
    /// Enqueued but `not_before` is still in the future.
    Delayed,
    /// Exhausted retry budget — held until an operator requeues.
    DeadLetter,
}

/// Metadata for a single job returned by [`JobQueue::job_status`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobStatus {
    /// Job id within the stream (global id when using [`ShardedJobQueue`](super::sharded_queue::ShardedJobQueue)).
    pub job_id: JobId,
    /// Current lifecycle phase.
    pub lifecycle: JobLifecycle,
    /// Byte length of the stored payload (payload itself is not returned).
    pub payload_len: u64,
    /// Enqueue priority.
    pub priority: u8,
    /// Set when [`JobLifecycle::Leased`].
    pub leased_by: Option<WorkerId>,
    /// Delivery attempts recorded so far.
    pub attempts: u32,
    /// Configured retry ceiling (`0` = unlimited).
    pub max_attempts: u32,
    /// Client idempotency token from enqueue, when set.
    pub dedup_key: Option<Vec<u8>>,
}

/// Filters for [`JobQueue::list_jobs`] (admin / HTTP list endpoint).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JobListFilter {
    /// When set, only jobs in this lifecycle phase.
    pub lifecycle: Option<JobLifecycle>,
    /// When set, only jobs with `attempts >= min_attempts`.
    pub min_attempts: Option<u32>,
    /// When set, only jobs with this exact dedup key.
    pub dedup_key: Option<Vec<u8>>,
    /// Maximum rows to return (default [`LIST_JOBS_DEFAULT_LIMIT`], capped at
    /// [`crate::DEFAULT_QUEUE_BATCH_MAX`]).
    pub limit: Option<usize>,
    /// Pagination cursor — return jobs with id strictly greater than this.
    pub after_job_id: Option<JobId>,
}

/// Default page size for [`JobListFilter`].
pub const LIST_JOBS_DEFAULT_LIMIT: usize = 50;

impl JobListFilter {
    /// Resolved page size after applying defaults and the server cap.
    #[must_use]
    pub fn effective_limit(&self) -> usize {
        self.limit
            .unwrap_or(LIST_JOBS_DEFAULT_LIMIT)
            .clamp(1, crate::DEFAULT_QUEUE_BATCH_MAX)
    }
}

/// One page of jobs from [`JobQueue::list_jobs`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct JobListPage {
    /// Matching jobs in ascending job-id order.
    pub jobs: Vec<JobStatus>,
    /// `true` when more rows exist beyond this page.
    pub has_more: bool,
}

/// Outcome of [`JobQueue::requeue_dead_letter_batch`].
#[derive(Debug, Default)]
pub struct BatchRequeueResult {
    /// Job ids successfully moved back to pending.
    pub requeued: Vec<JobId>,
    /// Per-id failures (not dead letter, unknown id, …).
    pub failures: Vec<(JobId, QueueError)>,
}

/// Returns `true` when `status` satisfies optional list filters.
#[must_use]
pub fn job_status_matches_filter(status: &JobStatus, filter: &JobListFilter) -> bool {
    if filter.lifecycle.is_some_and(|l| status.lifecycle != l) {
        return false;
    }
    if filter.min_attempts.is_some_and(|min| status.attempts < min) {
        return false;
    }
    if let Some(key) = &filter.dedup_key {
        match &status.dedup_key {
            Some(k) if k == key => {}
            _ => return false,
        }
    }
    true
}

/// Depth gauges returned by [`JobQueue::metrics`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueueMetrics {
    /// Jobs eligible to lease now.
    pub pending: u64,
    /// Jobs currently leased.
    pub leased: u64,
    /// Jobs in the dead-letter set.
    pub dead_letter: u64,
    /// Age of the oldest ready pending job.
    pub oldest_pending_age: Duration,
    /// Jobs still in the queue that have already failed at least one attempt.
    ///
    /// An idempotency smell: every one of these will be delivered again, so the
    /// handler must be safe to re-run ([background-jobs](../../../docs/scenarios/background-jobs.md#delivery-semantics)).
    pub redelivered: u64,
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
    /// Job is not in the dead-letter set.
    #[error("job is not in dead letter")]
    NotDeadLetter,
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

    /// Reset visibility timeout for a live lease (worker heartbeat).
    fn extend_lease(
        &self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'_, Result<(), QueueError>> {
        Box::pin(async move {
            self.extend_lease_replicated(worker, lease_id)
                .await
                .map(|_| ())
        })
    }

    /// Like [`extend_lease`](Self::extend_lease) but returns a replication op on success.
    fn extend_lease_replicated(
        &self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'_, Result<QueueReplicationOps, QueueError>>;

    /// Depth gauges for observability and autoscale.
    fn metrics(&self) -> BoxFuture<'_, Result<QueueMetrics, QueueError>>;

    /// Lookup job metadata by id (`None` when acked or unknown).
    fn job_status(&self, job_id: JobId) -> BoxFuture<'_, Result<Option<JobStatus>, QueueError>>;

    /// List jobs in the stream with optional filters (admin inspection).
    fn list_jobs(&self, filter: JobListFilter) -> BoxFuture<'_, Result<JobListPage, QueueError>>;

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

    /// Peek dedup key and attempts for a live lease (external backlog settlement).
    fn peek_lease_meta(&self, lease_id: LeaseId) -> BoxFuture<'_, Option<(Option<Vec<u8>>, u32)>> {
        let _ = lease_id;
        Box::pin(async { None })
    }

    /// Move a dead-letter job back to pending (operator recovery).
    fn requeue_dead_letter(&self, job_id: JobId) -> BoxFuture<'_, Result<(), QueueError>>;

    /// Requeue many dead-letter jobs; partial success is allowed.
    fn requeue_dead_letter_batch(
        &self,
        job_ids: &[JobId],
    ) -> BoxFuture<'_, Result<BatchRequeueResult, QueueError>> {
        let ids: Vec<JobId> = job_ids.to_vec();
        Box::pin(async move {
            let (requeued, failures, _) = self.requeue_dead_letter_batch_replicated(&ids).await?;
            Ok(BatchRequeueResult { requeued, failures })
        })
    }

    /// Like [`requeue_dead_letter_batch`](Self::requeue_dead_letter_batch) but returns replication ops.
    #[allow(clippy::type_complexity)]
    fn requeue_dead_letter_batch_replicated<'a>(
        &'a self,
        job_ids: &'a [JobId],
    ) -> BoxFuture<
        'a,
        Result<(Vec<JobId>, Vec<(JobId, QueueError)>, QueueReplicationOps), QueueError>,
    >;

    /// Like [`requeue_dead_letter`](Self::requeue_dead_letter) but returns replication ops.
    fn requeue_dead_letter_replicated(
        &self,
        job_id: JobId,
    ) -> BoxFuture<'_, Result<QueueReplicationOps, QueueError>> {
        Box::pin(async move {
            self.requeue_dead_letter(job_id).await?;
            Ok(vec![QueueReplicateOp::RequeueDeadLetter {
                job_id: job_id.0,
                attempts: 0,
            }])
        })
    }

    /// Append many jobs in one leader transaction when the backend supports it.
    fn enqueue_batch_opts<'a>(
        &'a self,
        jobs: &'a [(Vec<u8>, EnqueueOptions)],
    ) -> BoxFuture<'a, Result<Vec<JobId>, QueueError>> {
        Box::pin(async move {
            let (ids, _) = self.enqueue_batch_opts_replicated(jobs).await?;
            Ok(ids)
        })
    }

    /// Like [`enqueue_batch_opts`](Self::enqueue_batch_opts) with default options per payload.
    fn enqueue_batch<'a>(
        &'a self,
        payloads: &'a [&'a [u8]],
    ) -> BoxFuture<'a, Result<Vec<JobId>, QueueError>> {
        Box::pin(async move {
            let jobs: Vec<(Vec<u8>, EnqueueOptions)> = payloads
                .iter()
                .map(|payload| ((*payload).to_vec(), EnqueueOptions::default()))
                .collect();
            self.enqueue_batch_opts(&jobs).await
        })
    }

    /// Acknowledge many leases in one leader transaction when supported.
    fn ack_batch<'a>(
        &'a self,
        worker: WorkerId,
        lease_ids: &'a [LeaseId],
    ) -> BoxFuture<'a, Result<(), QueueError>> {
        Box::pin(async move {
            if lease_ids.is_empty() {
                return Ok(());
            }
            self.ack_batch_replicated(worker, lease_ids).await?;
            Ok(())
        })
    }

    /// Append many jobs in one backend transaction when supported.
    fn enqueue_batch_opts_replicated<'a>(
        &'a self,
        jobs: &'a [(Vec<u8>, EnqueueOptions)],
    ) -> BoxFuture<'a, Result<(Vec<JobId>, QueueReplicationOps), QueueError>> {
        Box::pin(async move {
            let mut ids = Vec::with_capacity(jobs.len());
            let mut ops = Vec::new();
            for (payload, options) in jobs {
                let (id, mut step) = self
                    .enqueue_opts_replicated(payload, options.clone())
                    .await?;
                ids.push(id);
                ops.append(&mut step);
            }
            Ok((ids, ops))
        })
    }

    /// Acknowledge many leases in one backend transaction when supported.
    fn ack_batch_replicated<'a>(
        &'a self,
        worker: WorkerId,
        lease_ids: &'a [LeaseId],
    ) -> BoxFuture<'a, Result<QueueReplicationOps, QueueError>> {
        Box::pin(async move {
            let mut ops = Vec::with_capacity(lease_ids.len());
            for lease_id in lease_ids {
                ops.extend(self.ack_replicated(worker, *lease_id).await?);
            }
            Ok(ops)
        })
    }
}

#[derive(Debug)]
struct JobEntry {
    payload: Vec<u8>,
    enqueued_at: Instant,
    priority: u8,
    not_before_ms: u64,
    dedup_key: Option<Vec<u8>>,
    attempts: u32,
    max_attempts: u32,
    dead_letter: bool,
}

pub(crate) struct AttemptOutcome {
    pub attempts: u32,
    pub dead_letter: bool,
    pub not_before_ms: u64,
}

pub(crate) fn after_failed_attempt(
    attempts: u32,
    max_attempts: u32,
    now_ms: u64,
) -> AttemptOutcome {
    let attempts = attempts.saturating_add(1);
    if max_attempts > 0 && attempts >= max_attempts {
        AttemptOutcome {
            attempts,
            dead_letter: true,
            not_before_ms: now_ms,
        }
    } else {
        let delay_ms = (1000u64 * u64::from(attempts)).min(300_000);
        AttemptOutcome {
            attempts,
            dead_letter: false,
            not_before_ms: now_ms.saturating_add(delay_ms),
        }
    }
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
    default_max_attempts: u32,
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
            default_max_attempts: 0,
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

    /// Attempt ceiling applied when [`EnqueueOptions::max_attempts`] is `None` (`0` = unlimited).
    #[must_use]
    pub fn default_max_attempts(mut self, max: u32) -> Self {
        self.default_max_attempts = max;
        self
    }

    fn with_inner<R>(&self, f: impl FnOnce(&mut Inner) -> R) -> R {
        let mut inner = self.inner.lock().expect("poisoned");
        inner.reclaim_expired();
        f(&mut inner)
    }

    pub(crate) fn peek_lease_for_settle(
        &self,
        lease_id: LeaseId,
    ) -> Option<(Option<Vec<u8>>, u32)> {
        let inner = self.inner.lock().ok()?;
        let lease = inner.leases.get(&lease_id)?;
        let entry = inner.jobs.get(&lease.job_id)?;
        Some((entry.dedup_key.clone(), entry.attempts))
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
        let now_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX);
        let expired: Vec<LeaseId> = self
            .leases
            .iter()
            .filter(|(_, lease)| now >= lease.expires_at)
            .map(|(id, _)| *id)
            .collect();
        for lease_id in expired {
            if let Some(lease) = self.leases.remove(&lease_id)
                && let Some(entry) = self.jobs.get_mut(&lease.job_id)
            {
                let outcome = after_failed_attempt(entry.attempts, entry.max_attempts, now_ms);
                entry.attempts = outcome.attempts;
                entry.dead_letter = outcome.dead_letter;
                entry.not_before_ms = outcome.not_before_ms;
                if !outcome.dead_letter {
                    self.pending.push_back(lease.job_id);
                }
            }
        }
    }

    fn release_expired_lease(
        &mut self,
        lease_id: LeaseId,
        now_ms: u64,
    ) -> Result<(JobId, AttemptOutcome), QueueError> {
        let lease = self
            .leases
            .remove(&lease_id)
            .ok_or(QueueError::InvalidLease)?;
        let entry = self
            .jobs
            .get_mut(&lease.job_id)
            .ok_or(QueueError::InvalidLease)?;
        let outcome = after_failed_attempt(entry.attempts, entry.max_attempts, now_ms);
        entry.attempts = outcome.attempts;
        entry.dead_letter = outcome.dead_letter;
        entry.not_before_ms = outcome.not_before_ms;
        if !outcome.dead_letter {
            self.pending.push_back(lease.job_id);
        }
        Ok((lease.job_id, outcome))
    }

    fn release_lease(
        &mut self,
        lease_id: LeaseId,
        worker: WorkerId,
        now_ms: u64,
    ) -> Result<(JobId, AttemptOutcome), QueueError> {
        let lease = self
            .leases
            .remove(&lease_id)
            .ok_or(QueueError::InvalidLease)?;
        if lease.worker != worker {
            self.leases.insert(lease_id, lease);
            return Err(QueueError::InvalidLease);
        }
        let entry = self
            .jobs
            .get_mut(&lease.job_id)
            .ok_or(QueueError::InvalidLease)?;
        let outcome = after_failed_attempt(entry.attempts, entry.max_attempts, now_ms);
        entry.attempts = outcome.attempts;
        entry.dead_letter = outcome.dead_letter;
        entry.not_before_ms = outcome.not_before_ms;
        if !outcome.dead_letter {
            self.pending.push_back(lease.job_id);
        }
        Ok((lease.job_id, outcome))
    }

    fn requeue_dead_letter(&mut self, job_id: JobId, now_ms: u64) -> Result<(), QueueError> {
        let entry = self
            .jobs
            .get_mut(&job_id)
            .ok_or(QueueError::NotDeadLetter)?;
        if !entry.dead_letter {
            return Err(QueueError::NotDeadLetter);
        }
        entry.dead_letter = false;
        entry.attempts = 0;
        entry.not_before_ms = now_ms;
        self.pending.push_back(job_id);
        Ok(())
    }

    fn job_status(&self, job_id: JobId) -> Option<JobStatus> {
        let entry = self.jobs.get(&job_id)?;
        let now_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX);
        let leased_by = self
            .leases
            .values()
            .find(|lease| lease.job_id == job_id)
            .map(|lease| lease.worker);
        let lifecycle = if entry.dead_letter {
            JobLifecycle::DeadLetter
        } else if leased_by.is_some() {
            JobLifecycle::Leased
        } else if entry.not_before_ms > now_ms {
            JobLifecycle::Delayed
        } else if self.pending.contains(&job_id) {
            JobLifecycle::Pending
        } else {
            return None;
        };
        Some(JobStatus {
            job_id,
            lifecycle,
            payload_len: u64::try_from(entry.payload.len()).unwrap_or(u64::MAX),
            priority: entry.priority,
            leased_by,
            attempts: entry.attempts,
            max_attempts: entry.max_attempts,
            dedup_key: entry.dedup_key.clone(),
        })
    }

    fn list_jobs(&self, filter: &JobListFilter) -> JobListPage {
        let limit = filter.effective_limit();
        let after = filter.after_job_id.map_or(0, |id| id.0);
        let mut jobs = Vec::new();
        let mut has_more = false;
        for job_id in self.jobs.keys().copied().filter(|id| id.0 > after) {
            let Some(status) = self.job_status(job_id) else {
                continue;
            };
            if !job_status_matches_filter(&status, filter) {
                continue;
            }
            jobs.push(status);
            if jobs.len() > limit {
                jobs.pop();
                has_more = true;
                break;
            }
        }
        JobListPage { jobs, has_more }
    }

    fn requeue_dead_letter_batch(
        &mut self,
        job_ids: &[JobId],
        now_ms: u64,
    ) -> (Vec<JobId>, Vec<(JobId, QueueError)>) {
        let mut requeued = Vec::with_capacity(job_ids.len());
        let mut failures = Vec::new();
        for &job_id in job_ids {
            match self.requeue_dead_letter(job_id, now_ms) {
                Ok(()) => requeued.push(job_id),
                Err(e) => failures.push((job_id, e)),
            }
        }
        (requeued, failures)
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
            dead_letter: self.jobs.values().filter(|entry| entry.dead_letter).count() as u64,
            oldest_pending_age: oldest,
            redelivered: self
                .jobs
                .values()
                .filter(|entry| entry.attempts > 0 && !entry.dead_letter)
                .count() as u64,
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
            let max_attempts = options.max_attempts.unwrap_or(self.default_max_attempts);
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
                        attempts: 0,
                        max_attempts,
                        dead_letter: false,
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
                        attempts: 0,
                        max_attempts,
                    }],
                )
            });
            Ok((job_id, ops))
        })
    }

    #[allow(clippy::too_many_lines)] // large replicate-op match
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
                    attempts,
                    max_attempts,
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
                            attempts: *attempts,
                            max_attempts: *max_attempts,
                            dead_letter: false,
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
                QueueReplicateOp::Nack {
                    lease_id,
                    job_id,
                    attempts,
                    dead_letter,
                    not_before_ms,
                }
                | QueueReplicateOp::Reclaim {
                    lease_id,
                    job_id,
                    attempts,
                    dead_letter,
                    not_before_ms,
                } => {
                    inner.leases.remove(&LeaseId(*lease_id));
                    if let Some(entry) = inner.jobs.get_mut(&JobId(*job_id)) {
                        entry.attempts = *attempts;
                        entry.dead_letter = *dead_letter;
                        entry.not_before_ms = *not_before_ms;
                        if !dead_letter {
                            inner.pending.push_back(JobId(*job_id));
                        }
                    }
                    Ok(())
                }
                QueueReplicateOp::RequeueDeadLetter { job_id, attempts } => {
                    if let Some(entry) = inner.jobs.get_mut(&JobId(*job_id)) {
                        entry.dead_letter = false;
                        entry.attempts = *attempts;
                        entry.not_before_ms = u64::try_from(
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis(),
                        )
                        .unwrap_or(u64::MAX);
                        inner.pending.push_back(JobId(*job_id));
                    }
                    Ok(())
                }
                QueueReplicateOp::ExtendLease {
                    lease_id,
                    worker_node,
                    worker_instance,
                    expires_at_ms,
                } => {
                    if let Some(lease) = inner.leases.get_mut(&LeaseId(*lease_id))
                        && lease.worker.node.0 == *worker_node
                        && lease.worker.instance == *worker_instance
                    {
                        lease.expires_at = instant_from_unix_ms(*expires_at_ms);
                    }
                    Ok(())
                }
                QueueReplicateOp::UpsertSchedule { .. }
                | QueueReplicateOp::UpdateScheduleNextRun { .. }
                | QueueReplicateOp::RemoveSchedule { .. } => Ok(()),
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
            let now_ms = u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
            )
            .unwrap_or(u64::MAX);
            for (lease_id, _job_id) in expired {
                let op = self.with_inner(|inner| {
                    let (job_id, outcome) = inner.release_expired_lease(lease_id, now_ms)?;
                    Ok::<_, QueueError>(QueueReplicateOp::Reclaim {
                        lease_id: lease_id.0,
                        job_id: job_id.0,
                        attempts: outcome.attempts,
                        dead_letter: outcome.dead_letter,
                        not_before_ms: outcome.not_before_ms,
                    })
                });
                if let Ok(op) = op {
                    ops.push(op);
                }
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
                        // `entry.attempts` counts attempts that already failed;
                        // this delivery is the next one.
                        attempts: entry.attempts + 1,
                        dedup_key: entry.dedup_key.clone(),
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
            let now_ms = u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
            )
            .unwrap_or(u64::MAX);
            let (job_id, outcome) =
                self.with_inner(|inner| inner.release_lease(lease_id, worker, now_ms))?;
            Ok(vec![QueueReplicateOp::Nack {
                lease_id: lease_id.0,
                job_id: job_id.0,
                attempts: outcome.attempts,
                dead_letter: outcome.dead_letter,
                not_before_ms: outcome.not_before_ms,
            }])
        })
    }

    fn extend_lease_replicated(
        &self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'_, Result<QueueReplicationOps, QueueError>> {
        Box::pin(async move {
            let expires_at = Instant::now() + self.lease_timeout;
            let expires_at_ms = unix_ms_from_instant(expires_at);
            self.with_inner(|inner| {
                let lease = inner
                    .leases
                    .get_mut(&lease_id)
                    .ok_or(QueueError::InvalidLease)?;
                if lease.worker != worker {
                    return Err(QueueError::InvalidLease);
                }
                lease.expires_at = expires_at;
                Ok(())
            })?;
            Ok(vec![QueueReplicateOp::ExtendLease {
                lease_id: lease_id.0,
                worker_node: worker.node.0,
                worker_instance: worker.instance,
                expires_at_ms,
            }])
        })
    }

    fn requeue_dead_letter(&self, job_id: JobId) -> BoxFuture<'_, Result<(), QueueError>> {
        Box::pin(async move {
            self.requeue_dead_letter_replicated(job_id)
                .await
                .map(|_| ())
        })
    }

    fn requeue_dead_letter_replicated(
        &self,
        job_id: JobId,
    ) -> BoxFuture<'_, Result<QueueReplicationOps, QueueError>> {
        Box::pin(async move {
            let now_ms = u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
            )
            .unwrap_or(u64::MAX);
            self.with_inner(|inner| inner.requeue_dead_letter(job_id, now_ms))?;
            Ok(vec![QueueReplicateOp::RequeueDeadLetter {
                job_id: job_id.0,
                attempts: 0,
            }])
        })
    }

    fn metrics(&self) -> BoxFuture<'_, Result<QueueMetrics, QueueError>> {
        Box::pin(async move { Ok(self.with_inner(|inner| inner.metrics())) })
    }

    fn job_status(&self, job_id: JobId) -> BoxFuture<'_, Result<Option<JobStatus>, QueueError>> {
        Box::pin(async move { Ok(self.with_inner(|inner| inner.job_status(job_id))) })
    }

    fn list_jobs(&self, filter: JobListFilter) -> BoxFuture<'_, Result<JobListPage, QueueError>> {
        Box::pin(async move { Ok(self.with_inner(|inner| inner.list_jobs(&filter))) })
    }

    fn requeue_dead_letter_batch_replicated<'a>(
        &'a self,
        job_ids: &'a [JobId],
    ) -> BoxFuture<
        'a,
        Result<(Vec<JobId>, Vec<(JobId, QueueError)>, QueueReplicationOps), QueueError>,
    > {
        Box::pin(async move {
            let now_ms = u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
            )
            .unwrap_or(u64::MAX);
            let (requeued, failures) =
                self.with_inner(|inner| Ok(inner.requeue_dead_letter_batch(job_ids, now_ms)))?;
            let ops: QueueReplicationOps = requeued
                .iter()
                .map(|job_id| QueueReplicateOp::RequeueDeadLetter {
                    job_id: job_id.0,
                    attempts: 0,
                })
                .collect();
            Ok((requeued, failures, ops))
        })
    }

    fn peek_lease_meta(&self, lease_id: LeaseId) -> BoxFuture<'_, Option<(Option<Vec<u8>>, u32)>> {
        Box::pin(async move { self.peek_lease_for_settle(lease_id) })
    }
}

/// Optional workload governor integration for [`run_queue_consumer`].
pub struct QueueConsumerWorkload {
    /// Shared compute token pool (acquired per handler invocation).
    pub tokens: std::sync::Arc<crate::ComputeTokenPool>,
    /// Live consumer tuning from [`crate::run_workload_governor`].
    pub tune: tokio::sync::watch::Receiver<crate::ConsumerTune>,
}

/// Poll a [`JobQueue`], invoke `handle` on each leased job, then ack or nack.
///
/// Leases up to `batch` jobs per poll and acknowledges successes with
/// [`JobQueue::ack_batch`] (one leader transaction when using [`crate::ClusterJobQueue`]).
///
/// Runs until `stop` is set. When the queue is empty, sleeps `idle_sleep` between polls.
///
/// When `workload` is set, `batch` / `idle_sleep` come from the governor's
/// [`crate::ConsumerTune`] watch channel and each handler acquires `compute_cost`
/// token units from the pool (default 1).
#[allow(clippy::too_many_arguments)]
pub async fn run_queue_consumer<Q, F, Fut, E>(
    queue: std::sync::Arc<Q>,
    worker: WorkerId,
    batch: usize,
    idle_sleep: Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
    mut handle: F,
    workload: Option<QueueConsumerWorkload>,
    compute_cost: usize,
) where
    Q: JobQueue + ?Sized,
    F: FnMut(&LeasedJob) -> Fut,
    Fut: Future<Output = Result<(), E>>,
{
    let mut tune_rx = workload.as_ref().map(|w| w.tune.clone());
    loop {
        if *stop.borrow() {
            break;
        }
        let (batch, idle_sleep) = tune_rx.as_ref().map_or((batch.max(1), idle_sleep), |rx| {
            let tune = *rx.borrow();
            (tune.batch.max(1), tune.idle_sleep)
        });
        let Ok(jobs) = queue.lease(worker, batch).await else {
            tokio::time::sleep(idle_sleep).await;
            continue;
        };
        if jobs.is_empty() {
            if let Some(rx) = tune_rx.as_mut() {
                tokio::select! {
                    () = tokio::time::sleep(idle_sleep) => {}
                    _ = stop.changed() => {
                        if *stop.borrow() {
                            break;
                        }
                    }
                    _ = rx.changed() => {}
                }
            } else {
                tokio::select! {
                    () = tokio::time::sleep(idle_sleep) => {}
                    _ = stop.changed() => {
                        if *stop.borrow() {
                            break;
                        }
                    }
                }
            }
            continue;
        }
        let mut acks = Vec::with_capacity(jobs.len());
        let mut nacks = Vec::new();
        for job in jobs {
            let _token = if let Some(wl) = &workload {
                Some(wl.tokens.acquire_weighted(compute_cost).await)
            } else {
                None
            };
            match handle(&job).await {
                Ok(()) => acks.push(job.lease_id),
                Err(_) => nacks.push(job.lease_id),
            }
        }
        if !acks.is_empty() {
            let _ = queue.ack_batch(worker, &acks).await;
        }
        for lease_id in nacks {
            let _ = queue.nack(worker, lease_id).await;
        }
        if *stop.borrow() {
            break;
        }
    }
}

fn unix_ms_now() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn unix_ms_from_instant(deadline: Instant) -> u64 {
    let now_ms = unix_ms_now();
    let now = Instant::now();
    if deadline <= now {
        now_ms
    } else {
        now_ms.saturating_add(
            u64::try_from(deadline.duration_since(now).as_millis()).unwrap_or(u64::MAX),
        )
    }
}

fn instant_from_unix_ms(ms: u64) -> Instant {
    let now_ms = unix_ms_now();
    let now = Instant::now();
    if ms <= now_ms {
        now
    } else {
        now + Duration::from_millis(ms - now_ms)
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

        // First failure schedules ~1s backoff before the job is leasable again.
        tokio::time::sleep(Duration::from_millis(1100)).await;
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
        assert_eq!(m.leased, 0);
        // Reclaimed jobs wait out the retry backoff before counting as pending.
        assert_eq!(m.pending, 0);

        tokio::time::sleep(Duration::from_millis(1100)).await;
        let m = q.metrics().await.unwrap();
        assert_eq!(m.pending, 1);

        let again = q.lease(worker(1), 1).await.unwrap();
        assert_eq!(again[0].payload, b"z");
    }

    #[tokio::test]
    async fn extend_lease_prevents_reclaim() {
        let q = InMemoryJobQueue::new(Duration::from_millis(50));
        q.enqueue(b"long").await.unwrap();
        let leased = q.lease(worker(0), 1).await.unwrap();
        let lease_id = leased[0].lease_id;

        tokio::time::sleep(Duration::from_millis(40)).await;
        q.extend_lease(worker(0), lease_id).await.unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;

        let other = q.lease(worker(1), 1).await.unwrap();
        assert!(other.is_empty());
        let m = q.metrics().await.unwrap();
        assert_eq!(m.leased, 1);

        q.ack(worker(0), lease_id).await.unwrap();
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

    #[tokio::test]
    async fn job_status_reports_lifecycle() {
        let q = InMemoryJobQueue::new(Duration::from_secs(30));
        let id = q.enqueue(b"job").await.unwrap();
        let pending = q.job_status(id).await.unwrap().expect("pending");
        assert_eq!(pending.lifecycle, JobLifecycle::Pending);

        let leased = q.lease(worker(0), 1).await.unwrap();
        let status = q.job_status(id).await.unwrap().expect("leased");
        assert_eq!(status.lifecycle, JobLifecycle::Leased);
        q.ack(worker(0), leased[0].lease_id).await.unwrap();
        assert!(q.job_status(id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn max_attempts_moves_job_to_dead_letter() {
        let q = InMemoryJobQueue::new(Duration::from_secs(30));
        let id = q
            .enqueue_opts(b"poison", EnqueueOptions::max_attempts(2))
            .await
            .unwrap();
        for _ in 0..2 {
            let leased = q.lease(worker(0), 1).await.unwrap();
            q.nack(worker(0), leased[0].lease_id).await.unwrap();
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        let status = q.job_status(id).await.unwrap().expect("dead letter");
        assert_eq!(status.lifecycle, JobLifecycle::DeadLetter);
        assert_eq!(q.metrics().await.unwrap().dead_letter, 1);
        assert!(q.lease(worker(1), 1).await.unwrap().is_empty());

        q.requeue_dead_letter(id).await.unwrap();
        let pending = q.job_status(id).await.unwrap().expect("pending again");
        assert_eq!(pending.lifecycle, JobLifecycle::Pending);
        assert_eq!(pending.attempts, 0);
    }

    #[tokio::test]
    async fn list_jobs_filters_dead_letter() {
        let q = InMemoryJobQueue::new(Duration::from_secs(30)).default_max_attempts(1);
        let id = q.enqueue(b"x").await.unwrap();
        let leased = q.lease(worker(0), 1).await.unwrap();
        q.nack(worker(0), leased[0].lease_id).await.unwrap();

        let dl = q
            .list_jobs(JobListFilter {
                lifecycle: Some(JobLifecycle::DeadLetter),
                ..JobListFilter::default()
            })
            .await
            .unwrap();
        assert_eq!(dl.jobs.len(), 1);
        assert_eq!(dl.jobs[0].job_id, id);
        assert_eq!(dl.jobs[0].lifecycle, JobLifecycle::DeadLetter);
    }

    #[tokio::test]
    async fn requeue_dead_letter_batch_partial_success() {
        let q = InMemoryJobQueue::new(Duration::from_secs(30)).default_max_attempts(1);
        let id = q.enqueue(b"poison").await.unwrap();
        let leased = q.lease(worker(0), 1).await.unwrap();
        q.nack(worker(0), leased[0].lease_id).await.unwrap();

        let result = q
            .requeue_dead_letter_batch(&[id, JobId(999)])
            .await
            .unwrap();
        assert_eq!(result.requeued, vec![id]);
        assert_eq!(result.failures.len(), 1);
        assert_eq!(result.failures[0].0, JobId(999));
    }

    #[tokio::test]
    async fn stream_default_max_attempts_applies_when_job_leaves_it_unset() {
        let q = InMemoryJobQueue::new(Duration::from_secs(30)).default_max_attempts(2);
        // Plain enqueue: no per-job options at all.
        let id = q.enqueue(b"inherits").await.unwrap();
        assert_eq!(
            q.job_status(id)
                .await
                .unwrap()
                .expect("status")
                .max_attempts,
            2
        );

        for _ in 0..2 {
            let leased = q.lease(worker(0), 1).await.unwrap();
            q.nack(worker(0), leased[0].lease_id).await.unwrap();
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        assert_eq!(
            q.job_status(id).await.unwrap().expect("status").lifecycle,
            JobLifecycle::DeadLetter
        );
    }

    #[tokio::test]
    async fn explicit_max_attempts_overrides_stream_default() {
        let q = InMemoryJobQueue::new(Duration::from_secs(30)).default_max_attempts(2);

        // Explicit `0` is a request for unlimited retries, not "unset".
        let unlimited = q
            .enqueue_opts(b"unlimited", EnqueueOptions::max_attempts(0))
            .await
            .unwrap();
        assert_eq!(
            q.job_status(unlimited)
                .await
                .unwrap()
                .expect("status")
                .max_attempts,
            0
        );

        // A non-zero explicit ceiling wins over the stream default too.
        let capped = q
            .enqueue_opts(b"capped", EnqueueOptions::max_attempts(5))
            .await
            .unwrap();
        assert_eq!(
            q.job_status(capped)
                .await
                .unwrap()
                .expect("status")
                .max_attempts,
            5
        );
    }

    #[tokio::test]
    async fn zero_stream_default_keeps_retries_unlimited() {
        let q = InMemoryJobQueue::new(Duration::from_secs(30));
        let id = q.enqueue(b"forever").await.unwrap();
        assert_eq!(
            q.job_status(id)
                .await
                .unwrap()
                .expect("status")
                .max_attempts,
            0
        );
        for _ in 0..3 {
            let leased = q.lease(worker(0), 1).await.unwrap();
            q.nack(worker(0), leased[0].lease_id).await.unwrap();
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        // Still retrying (pending, or delayed by retry backoff) — never dead-lettered.
        let status = q.job_status(id).await.unwrap().expect("status");
        assert_ne!(status.lifecycle, JobLifecycle::DeadLetter);
        assert_eq!(status.attempts, 3);
        assert_eq!(q.metrics().await.unwrap().dead_letter, 0);
    }

    #[tokio::test]
    async fn redelivered_gauge_counts_jobs_that_failed_an_attempt() {
        let q = InMemoryJobQueue::new(Duration::from_secs(30));
        let id = q.enqueue(b"flaky").await.unwrap();
        assert_eq!(q.metrics().await.unwrap().redelivered, 0, "first delivery");

        let leased = q.lease(worker(0), 1).await.unwrap();
        assert_eq!(leased[0].attempts, 1, "attempts start at 1");
        q.nack(worker(0), leased[0].lease_id).await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;

        assert_eq!(
            q.metrics().await.unwrap().redelivered,
            1,
            "job awaiting redelivery is an idempotency smell"
        );

        let again = q.lease(worker(0), 1).await.unwrap();
        assert_eq!(again[0].attempts, 2, "redelivery reports attempt 2");
        q.ack(worker(0), again[0].lease_id).await.unwrap();
        assert_eq!(
            q.metrics().await.unwrap().redelivered,
            0,
            "acked job leaves the queue"
        );
        let _ = id;
    }

    #[tokio::test]
    async fn enqueue_batch_and_ack_batch_round_trip() {
        let q = InMemoryJobQueue::new(Duration::from_secs(30));
        let ids = q
            .enqueue_batch(&[b"a".as_slice(), b"b", b"c"])
            .await
            .unwrap();
        assert_eq!(ids.len(), 3);

        let leased = q.lease(worker(0), 8).await.unwrap();
        assert_eq!(leased.len(), 3);
        let lease_ids: Vec<_> = leased.iter().map(|j| j.lease_id).collect();
        q.ack_batch(worker(0), &lease_ids).await.unwrap();
        assert_eq!(q.metrics().await.unwrap().pending, 0);
    }

    #[tokio::test]
    async fn run_queue_consumer_acks_leased_batch() {
        use std::sync::Arc;

        let q = Arc::new(InMemoryJobQueue::new(Duration::from_secs(30)));
        q.enqueue_batch(&[b"1".as_slice(), b"2"]).await.unwrap();
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let worker = worker(0);
        let queue = Arc::clone(&q);
        let consumer = tokio::spawn(async move {
            run_queue_consumer(
                queue,
                worker,
                8,
                Duration::from_millis(10),
                stop_rx,
                |job| {
                    let len = job.payload.len();
                    let attempts = job.attempts;
                    async move {
                        assert!(len > 0);
                        assert_eq!(attempts, 1, "first delivery");
                        Ok::<(), ()>(())
                    }
                },
                None,
                1,
            )
            .await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        stop_tx.send(true).unwrap();
        consumer.await.unwrap();
        assert_eq!(q.metrics().await.unwrap().pending, 0);
    }

    #[tokio::test]
    async fn run_queue_consumer_finishes_in_flight_batch_on_stop() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let q = Arc::new(InMemoryJobQueue::new(Duration::from_secs(30)));
        q.enqueue_batch(&[b"a".as_slice(), b"b"]).await.unwrap();
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let processed = Arc::new(AtomicUsize::new(0));
        let worker = worker(0);
        let queue = Arc::clone(&q);
        let processed_in_task = Arc::clone(&processed);
        let consumer = tokio::spawn(async move {
            run_queue_consumer(
                queue,
                worker,
                8,
                Duration::from_millis(10),
                stop_rx,
                move |_job| {
                    let processed = Arc::clone(&processed_in_task);
                    async move {
                        processed.fetch_add(1, Ordering::SeqCst);
                        Ok::<(), ()>(())
                    }
                },
                None,
                1,
            )
            .await;
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        stop_tx.send(true).unwrap();
        consumer.await.unwrap();
        assert_eq!(
            processed.load(Ordering::SeqCst),
            2,
            "both leased jobs must finish"
        );
        assert_eq!(q.metrics().await.unwrap().pending, 0);
    }
}
