use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use trembita_proto::{QueueReplicateOp, WorkerId};

use super::port::JobQueue;

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
    /// Routes to a shard when using [`ShardedJobQueue`](crate::sharded_queue::ShardedJobQueue).
    pub shard_key: Option<Vec<u8>>,
    /// Client-supplied idempotency token; duplicate enqueues return the same [`JobId`]
    /// while a job with that key exists (pending, leased, delayed, or dead-letter).
    ///
    /// The key is **released after ack** — once the job is removed from the queue,
    /// the same token may be used to enqueue a new job. See
    /// [background-jobs § `dedup_key` lifecycle](../../../docs/scenarios/background-jobs.md#dedup_key-lifecycle).
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
    ///
    /// While a job with this key exists in the queue, duplicate enqueues return the
    /// same [`JobId`]. The slot is **released after ack**, so the same key can
    /// enqueue again once the prior job completes successfully.
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
    /// Job id within the stream (global id when using [`ShardedJobQueue`](crate::sharded_queue::ShardedJobQueue)).
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
