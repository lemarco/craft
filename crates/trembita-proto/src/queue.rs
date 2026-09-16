//! Job queue wire types ([job-queue](../../../docs/decisions/job-queue.md)).

use serde::{Deserialize, Serialize};

use crate::product::ProductWireError;
use crate::{DedupKey, JobId, JobPriority, LeaseId, MaxAttempts, NodeId, StreamName, UnixMillis};

/// Enqueue a job on stream `stream` (`POST /raft/v1/queue/enqueue`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueEnqueueRequest {
    /// Logical queue stream name (e.g. `"jobs"` or sharded `"jobs~0"`).
    pub stream: StreamName,
    /// Opaque job body handed to workers after lease.
    pub payload: Vec<u8>,
    /// Higher values are leased before lower (default `0`).
    #[serde(default)]
    pub priority: JobPriority,
    /// Earliest wall time (unix ms) the job may be leased; `0` = immediately.
    #[serde(default)]
    pub not_before_ms: UnixMillis,
    /// Optional routing key for sharded streams (defaults to hashing `payload`).
    #[serde(default)]
    pub shard_key: Option<Vec<u8>>,
    /// Idempotency key — retries return the same `job_id` while the job exists.
    #[serde(default)]
    pub dedup_key: Option<DedupKey>,
    /// Maximum delivery attempts before dead letter (`0` = unlimited).
    #[serde(default)]
    pub max_attempts: MaxAttempts,
}

/// Response to [`QueueEnqueueRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueEnqueueReply {
    /// Assigned job id when enqueue succeeded.
    pub job_id: Option<JobId>,
    /// Human-readable error when enqueue failed.
    pub error: Option<ProductWireError>,
}

/// One job in a batch enqueue (`POST /raft/v1/queue/enqueue-batch`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueBatchEnqueueJob {
    /// Opaque job body.
    pub payload: Vec<u8>,
    /// Higher values are leased before lower (default `0`).
    #[serde(default)]
    pub priority: JobPriority,
    /// Earliest wall time (unix ms) the job may be leased; `0` = immediately.
    #[serde(default)]
    pub not_before_ms: UnixMillis,
    /// Optional routing key for sharded streams.
    #[serde(default)]
    pub shard_key: Option<Vec<u8>>,
    /// Idempotency key — retries return the same `job_id` while the job exists.
    #[serde(default)]
    pub dedup_key: Option<DedupKey>,
    /// Maximum delivery attempts before dead letter (`0` = unlimited).
    #[serde(default)]
    pub max_attempts: MaxAttempts,
}

/// Enqueue many jobs in one leader transaction (`POST /raft/v1/queue/enqueue-batch`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueEnqueueBatchRequest {
    /// Logical queue stream name.
    pub stream: StreamName,
    /// Jobs to append (leader caps batch size).
    pub jobs: Vec<QueueBatchEnqueueJob>,
}

/// Response to [`QueueEnqueueBatchRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueEnqueueBatchReply {
    /// Assigned ids in the same order as the request (dedup hits echo existing ids).
    pub job_ids: Vec<JobId>,
    /// Set when the batch failed before any job was committed.
    pub error: Option<ProductWireError>,
}

/// Lease jobs for a worker (`POST /raft/v1/queue/lease`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueLeaseRequest {
    /// Queue stream to pull from.
    pub stream: StreamName,
    /// [`NodeId`] of the leasing worker (`.0` wire encoding).
    pub worker_node: NodeId,
    /// Worker actor instance id on that node.
    pub worker_instance: u32,
    /// Maximum jobs to lease in one call.
    pub max: usize,
}

/// One job returned under lease on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueLeasedJobWire {
    /// Lease token — required for ack/nack.
    pub lease_id: LeaseId,
    /// Job id within the stream.
    pub job_id: JobId,
    /// Job body copied at enqueue time.
    pub payload: Vec<u8>,
    /// Delivery attempts including this one (`1` on first delivery).
    pub attempts: u32,
    /// Client idempotency token supplied at enqueue, when there was one.
    pub dedup_key: Option<DedupKey>,
}

/// Response to [`QueueLeaseRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueLeaseReply {
    /// Leased jobs (may be empty when the queue is idle).
    pub jobs: Vec<QueueLeasedJobWire>,
    /// Set when the lease RPC failed.
    pub error: Option<ProductWireError>,
}

/// Acknowledge successful processing (`POST /raft/v1/queue/ack`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueAckRequest {
    /// Queue stream the lease belongs to.
    pub stream: StreamName,
    /// Leasing worker node id.
    pub worker_node: NodeId,
    /// Leasing worker instance id.
    pub worker_instance: u32,
    /// Lease token from [`QueueLeasedJobWire::lease_id`].
    pub lease_id: LeaseId,
}

/// Response to [`QueueAckRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueAckReply {
    /// Set when ack failed (unknown lease, wrong worker, etc.).
    pub error: Option<ProductWireError>,
}

/// Acknowledge many leased jobs in one leader transaction
/// (`POST /raft/v1/queue/ack-batch`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueAckBatchRequest {
    /// Queue stream the leases belong to.
    pub stream: StreamName,
    /// Leasing worker node id.
    pub worker_node: NodeId,
    /// Leasing worker instance id.
    pub worker_instance: u32,
    /// Lease tokens from [`QueueLeasedJobWire::lease_id`].
    pub lease_ids: Vec<LeaseId>,
}

/// Response to [`QueueAckBatchRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueAckBatchReply {
    /// Set when the batch ack failed.
    pub error: Option<ProductWireError>,
}

/// Return a leased job to pending immediately (`POST /raft/v1/queue/nack`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueNackRequest {
    /// Queue stream the lease belongs to.
    pub stream: StreamName,
    /// Leasing worker node id.
    pub worker_node: NodeId,
    /// Leasing worker instance id.
    pub worker_instance: u32,
    /// Lease token to release.
    pub lease_id: LeaseId,
}

/// Response to [`QueueNackRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueNackReply {
    /// Set when nack failed.
    pub error: Option<ProductWireError>,
}

/// Extend a live lease visibility timeout (`POST /raft/v1/queue/extend-lease`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueExtendLeaseRequest {
    /// Queue stream the lease belongs to.
    pub stream: StreamName,
    /// Leasing worker node id.
    pub worker_node: NodeId,
    /// Leasing worker instance id.
    pub worker_instance: u32,
    /// Lease token to extend.
    pub lease_id: LeaseId,
}

/// Response to [`QueueExtendLeaseRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueExtendLeaseReply {
    /// Set when extend failed.
    pub error: Option<ProductWireError>,
}

/// Read queue depth gauges (`POST /raft/v1/queue/metrics`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueMetricsRequest {
    /// Stream to inspect.
    pub stream: StreamName,
}

/// Depth and age gauges for autoscale / observability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueMetricsReply {
    /// Jobs waiting to be leased.
    pub pending: u64,
    /// Jobs currently leased to workers.
    pub leased: u64,
    /// Jobs in the dead-letter set (exhausted retries).
    pub dead_letter: u64,
    /// Age in ms of the oldest ready pending job (`0` when empty).
    pub oldest_pending_age_ms: u64,
    /// Jobs that have already failed at least one attempt (idempotency smell).
    pub redelivered: u64,
    /// Set when metrics collection failed.
    pub error: Option<ProductWireError>,
}

/// Job lifecycle returned by [`QueueJobStatusReply`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum QueueJobLifecycleWire {
    /// Waiting in pending (ready to lease).
    Pending = 0,
    /// Currently leased to a worker.
    Leased = 1,
    /// Delayed until `not_before`.
    Delayed = 2,
    /// Exhausted retry budget — not leased until requeued by an operator.
    DeadLetter = 3,
}

/// Lookup job metadata by id (`POST /raft/v1/queue/job-status`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueJobStatusRequest {
    /// Stream to inspect.
    pub stream: StreamName,
    /// Job id within the stream (global id when sharded).
    pub job_id: JobId,
}

/// Metadata for a single job (`POST /raft/v1/queue/job-status`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueJobStatusReply {
    /// `true` when the job exists (pending, leased, or delayed).
    pub found: bool,
    /// Echo of the requested id.
    pub job_id: JobId,
    /// Set when [`Self::found`] is true.
    pub lifecycle: Option<QueueJobLifecycleWire>,
    /// Byte length of stored payload.
    pub payload_len: u64,
    /// Enqueue priority.
    pub priority: JobPriority,
    /// Worker node when leased.
    pub leased_worker_node: Option<NodeId>,
    /// Worker instance when leased.
    pub leased_worker_instance: Option<u32>,
    /// Delivery attempts so far (including the attempt that dead-lettered).
    pub attempts: u32,
    /// Configured retry ceiling (`0` = unlimited).
    pub max_attempts: MaxAttempts,
    /// Client idempotency token from enqueue, when set.
    #[serde(default)]
    pub dedup_key: Option<DedupKey>,
    /// Set when lookup failed.
    pub error: Option<ProductWireError>,
}

const fn default_true() -> bool {
    true
}

/// Cron-driven recurring job registered on a queue stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecurringScheduleWire {
    /// Unique schedule name within the stream.
    pub name: String,
    /// Cron expression (5-field `min hour dom month dow` or 6-field with seconds).
    ///
    /// Empty when [`every_days`](Self::every_days) is set (calendar interval mode).
    pub cron: String,
    /// Fire every N **calendar** days from [`anchor_ms`](Self::anchor_ms) (`0` = use `cron`).
    #[serde(default)]
    pub every_days: u32,
    /// First fire instant (unix ms) for calendar interval mode.
    #[serde(default)]
    pub anchor_ms: u64,
    /// Payload enqueued on each tick.
    pub payload: Vec<u8>,
    /// Passed to [`QueueEnqueueRequest::priority`].
    #[serde(default)]
    pub priority: JobPriority,
    /// Passed to [`QueueEnqueueRequest::max_attempts`].
    #[serde(default)]
    pub max_attempts: MaxAttempts,
    /// When false the schedule is stored but does not fire.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Next fire time (unix ms); leader-maintained.
    #[serde(default)]
    pub next_run_ms: u64,
}

/// Idempotent state transition replicated from the queue leader to every voter
/// (`POST /raft/v1/queue/replicate`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueReplicateOp {
    /// Append a job and advance the stream's `next_job_id`.
    Enqueue {
        /// Assigned job id.
        job_id: JobId,
        /// Job body.
        payload: Vec<u8>,
        /// Leader wall time at enqueue (unix ms).
        enqueued_at_ms: UnixMillis,
        /// Monotonic id generator after this enqueue.
        next_job_id: JobId,
        #[serde(default)]
        /// Lease priority (higher first).
        priority: JobPriority,
        #[serde(default)]
        /// Earliest lease time (unix ms).
        not_before_ms: UnixMillis,
        #[serde(default)]
        /// Optional dedup key index update.
        dedup_key: Option<DedupKey>,
        #[serde(default)]
        /// Attempts already recorded for this job.
        attempts: u32,
        #[serde(default)]
        /// Retry ceiling (`0` = unlimited).
        max_attempts: MaxAttempts,
    },
    /// Move a job from pending to leased.
    Lease {
        /// New lease token.
        lease_id: LeaseId,
        /// Job being leased.
        job_id: JobId,
        /// Worker node id.
        worker_node: NodeId,
        /// Worker instance id.
        worker_instance: u32,
        /// Lease expiry (unix ms; followers may use local timeout).
        expires_at_ms: UnixMillis,
        /// Monotonic lease id generator after this lease.
        next_lease_id: LeaseId,
    },
    /// Job completed — remove job and lease rows.
    Ack {
        /// Released lease.
        lease_id: LeaseId,
        /// Completed job.
        job_id: JobId,
    },
    /// Worker rejected the job — return to pending or dead letter.
    Nack {
        /// Released lease.
        lease_id: LeaseId,
        /// Requeued or dead-lettered job.
        job_id: JobId,
        #[serde(default)]
        /// Attempt count after this failure.
        attempts: u32,
        #[serde(default)]
        /// When true the job is in the dead-letter set, not pending.
        dead_letter: bool,
        #[serde(default)]
        /// Earliest re-lease time (unix ms) when requeued.
        not_before_ms: UnixMillis,
    },
    /// Visibility timeout expired — job returns to pending or dead letter.
    Reclaim {
        /// Expired lease.
        lease_id: LeaseId,
        /// Requeued or dead-lettered job.
        job_id: JobId,
        #[serde(default)]
        /// Attempt count after this failure.
        attempts: u32,
        #[serde(default)]
        /// When true the job is in the dead-letter set, not pending.
        dead_letter: bool,
        #[serde(default)]
        /// Earliest re-lease time (unix ms) when requeued.
        not_before_ms: UnixMillis,
    },
    /// Worker heartbeat — push lease expiry forward without completing the job.
    ExtendLease {
        /// Live lease token.
        lease_id: LeaseId,
        /// Leasing worker node id.
        worker_node: NodeId,
        /// Leasing worker instance id.
        worker_instance: u32,
        /// New lease expiry (unix ms).
        expires_at_ms: UnixMillis,
    },
    /// Operator moved a dead-letter job back to pending.
    RequeueDeadLetter {
        /// Job id to retry.
        job_id: JobId,
        #[serde(default)]
        /// Reset attempt counter (usually `0`).
        attempts: u32,
    },
    /// Upsert a cron schedule (builder / operator).
    UpsertSchedule {
        /// Schedule body.
        schedule: RecurringScheduleWire,
    },
    /// Leader advanced a schedule after enqueueing its tick.
    UpdateScheduleNextRun {
        /// Schedule name within the stream.
        name: String,
        /// Next fire time (unix ms).
        next_run_ms: u64,
    },
    /// Remove a recurring schedule no longer present in the desired set.
    RemoveSchedule {
        /// Schedule name within the stream.
        name: String,
    },
}

/// Batch of replication ops from the queue leader (`POST /raft/v1/queue/replicate`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueReplicateRequest {
    /// Target stream.
    pub stream: StreamName,
    /// Idempotent mutations to apply in order.
    pub ops: Vec<QueueReplicateOp>,
    /// Declared Raft leader id (must match the receiver's leader hint).
    pub leader_id: NodeId,
}

/// Response to [`QueueReplicateRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueReplicateReply {
    /// Set when replication apply failed.
    pub error: Option<ProductWireError>,
}

/// Requeue a dead-letter job for another delivery attempt
/// (`POST /raft/v1/queue/requeue-dead-letter`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueRequeueDeadLetterRequest {
    /// Queue stream.
    pub stream: StreamName,
    /// Job id in the dead-letter set.
    pub job_id: JobId,
}

/// Response to [`QueueRequeueDeadLetterRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueRequeueDeadLetterReply {
    /// Set when requeue failed.
    pub error: Option<ProductWireError>,
}

/// List jobs in a stream with optional filters (`POST /raft/v1/queue/list-jobs`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueListJobsRequest {
    /// Queue stream.
    pub stream: StreamName,
    /// When set, only jobs in this lifecycle phase.
    pub lifecycle: Option<QueueJobLifecycleWire>,
    /// When set, only jobs with `attempts >= min_attempts`.
    pub min_attempts: Option<u32>,
    /// When set, only jobs with this exact dedup key.
    #[serde(default)]
    pub dedup_key: Option<DedupKey>,
    /// Maximum rows to return (capped server-side).
    #[serde(default = "default_list_jobs_limit")]
    pub limit: u32,
    /// Pagination cursor — return jobs with id strictly greater than this.
    #[serde(default)]
    pub after_job_id: JobId,
}

const fn default_list_jobs_limit() -> u32 {
    50
}

/// One row in [`QueueListJobsReply::jobs`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueJobListEntryWire {
    /// Job id within the stream (global id when sharded).
    pub job_id: JobId,
    /// Current lifecycle phase.
    pub lifecycle: QueueJobLifecycleWire,
    /// Byte length of stored payload.
    pub payload_len: u64,
    /// Enqueue priority.
    pub priority: JobPriority,
    /// Worker node when leased.
    pub leased_worker_node: Option<NodeId>,
    /// Worker instance when leased.
    pub leased_worker_instance: Option<u32>,
    /// Delivery attempts so far.
    pub attempts: u32,
    /// Configured retry ceiling (`0` = unlimited).
    pub max_attempts: MaxAttempts,
    /// Client idempotency token from enqueue, when set.
    #[serde(default)]
    pub dedup_key: Option<DedupKey>,
}

/// Response to [`QueueListJobsRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueListJobsReply {
    /// Matching jobs in ascending job-id order.
    pub jobs: Vec<QueueJobListEntryWire>,
    /// `true` when more rows exist beyond this page.
    pub has_more: bool,
    /// Set when listing failed.
    pub error: Option<ProductWireError>,
}

/// Requeue many dead-letter jobs (`POST /raft/v1/queue/requeue-dead-letter-batch`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueRequeueDeadLetterBatchRequest {
    /// Queue stream.
    pub stream: StreamName,
    /// Dead-letter job ids to move back to pending.
    pub job_ids: Vec<JobId>,
}

/// Per-job failure in a batch requeue response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueRequeueFailureWire {
    /// Job id that could not be requeued.
    pub job_id: JobId,
    /// Why requeue failed for this id.
    pub error: String,
}

/// List recurring cron schedules on a stream (`POST /raft/v1/queue/list-schedules`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueListSchedulesRequest {
    /// Queue stream.
    pub stream: StreamName,
}

/// Response to [`QueueListSchedulesRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueListSchedulesReply {
    /// Schedules stored in the stream queue file (includes leader `next_run_ms`).
    pub schedules: Vec<RecurringScheduleWire>,
    /// Set when listing failed.
    pub error: Option<ProductWireError>,
}

/// Upsert a recurring schedule (`POST /raft/v1/queue/upsert-schedule`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueUpsertScheduleRequest {
    /// Queue stream.
    pub stream: StreamName,
    /// Schedule body (`next_run_ms` is recomputed on the leader when unset or zero).
    pub schedule: RecurringScheduleWire,
}

/// Response to [`QueueUpsertScheduleRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueUpsertScheduleReply {
    /// Set when upsert failed.
    pub error: Option<ProductWireError>,
}

/// Remove a recurring schedule by name (`POST /raft/v1/queue/remove-schedule`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueRemoveScheduleRequest {
    /// Queue stream.
    pub stream: StreamName,
    /// Schedule name within the stream.
    pub name: String,
}

/// Response to [`QueueRemoveScheduleRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueRemoveScheduleReply {
    /// Set when removal failed.
    pub error: Option<ProductWireError>,
}

/// Response to [`QueueRequeueDeadLetterBatchRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueRequeueDeadLetterBatchReply {
    /// Job ids successfully moved back to pending.
    pub requeued: Vec<JobId>,
    /// Per-id failures (not dead letter, unknown id, …).
    pub failures: Vec<QueueRequeueFailureWire>,
    /// Set when the whole request failed before per-id processing.
    pub error: Option<ProductWireError>,
}
