//! Job queue wire types ([job-queue](../../../docs/decisions/job-queue.md)).

use serde::{Deserialize, Serialize};

/// Enqueue a job on stream `stream` (`POST /raft/v1/queue/enqueue`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueEnqueueRequest {
    /// Logical queue stream name (e.g. `"jobs"` or sharded `"jobs~0"`).
    pub stream: String,
    /// Opaque job body handed to workers after lease.
    pub payload: Vec<u8>,
    /// Higher values are leased before lower (default `0`).
    #[serde(default)]
    pub priority: u8,
    /// Earliest wall time (unix ms) the job may be leased; `0` = immediately.
    #[serde(default)]
    pub not_before_ms: u64,
    /// Optional routing key for sharded streams (defaults to hashing `payload`).
    #[serde(default)]
    pub shard_key: Option<Vec<u8>>,
    /// Idempotency key — retries return the same `job_id` while the job exists.
    #[serde(default)]
    pub dedup_key: Option<Vec<u8>>,
}

/// Response to [`QueueEnqueueRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueEnqueueReply {
    /// Assigned job id when enqueue succeeded.
    pub job_id: Option<u64>,
    /// Human-readable error when enqueue failed.
    pub error: Option<String>,
}

/// Lease jobs for a worker (`POST /raft/v1/queue/lease`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueLeaseRequest {
    /// Queue stream to pull from.
    pub stream: String,
    /// [`NodeId`](crate::NodeId) of the leasing worker (`.0` wire encoding).
    pub worker_node: u64,
    /// Worker actor instance id on that node.
    pub worker_instance: u32,
    /// Maximum jobs to lease in one call.
    pub max: usize,
}

/// One job returned under lease on the wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueLeasedJobWire {
    /// Lease token — required for ack/nack.
    pub lease_id: u64,
    /// Job id within the stream.
    pub job_id: u64,
    /// Job body copied at enqueue time.
    pub payload: Vec<u8>,
}

/// Response to [`QueueLeaseRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueLeaseReply {
    /// Leased jobs (may be empty when the queue is idle).
    pub jobs: Vec<QueueLeasedJobWire>,
    /// Set when the lease RPC failed.
    pub error: Option<String>,
}

/// Acknowledge successful processing (`POST /raft/v1/queue/ack`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueAckRequest {
    /// Queue stream the lease belongs to.
    pub stream: String,
    /// Leasing worker node id.
    pub worker_node: u64,
    /// Leasing worker instance id.
    pub worker_instance: u32,
    /// Lease token from [`QueueLeasedJobWire::lease_id`].
    pub lease_id: u64,
}

/// Response to [`QueueAckRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueAckReply {
    /// Set when ack failed (unknown lease, wrong worker, etc.).
    pub error: Option<String>,
}

/// Return a leased job to pending immediately (`POST /raft/v1/queue/nack`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueNackRequest {
    /// Queue stream the lease belongs to.
    pub stream: String,
    /// Leasing worker node id.
    pub worker_node: u64,
    /// Leasing worker instance id.
    pub worker_instance: u32,
    /// Lease token to release.
    pub lease_id: u64,
}

/// Response to [`QueueNackRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueNackReply {
    /// Set when nack failed.
    pub error: Option<String>,
}

/// Read queue depth gauges (`POST /raft/v1/queue/metrics`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueMetricsRequest {
    /// Stream to inspect.
    pub stream: String,
}

/// Depth and age gauges for autoscale / observability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueMetricsReply {
    /// Jobs waiting to be leased.
    pub pending: u64,
    /// Jobs currently leased to workers.
    pub leased: u64,
    /// Age in ms of the oldest ready pending job (`0` when empty).
    pub oldest_pending_age_ms: u64,
    /// Set when metrics collection failed.
    pub error: Option<String>,
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
}

/// Lookup job metadata by id (`POST /raft/v1/queue/job-status`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueJobStatusRequest {
    /// Stream to inspect.
    pub stream: String,
    /// Job id within the stream (global id when sharded).
    pub job_id: u64,
}

/// Metadata for a single job (`POST /raft/v1/queue/job-status`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueJobStatusReply {
    /// `true` when the job exists (pending, leased, or delayed).
    pub found: bool,
    /// Echo of the requested id.
    pub job_id: u64,
    /// Set when [`Self::found`] is true.
    pub lifecycle: Option<QueueJobLifecycleWire>,
    /// Byte length of stored payload.
    pub payload_len: u64,
    /// Enqueue priority.
    pub priority: u8,
    /// Worker node when leased.
    pub leased_worker_node: Option<u64>,
    /// Worker instance when leased.
    pub leased_worker_instance: Option<u32>,
    /// Set when lookup failed.
    pub error: Option<String>,
}

/// Idempotent state transition replicated from the queue leader to every voter
/// (`POST /raft/v1/queue/replicate`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueReplicateOp {
    /// Append a job and advance the stream's `next_job_id`.
    Enqueue {
        /// Assigned job id.
        job_id: u64,
        /// Job body.
        payload: Vec<u8>,
        /// Leader wall time at enqueue (unix ms).
        enqueued_at_ms: u64,
        /// Monotonic id generator after this enqueue.
        next_job_id: u64,
        #[serde(default)]
        /// Lease priority (higher first).
        priority: u8,
        #[serde(default)]
        /// Earliest lease time (unix ms).
        not_before_ms: u64,
        #[serde(default)]
        /// Optional dedup key index update.
        dedup_key: Option<Vec<u8>>,
    },
    /// Move a job from pending to leased.
    Lease {
        /// New lease token.
        lease_id: u64,
        /// Job being leased.
        job_id: u64,
        /// Worker node id.
        worker_node: u64,
        /// Worker instance id.
        worker_instance: u32,
        /// Lease expiry (unix ms; followers may use local timeout).
        expires_at_ms: u64,
        /// Monotonic lease id generator after this lease.
        next_lease_id: u64,
    },
    /// Job completed — remove job and lease rows.
    Ack {
        /// Released lease.
        lease_id: u64,
        /// Completed job.
        job_id: u64,
    },
    /// Worker rejected the job — return to pending.
    Nack {
        /// Released lease.
        lease_id: u64,
        /// Requeued job.
        job_id: u64,
    },
    /// Visibility timeout expired — job returns to pending.
    Reclaim {
        /// Expired lease.
        lease_id: u64,
        /// Requeued job.
        job_id: u64,
    },
}

/// Batch of replication ops from the queue leader (`POST /raft/v1/queue/replicate`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueReplicateRequest {
    /// Target stream.
    pub stream: String,
    /// Idempotent mutations to apply in order.
    pub ops: Vec<QueueReplicateOp>,
}

/// Response to [`QueueReplicateRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueReplicateReply {
    /// Set when replication apply failed.
    pub error: Option<String>,
}
