//! Job queue wire types ([job-queue](../../../docs/decisions/job-queue.md)).

use serde::{Deserialize, Serialize};

/// Enqueue a job on stream `stream` (`POST /raft/v1/queue/enqueue`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueEnqueueRequest {
    pub stream: String,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueEnqueueReply {
    pub job_id: Option<u64>,
    pub error: Option<String>,
}

/// Lease jobs for a worker (`POST /raft/v1/queue/lease`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueLeaseRequest {
    pub stream: String,
    pub worker_node: u64,
    pub worker_instance: u32,
    pub max: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueLeasedJobWire {
    pub lease_id: u64,
    pub job_id: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueLeaseReply {
    pub jobs: Vec<QueueLeasedJobWire>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueAckRequest {
    pub stream: String,
    pub worker_node: u64,
    pub worker_instance: u32,
    pub lease_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueAckReply {
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueNackRequest {
    pub stream: String,
    pub worker_node: u64,
    pub worker_instance: u32,
    pub lease_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueNackReply {
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueMetricsRequest {
    pub stream: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueMetricsReply {
    pub pending: u64,
    pub leased: u64,
    pub oldest_pending_age_ms: u64,
    pub error: Option<String>,
}

/// Idempotent state transition replicated from the queue leader to every voter
/// (`POST /raft/v1/queue/replicate`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueReplicateOp {
    Enqueue {
        job_id: u64,
        payload: Vec<u8>,
        enqueued_at_ms: u64,
        next_job_id: u64,
        #[serde(default)]
        priority: u8,
        #[serde(default)]
        not_before_ms: u64,
        #[serde(default)]
        dedup_key: Option<Vec<u8>>,
    },
    Lease {
        lease_id: u64,
        job_id: u64,
        worker_node: u64,
        worker_instance: u32,
        expires_at_ms: u64,
        next_lease_id: u64,
    },
    Ack {
        lease_id: u64,
        job_id: u64,
    },
    Nack {
        lease_id: u64,
        job_id: u64,
    },
    /// Visibility timeout expired — job returns to pending.
    Reclaim {
        lease_id: u64,
        job_id: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueReplicateRequest {
    pub stream: String,
    pub ops: Vec<QueueReplicateOp>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueReplicateReply {
    pub error: Option<String>,
}
