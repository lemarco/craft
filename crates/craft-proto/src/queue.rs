//! Job queue wire types ([job-queue](../../../docs/decisions/job-queue.md)).

use serde::{Deserialize, Serialize};

/// Enqueue a job on stream `stream` (`POST /raft/v1/queue/enqueue`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueEnqueueRequest {
    pub stream: String,
    pub payload: Vec<u8>,
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
