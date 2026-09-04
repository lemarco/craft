//! Durable job backlog port ([job-queue](../../../docs/decisions/job-queue.md)).
//!
//! [`JobQueue`] is the job queue layer: shared async work with `lease` / `ack`,
//! distinct from actor mailboxes and Raft consensus. [`InMemoryJobQueue`]
//! backs tests and single-node dev; production uses [`RedbJobQueue`](crate::redb_queue::RedbJobQueue).

mod consumer;
mod in_memory;
mod port;
#[cfg(test)]
mod tests;
mod time;
mod types;

pub use consumer::{QueueConsumerWorkload, run_queue_consumer};
pub use in_memory::InMemoryJobQueue;
pub use port::JobQueue;
pub use types::{
    BatchRequeueResult, EnqueueOptions, JobContext, JobId, JobLifecycle, JobListFilter,
    JobListPage, JobStatus, LIST_JOBS_DEFAULT_LIMIT, LeaseId, LeasedJob, QueueError, QueueMetrics,
    QueueReplicationOps, job_status_matches_filter,
};
