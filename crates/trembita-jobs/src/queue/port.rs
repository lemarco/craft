use trembita_proto::{BoxFuture, QueueReplicateOp, WorkerId};

use super::types::{
    BatchRequeueResult, EnqueueOptions, JobId, JobListFilter, JobListPage, JobStatus, LeaseId,
    LeasedJob, QueueError, QueueMetrics, QueueReplicationOps,
};

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
