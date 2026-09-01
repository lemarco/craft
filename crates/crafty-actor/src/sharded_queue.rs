//! Federated [`JobQueue`] over multiple streams ([job-queue](../../../docs/decisions/job-queue.md)).
//!
//! Spreads enqueue/load across independent queue shards (separate redb files and
//! replication paths) while presenting one logical queue to producers/consumers.

use std::collections::HashMap;
use std::sync::Arc;

use super::{
    BoxFuture, EnqueueOptions, JobId, JobQueue, JobStatus, LeaseId, LeasedJob, QueueError,
    QueueMetrics, QueueReplicationOps, WorkerId,
};

const SHARD_SHIFT: u32 = 56;
const LOCAL_MASK: u64 = (1u64 << SHARD_SHIFT) - 1;

fn stable_hash(key: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325_u64;
    for b in key {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

fn encode_id(shard: usize, local: u64) -> u64 {
    ((shard as u64) << SHARD_SHIFT) | (local & LOCAL_MASK)
}

fn decode_id(id: u64) -> (usize, u64) {
    ((id >> SHARD_SHIFT) as usize, id & LOCAL_MASK)
}

/// Split a global sharded job/lease id into `(shard_index, local_id)`.
pub(crate) fn decode_global_id(id: u64) -> (usize, u64) {
    decode_id(id)
}

/// Combine shard index and local id into the global wire id.
pub(crate) fn encode_global_id(shard: usize, local: u64) -> u64 {
    encode_id(shard, local)
}

/// Routes jobs across `shards` by hashing the shard key (or payload).
pub struct ShardedJobQueue {
    shards: Vec<Arc<dyn JobQueue>>,
}

impl ShardedJobQueue {
    /// Federate existing queue clients/shards (length ≥ 1).
    ///
    /// # Panics
    /// If `shards` is empty.
    #[must_use]
    pub fn new(shards: Vec<Arc<dyn JobQueue>>) -> Self {
        assert!(
            !shards.is_empty(),
            "ShardedJobQueue requires at least one shard"
        );
        Self { shards }
    }

    /// Number of federated shards.
    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    fn pick_shard(&self, payload: &[u8], shard_key: Option<&[u8]>) -> usize {
        let key = shard_key.unwrap_or(payload);
        usize::try_from(stable_hash(key)).unwrap_or(usize::MAX) % self.shards.len()
    }

    fn shard(&self, index: usize) -> Result<&Arc<dyn JobQueue>, QueueError> {
        self.shards
            .get(index)
            .ok_or_else(|| QueueError::Backend(format!("invalid shard index {index}")))
    }
}

/// One shard's replication batch plus its index (for wire stream `{name}~{shard}`).
#[derive(Debug, Clone)]
pub struct ShardedReplication {
    /// Index into the shard vector passed to [`ShardedJobQueue::new`].
    pub shard: usize,
    /// Replication ops produced by that shard's leader mutation.
    pub ops: QueueReplicationOps,
}

impl ShardedJobQueue {
    /// Enqueue on the routed shard; returns global job id and replication ops for that shard only.
    ///
    /// # Errors
    /// Returns [`QueueError`] if routing or the shard enqueue fails.
    pub async fn enqueue_opts_replicated_sharded(
        &self,
        payload: &[u8],
        options: EnqueueOptions,
    ) -> Result<(JobId, ShardedReplication), QueueError> {
        let shard = self.pick_shard(payload, options.shard_key.as_deref());
        let (local, ops) = self
            .shard(shard)?
            .enqueue_opts_replicated(payload, options)
            .await?;
        Ok((
            JobId(encode_id(shard, local.0)),
            ShardedReplication { shard, ops },
        ))
    }

    /// Batch enqueue on routed shards; returns global job ids in input order.
    ///
    /// # Errors
    /// Returns [`QueueError`] if routing or any shard batch enqueue fails.
    pub async fn enqueue_batch_opts_replicated_sharded(
        &self,
        jobs: &[(Vec<u8>, EnqueueOptions)],
    ) -> Result<(Vec<JobId>, Vec<ShardedReplication>), QueueError> {
        if jobs.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let mut by_shard: HashMap<usize, Vec<(usize, Vec<u8>, EnqueueOptions)>> = HashMap::new();
        for (idx, (payload, options)) in jobs.iter().enumerate() {
            let shard = self.pick_shard(payload, options.shard_key.as_deref());
            by_shard
                .entry(shard)
                .or_default()
                .push((idx, payload.clone(), options.clone()));
        }
        let mut ids = vec![JobId(0); jobs.len()];
        let mut replications = Vec::new();
        for (shard, batch) in by_shard {
            let shard_jobs: Vec<(Vec<u8>, EnqueueOptions)> = batch
                .iter()
                .map(|(_, payload, options)| (payload.clone(), options.clone()))
                .collect();
            let (local_ids, ops) = self
                .shard(shard)?
                .enqueue_batch_opts_replicated(&shard_jobs)
                .await?;
            if !ops.is_empty() {
                replications.push(ShardedReplication { shard, ops });
            }
            for ((idx, _, _), local_id) in batch.into_iter().zip(local_ids) {
                ids[idx] = JobId(encode_id(shard, local_id.0));
            }
        }
        Ok((ids, replications))
    }

    /// Batch ack across shards; groups leases by encoded shard index.
    ///
    /// # Errors
    /// Returns [`QueueError`] if any shard index is invalid or ack fails.
    pub async fn ack_batch_replicated_sharded(
        &self,
        worker: WorkerId,
        lease_ids: &[LeaseId],
    ) -> Result<Vec<ShardedReplication>, QueueError> {
        if lease_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut by_shard: HashMap<usize, Vec<LeaseId>> = HashMap::new();
        for lease_id in lease_ids {
            let (shard, local) = decode_id(lease_id.0);
            by_shard.entry(shard).or_default().push(LeaseId(local));
        }
        let mut replications = Vec::new();
        for (shard, locals) in by_shard {
            let ops = self
                .shard(shard)?
                .ack_batch_replicated(worker, &locals)
                .await?;
            if !ops.is_empty() {
                replications.push(ShardedReplication { shard, ops });
            }
        }
        Ok(replications)
    }

    /// Lease across shards; replication ops are grouped per shard.
    ///
    /// # Errors
    /// Returns [`QueueError`] if any shard lease fails.
    pub async fn lease_replicated_sharded(
        &self,
        worker: WorkerId,
        max: usize,
    ) -> Result<(Vec<LeasedJob>, Vec<ShardedReplication>), QueueError> {
        let mut out = Vec::new();
        let mut replications = Vec::new();
        let mut need = max;
        for (shard, queue) in self.shards.iter().enumerate() {
            if need == 0 {
                break;
            }
            let (jobs, ops) = queue.lease_replicated(worker, need).await?;
            need = need.saturating_sub(jobs.len());
            if !ops.is_empty() {
                replications.push(ShardedReplication { shard, ops });
            }
            out.extend(jobs.into_iter().map(|job| LeasedJob {
                lease_id: LeaseId(encode_id(shard, job.lease_id.0)),
                job_id: JobId(encode_id(shard, job.job_id.0)),
                payload: job.payload,
                attempts: job.attempts,
                dedup_key: job.dedup_key,
            }));
        }
        Ok((out, replications))
    }

    /// Lease up to `max` jobs from a single shard; ids are encoded globally.
    ///
    /// # Errors
    /// Returns [`QueueError`] if the shard index is invalid or lease fails.
    pub async fn lease_shard_replicated(
        &self,
        shard: usize,
        worker: WorkerId,
        max: usize,
    ) -> Result<(Vec<LeasedJob>, ShardedReplication), QueueError> {
        let (jobs, ops) = self.shard(shard)?.lease_replicated(worker, max).await?;
        let out = jobs
            .into_iter()
            .map(|job| LeasedJob {
                lease_id: LeaseId(encode_id(shard, job.lease_id.0)),
                job_id: JobId(encode_id(shard, job.job_id.0)),
                payload: job.payload,
                attempts: job.attempts,
                dedup_key: job.dedup_key,
            })
            .collect();
        Ok((out, ShardedReplication { shard, ops }))
    }

    /// Ack a leased job, routing to the shard encoded in `lease_id`.
    ///
    /// # Errors
    /// Returns [`QueueError`] if the shard index is invalid or ack fails.
    pub async fn ack_replicated_sharded(
        &self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> Result<ShardedReplication, QueueError> {
        let (shard, local) = decode_id(lease_id.0);
        let ops = self
            .shard(shard)?
            .ack_replicated(worker, LeaseId(local))
            .await?;
        Ok(ShardedReplication { shard, ops })
    }

    /// Nack a leased job, routing to the shard encoded in `lease_id`.
    ///
    /// # Errors
    /// Returns [`QueueError`] if the shard index is invalid or nack fails.
    pub async fn nack_replicated_sharded(
        &self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> Result<ShardedReplication, QueueError> {
        let (shard, local) = decode_id(lease_id.0);
        let ops = self
            .shard(shard)?
            .nack_replicated(worker, LeaseId(local))
            .await?;
        Ok(ShardedReplication { shard, ops })
    }
}

impl JobQueue for ShardedJobQueue {
    fn enqueue_opts<'a>(
        &'a self,
        payload: &'a [u8],
        options: EnqueueOptions,
    ) -> BoxFuture<'a, Result<JobId, QueueError>> {
        Box::pin(async move {
            let shard = self.pick_shard(payload, options.shard_key.as_deref());
            let local = self.shard(shard)?.enqueue_opts(payload, options).await?;
            Ok(JobId(encode_id(shard, local.0)))
        })
    }

    fn enqueue_opts_replicated<'a>(
        &'a self,
        payload: &'a [u8],
        options: EnqueueOptions,
    ) -> BoxFuture<'a, Result<(JobId, QueueReplicationOps), QueueError>> {
        Box::pin(async move {
            let shard = self.pick_shard(payload, options.shard_key.as_deref());
            let (local, ops) = self
                .shard(shard)?
                .enqueue_opts_replicated(payload, options)
                .await?;
            Ok((JobId(encode_id(shard, local.0)), ops))
        })
    }

    fn apply_replicate<'a>(
        &'a self,
        _op: &'a crafty_proto::QueueReplicateOp,
    ) -> BoxFuture<'a, Result<(), QueueError>> {
        Box::pin(async move {
            Err(QueueError::Backend(
                "ShardedJobQueue does not apply replication directly".into(),
            ))
        })
    }

    fn lease(
        &self,
        worker: WorkerId,
        max: usize,
    ) -> BoxFuture<'_, Result<Vec<LeasedJob>, QueueError>> {
        Box::pin(async move { self.lease_replicated(worker, max).await.map(|(j, _)| j) })
    }

    fn lease_replicated(
        &self,
        worker: WorkerId,
        max: usize,
    ) -> BoxFuture<'_, Result<(Vec<LeasedJob>, QueueReplicationOps), QueueError>> {
        Box::pin(async move {
            let mut out = Vec::new();
            let mut ops = Vec::new();
            let mut need = max;
            for (shard, queue) in self.shards.iter().enumerate() {
                if need == 0 {
                    break;
                }
                let (jobs, shard_ops) = queue.lease_replicated(worker, need).await?;
                need = need.saturating_sub(jobs.len());
                ops.extend(shard_ops);
                out.extend(jobs.into_iter().map(|job| LeasedJob {
                    lease_id: LeaseId(encode_id(shard, job.lease_id.0)),
                    job_id: JobId(encode_id(shard, job.job_id.0)),
                    payload: job.payload,
                    attempts: job.attempts,
                    dedup_key: job.dedup_key,
                }));
            }
            Ok((out, ops))
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
            let (shard, local) = decode_id(lease_id.0);
            self.shard(shard)?
                .ack_replicated(worker, LeaseId(local))
                .await
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
            let (shard, local) = decode_id(lease_id.0);
            self.shard(shard)?
                .nack_replicated(worker, LeaseId(local))
                .await
        })
    }

    fn metrics(&self) -> BoxFuture<'_, Result<QueueMetrics, QueueError>> {
        Box::pin(async move {
            let mut total = QueueMetrics::default();
            for shard in &self.shards {
                let m = shard.metrics().await?;
                total.pending += m.pending;
                total.leased += m.leased;
                total.dead_letter += m.dead_letter;
                if m.oldest_pending_age > total.oldest_pending_age {
                    total.oldest_pending_age = m.oldest_pending_age;
                }
            }
            Ok(total)
        })
    }

    fn requeue_dead_letter(&self, job_id: JobId) -> BoxFuture<'_, Result<(), QueueError>> {
        Box::pin(async move {
            let (shard, local) = decode_id(job_id.0);
            self.shard(shard)?.requeue_dead_letter(JobId(local)).await
        })
    }

    fn job_status(&self, job_id: JobId) -> BoxFuture<'_, Result<Option<JobStatus>, QueueError>> {
        Box::pin(async move {
            let (shard, local) = decode_id(job_id.0);
            let status = self.shard(shard)?.job_status(JobId(local)).await?;
            Ok(status.map(|mut s| {
                s.job_id = job_id;
                s
            }))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::InMemoryJobQueue;

    #[tokio::test]
    async fn routes_enqueue_by_shard_key() {
        let shards: Vec<_> = (0..4)
            .map(|_| Arc::new(InMemoryJobQueue::new(Duration::from_secs(30))) as Arc<dyn JobQueue>)
            .collect();
        let q = ShardedJobQueue::new(shards);
        let id_a = q
            .enqueue_opts(
                b"a",
                EnqueueOptions {
                    shard_key: Some(b"tenant-1".to_vec()),
                    ..EnqueueOptions::default()
                },
            )
            .await
            .unwrap();
        let id_b = q
            .enqueue_opts(
                b"b",
                EnqueueOptions {
                    shard_key: Some(b"tenant-1".to_vec()),
                    ..EnqueueOptions::default()
                },
            )
            .await
            .unwrap();
        let (shard_a, _) = decode_id(id_a.0);
        let (shard_b, _) = decode_id(id_b.0);
        assert_eq!(shard_a, shard_b);
    }

    #[tokio::test]
    async fn lease_ack_round_trip_across_shards() {
        let shards: Vec<_> = (0..3)
            .map(|_| Arc::new(InMemoryJobQueue::new(Duration::from_secs(30))) as Arc<dyn JobQueue>)
            .collect();
        let q = ShardedJobQueue::new(shards);
        for i in 0..6u8 {
            q.enqueue(&[i]).await.unwrap();
        }
        let worker = WorkerId {
            node: crafty_proto::NodeId(1),
            instance: 0,
        };
        let leased = q.lease(worker, 6).await.unwrap();
        assert_eq!(leased.len(), 6);
        for job in leased {
            q.ack(worker, job.lease_id).await.unwrap();
        }
        assert_eq!(q.metrics().await.unwrap().pending, 0);
    }

    #[tokio::test]
    async fn batch_enqueue_preserves_input_order_on_one_shard() {
        let shards: Vec<_> = (0..4)
            .map(|_| Arc::new(InMemoryJobQueue::new(Duration::from_secs(30))) as Arc<dyn JobQueue>)
            .collect();
        let q = ShardedJobQueue::new(shards);
        let batch: Vec<(Vec<u8>, EnqueueOptions)> = (0..5u8)
            .map(|i| {
                (
                    vec![i],
                    EnqueueOptions {
                        shard_key: Some(b"tenant-a".to_vec()),
                        ..EnqueueOptions::default()
                    },
                )
            })
            .collect();
        let (ids, reps) = q
            .enqueue_batch_opts_replicated_sharded(&batch)
            .await
            .unwrap();
        assert_eq!(ids.len(), 5);
        assert_eq!(reps.len(), 1);
        let (picked_shard, local0) = decode_id(ids[0].0);
        for (offset, id) in ids.iter().enumerate().skip(1) {
            let (shard, local) = decode_id(id.0);
            assert_eq!(shard, picked_shard);
            assert_eq!(local, local0 + offset as u64);
        }
    }

    #[tokio::test]
    async fn batch_ack_routes_by_shard() {
        let shards: Vec<_> = (0..3)
            .map(|_| Arc::new(InMemoryJobQueue::new(Duration::from_secs(30))) as Arc<dyn JobQueue>)
            .collect();
        let q = ShardedJobQueue::new(shards);
        for i in 0..6u8 {
            q.enqueue(&[i]).await.unwrap();
        }
        let worker = WorkerId {
            node: crafty_proto::NodeId(1),
            instance: 0,
        };
        let leased = q.lease(worker, 6).await.unwrap();
        let lease_ids: Vec<LeaseId> = leased.iter().map(|j| j.lease_id).collect();
        let reps = q
            .ack_batch_replicated_sharded(worker, &lease_ids)
            .await
            .unwrap();
        assert!(!reps.is_empty());
        assert_eq!(q.metrics().await.unwrap().pending, 0);
    }
}
