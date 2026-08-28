//! Federated [`JobQueue`] over multiple streams ([job-queue](../../../docs/decisions/job-queue.md)).
//!
//! Spreads enqueue/load across independent queue shards (separate redb files and
//! replication paths) while presenting one logical queue to producers/consumers.

use std::sync::Arc;

use super::{
    BoxFuture, EnqueueOptions, JobId, JobQueue, LeaseId, LeasedJob, QueueError, QueueMetrics,
    QueueReplicationOps, WorkerId,
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
    pub shard: usize,
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
            }));
        }
        Ok((out, replications))
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
                if m.oldest_pending_age > total.oldest_pending_age {
                    total.oldest_pending_age = m.oldest_pending_age;
                }
            }
            Ok(total)
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
}
