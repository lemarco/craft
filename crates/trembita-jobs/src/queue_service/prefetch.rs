//! Leader prefetch cache for low-latency leases after enqueue.

use std::sync::Arc;

use trembita_proto::{QueueLeasedJobWire, QueueReplicateOp};
use trembita_storage::now_ms;

use super::wire::shard_stream_name;
use crate::queue_prefetch::CachedPendingJob;
use crate::sharded_queue::{decode_global_id, encode_global_id};
use crate::{
    JobId, JobQueue, LeaseId, LeasedJob, QueueError, QueueReplicationOps, RedbJobQueue,
    ShardedJobQueue, ShardedReplication, WorkerId,
};

use super::QueueService;

impl QueueService {
    pub(super) fn sharded_stream(&self, stream: &str) -> Option<Arc<ShardedJobQueue>> {
        self.registry
            .lock()
            .expect("poisoned")
            .sharded
            .get(stream)
            .cloned()
    }

    pub(super) fn cache_enqueued(
        &self,
        stream: &str,
        job_id: u64,
        payload: Vec<u8>,
        priority: u8,
        not_before_ms: u64,
        dedup_key: Option<Vec<u8>>,
    ) {
        let mut registry = self.registry.lock().expect("poisoned");
        let Some(cache) = registry.prefetch.get_mut(stream) else {
            return;
        };
        let effective_not_before = if not_before_ms == 0 {
            now_ms()
        } else {
            not_before_ms
        };
        cache.insert_enqueued(CachedPendingJob {
            job_id,
            payload,
            priority,
            not_before_ms: effective_not_before,
            dedup_key,
        });
    }

    pub(super) fn evict_prefetch(&self, stream: &str, job_ids: impl IntoIterator<Item = u64>) {
        let mut registry = self.registry.lock().expect("poisoned");
        let Some(cache) = registry.prefetch.get_mut(stream) else {
            return;
        };
        for job_id in job_ids {
            cache.remove_job(job_id);
        }
    }

    pub(super) fn evict_prefetch_ack_ops(&self, stream: &str, ops: &[QueueReplicateOp]) {
        self.evict_prefetch(
            stream,
            ops.iter().filter_map(|op| match op {
                QueueReplicateOp::Ack { job_id, .. } => Some(*job_id),
                _ => None,
            }),
        );
    }

    pub(super) fn evict_prefetch_sharded_acks(&self, base: &str, reps: &[ShardedReplication]) {
        for rep in reps {
            self.evict_prefetch_ack_ops(&shard_stream_name(base, rep.shard), &rep.ops);
        }
    }

    pub(super) fn cache_enqueued_sharded(
        &self,
        base: &str,
        global_id: u64,
        payload: Vec<u8>,
        priority: u8,
        not_before_ms: u64,
        dedup_key: Option<Vec<u8>>,
    ) {
        let (shard, local) = decode_global_id(global_id);
        self.cache_enqueued(
            &shard_stream_name(base, shard),
            local,
            payload,
            priority,
            not_before_ms,
            dedup_key,
        );
    }

    pub(super) async fn lease_redb_with_prefetch(
        &self,
        stream: &str,
        queue: &RedbJobQueue,
        worker: WorkerId,
        max: usize,
    ) -> Result<(Vec<LeasedJob>, QueueReplicationOps), QueueError> {
        let now = now_ms();
        let prefetched = self
            .registry
            .lock()
            .expect("poisoned")
            .prefetch
            .get_mut(stream)
            .map(|cache| cache.select_for_lease(max, now))
            .unwrap_or_default();

        let mut jobs = Vec::new();
        let mut ops = Vec::new();
        if !prefetched.is_empty() {
            let (leased, mut step) = queue.lease_prefetched(worker, &prefetched)?;
            jobs.extend(leased);
            ops.append(&mut step);
        }
        if jobs.len() < max {
            let (mut more, mut more_ops) = queue.lease_replicated(worker, max - jobs.len()).await?;
            jobs.append(&mut more);
            ops.append(&mut more_ops);
        }
        Ok((jobs, ops))
    }

    pub(super) async fn lease_sharded_with_prefetch(
        &self,
        base: &str,
        sharded: &ShardedJobQueue,
        worker: WorkerId,
        max: usize,
    ) -> Result<(Vec<LeasedJob>, Vec<ShardedReplication>), QueueError> {
        let now = now_ms();
        let mut out = Vec::new();
        let mut replications = Vec::new();
        let mut need = max;

        for shard in 0..sharded.shard_count() {
            if need == 0 {
                break;
            }
            let stream = shard_stream_name(base, shard);
            let (redb, prefetched) = {
                let mut registry = self.registry.lock().expect("poisoned");
                let redb = registry.redb_streams.get(&stream).cloned();
                let prefetched = registry
                    .prefetch
                    .get_mut(&stream)
                    .map(|cache| cache.select_for_lease(need, now))
                    .unwrap_or_default();
                (redb, prefetched)
            };

            if let Some(redb) = redb
                && !prefetched.is_empty()
            {
                let (leased, step) = redb.lease_prefetched(worker, &prefetched)?;
                need = need.saturating_sub(leased.len());
                if !step.is_empty() {
                    replications.push(ShardedReplication { shard, ops: step });
                }
                out.extend(leased.into_iter().map(|job| LeasedJob {
                    lease_id: LeaseId(encode_global_id(shard, job.lease_id.0)),
                    job_id: JobId(encode_global_id(shard, job.job_id.0)),
                    payload: job.payload,
                    attempts: job.attempts,
                    dedup_key: job.dedup_key,
                }));
            }

            if need > 0 {
                let (jobs, rep) = sharded.lease_shard_replicated(shard, worker, need).await?;
                need = need.saturating_sub(jobs.len());
                out.extend(jobs);
                if !rep.ops.is_empty() {
                    replications.push(rep);
                }
            }
        }
        Ok((out, replications))
    }

    pub(super) fn leased_to_wire(jobs: Vec<LeasedJob>) -> Vec<QueueLeasedJobWire> {
        jobs.into_iter()
            .map(|j| QueueLeasedJobWire {
                lease_id: j.lease_id.0,
                job_id: j.job_id.0,
                payload: j.payload,
                attempts: j.attempts,
                dedup_key: j.dedup_key,
            })
            .collect()
    }
}
