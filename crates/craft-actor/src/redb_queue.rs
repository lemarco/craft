//! Durable [`JobQueue`](super::JobQueue) backed by `redb` ([job-queue](../../../docs/decisions/job-queue.md)).
//!
//! One `{data_dir}/queue-{name}.redb` file per stream; separate from Raft
//! `group-*.redb` files.

use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use craft_proto::{QueueReplicateOp, decode, encode};
use redb::{Database, ReadableTable, TableDefinition};

use super::{
    BoxFuture, JobId, JobQueue, LeaseId, LeasedJob, QueueError, QueueMetrics, QueueReplicationOps,
    WorkerId,
};

const JOBS: TableDefinition<u64, &[u8]> = TableDefinition::new("queue_jobs");
const PENDING: TableDefinition<u64, &[u8]> = TableDefinition::new("queue_pending");
const LEASES: TableDefinition<u64, &[u8]> = TableDefinition::new("queue_leases");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("queue_meta");

const K_NEXT_JOB: &str = "next_job_id";
const K_NEXT_LEASE: &str = "next_lease_id";

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct StoredJob {
    payload: Vec<u8>,
    enqueued_at_ms: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct StoredLease {
    job_id: u64,
    worker_node: u64,
    worker_instance: u32,
    expires_at_ms: u64,
}

fn backend(e: impl std::fmt::Display) -> QueueError {
    QueueError::Backend(e.to_string())
}

fn codec(e: impl std::fmt::Display) -> QueueError {
    QueueError::Codec(e.to_string())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Crash-safe [`JobQueue`] in a dedicated `redb` file.
#[derive(Debug)]
pub struct RedbJobQueue {
    lease_timeout: Duration,
    db: Database,
    /// Serializes write transactions (redb allows one writer).
    write_lock: Mutex<()>,
}

impl RedbJobQueue {
    /// Open or create the queue database at `path`.
    ///
    /// # Errors
    /// Returns [`QueueError::Backend`] if the file cannot be opened.
    pub fn open(path: impl AsRef<Path>, lease_timeout: Duration) -> Result<Self, QueueError> {
        let db = Database::create(path).map_err(backend)?;
        let write_lock = Mutex::new(());
        let queue = Self {
            lease_timeout,
            db,
            write_lock,
        };
        queue.bootstrap()?;
        Ok(queue)
    }

    fn bootstrap(&self) -> Result<(), QueueError> {
        let _g = self.write_lock.lock().expect("poisoned");
        let txn = self.db.begin_write().map_err(backend)?;
        {
            txn.open_table(JOBS).map_err(backend)?;
            txn.open_table(PENDING).map_err(backend)?;
            txn.open_table(LEASES).map_err(backend)?;
            let mut meta = txn.open_table(META).map_err(backend)?;
            if meta.get(K_NEXT_JOB).map_err(backend)?.is_none() {
                meta.insert(K_NEXT_JOB, encode(&1u64).map_err(codec)?.as_slice())
                    .map_err(backend)?;
            }
            if meta.get(K_NEXT_LEASE).map_err(backend)?.is_none() {
                meta.insert(K_NEXT_LEASE, encode(&1u64).map_err(codec)?.as_slice())
                    .map_err(backend)?;
            }
        }
        txn.commit().map_err(backend)?;
        Ok(())
    }

    fn read_meta_u64(&self, key: &str) -> Result<u64, QueueError> {
        let txn = self.db.begin_read().map_err(backend)?;
        let table = txn.open_table(META).map_err(backend)?;
        match table.get(key).map_err(backend)? {
            Some(v) => decode(v.value()).map_err(codec),
            None => Err(backend(format!("missing meta key {key}"))),
        }
    }

    fn reclaim_expired_ops(&self) -> Result<QueueReplicationOps, QueueError> {
        let now = now_ms();
        let mut ops = Vec::new();
        let _g = self.write_lock.lock().expect("poisoned");
        let txn = self.db.begin_write().map_err(backend)?;
        {
            let mut leases = txn.open_table(LEASES).map_err(backend)?;
            let mut pending = txn.open_table(PENDING).map_err(backend)?;
            let expired: Vec<(u64, u64)> = leases
                .iter()
                .map_err(backend)?
                .filter_map(|row| {
                    let (lease_id, bytes) = row.ok()?;
                    let stored: StoredLease = decode(bytes.value()).ok()?;
                    (stored.expires_at_ms <= now).then_some((lease_id.value(), stored.job_id))
                })
                .collect();
            for (lease_id, job_id) in expired {
                if leases.remove(lease_id).map_err(backend)?.is_some() {
                    pending.insert(job_id, &[] as &[u8]).map_err(backend)?;
                    ops.push(QueueReplicateOp::Reclaim { lease_id, job_id });
                }
            }
        }
        txn.commit().map_err(backend)?;
        Ok(ops)
    }

    fn bump_meta_u64(
        meta: &mut redb::Table<'_, &str, &[u8]>,
        key: &str,
        at_least: u64,
    ) -> Result<(), QueueError> {
        let current = match meta.get(key).map_err(backend)? {
            Some(v) => decode(v.value()).map_err(codec)?,
            None => 1,
        };
        if at_least > current {
            meta.insert(key, encode(&at_least).map_err(codec)?.as_slice())
                .map_err(backend)?;
        }
        Ok(())
    }

    fn apply_replicate_inner(&self, op: &QueueReplicateOp) -> Result<(), QueueError> {
        let _g = self.write_lock.lock().expect("poisoned");
        let txn = self.db.begin_write().map_err(backend)?;
        {
            let mut jobs = txn.open_table(JOBS).map_err(backend)?;
            let mut pending = txn.open_table(PENDING).map_err(backend)?;
            let mut leases = txn.open_table(LEASES).map_err(backend)?;
            let mut meta = txn.open_table(META).map_err(backend)?;
            match op {
                QueueReplicateOp::Enqueue {
                    job_id,
                    payload,
                    enqueued_at_ms,
                    next_job_id,
                } => {
                    if jobs.get(*job_id).map_err(backend)?.is_none() {
                        let stored = StoredJob {
                            payload: payload.clone(),
                            enqueued_at_ms: *enqueued_at_ms,
                        };
                        jobs.insert(*job_id, encode(&stored).map_err(codec)?.as_slice())
                            .map_err(backend)?;
                        pending.insert(*job_id, &[] as &[u8]).map_err(backend)?;
                    }
                    Self::bump_meta_u64(&mut meta, K_NEXT_JOB, *next_job_id)?;
                }
                QueueReplicateOp::Lease {
                    lease_id,
                    job_id,
                    worker_node,
                    worker_instance,
                    expires_at_ms,
                    next_lease_id,
                } => {
                    if leases.get(*lease_id).map_err(backend)?.is_none() {
                        pending.remove(*job_id).map_err(backend)?;
                        let lease = StoredLease {
                            job_id: *job_id,
                            worker_node: *worker_node,
                            worker_instance: *worker_instance,
                            expires_at_ms: *expires_at_ms,
                        };
                        leases
                            .insert(*lease_id, encode(&lease).map_err(codec)?.as_slice())
                            .map_err(backend)?;
                    }
                    Self::bump_meta_u64(&mut meta, K_NEXT_LEASE, *next_lease_id)?;
                }
                QueueReplicateOp::Ack { lease_id, job_id } => {
                    leases.remove(*lease_id).map_err(backend)?;
                    jobs.remove(*job_id).map_err(backend)?;
                }
                QueueReplicateOp::Nack { lease_id, job_id }
                | QueueReplicateOp::Reclaim { lease_id, job_id } => {
                    leases.remove(*lease_id).map_err(backend)?;
                    if jobs.get(*job_id).map_err(backend)?.is_some() {
                        pending.insert(*job_id, &[] as &[u8]).map_err(backend)?;
                    }
                }
            }
        }
        txn.commit().map_err(backend)?;
        Ok(())
    }

    fn reclaim_expired(&self) -> Result<(), QueueError> {
        let _ = self.reclaim_expired_ops()?;
        Ok(())
    }

    fn metrics_inner(&self) -> Result<QueueMetrics, QueueError> {
        self.reclaim_expired()?;
        let txn = self.db.begin_read().map_err(backend)?;
        let pending = txn.open_table(PENDING).map_err(backend)?;
        let leases = txn.open_table(LEASES).map_err(backend)?;
        let jobs = txn.open_table(JOBS).map_err(backend)?;

        let pending_count = pending.iter().map_err(backend)?.count() as u64;
        let leased_count = leases.iter().map_err(backend)?.count() as u64;

        let oldest_ms = pending
            .iter()
            .map_err(backend)?
            .filter_map(|row| row.ok())
            .filter_map(|(job_id, _)| {
                let job_id = job_id.value();
                let bytes = jobs.get(job_id).ok().flatten()?;
                let stored: StoredJob = decode(bytes.value()).ok()?;
                Some(stored.enqueued_at_ms)
            })
            .min();

        let oldest_pending_age = oldest_ms
            .map(|ms| Duration::from_millis(now_ms().saturating_sub(ms)))
            .unwrap_or_default();

        Ok(QueueMetrics {
            pending: pending_count,
            leased: leased_count,
            oldest_pending_age,
        })
    }
}

impl JobQueue for RedbJobQueue {
    fn enqueue<'a>(&'a self, payload: &'a [u8]) -> BoxFuture<'a, Result<JobId, QueueError>> {
        Box::pin(async move { self.enqueue_replicated(payload).await.map(|(id, _)| id) })
    }

    fn apply_replicate<'a>(
        &'a self,
        op: &'a QueueReplicateOp,
    ) -> BoxFuture<'a, Result<(), QueueError>> {
        Box::pin(async move { self.apply_replicate_inner(op) })
    }

    fn enqueue_replicated<'a>(
        &'a self,
        payload: &'a [u8],
    ) -> BoxFuture<'a, Result<(JobId, QueueReplicationOps), QueueError>> {
        Box::pin(async move {
            let mut ops = self.reclaim_expired_ops()?;
            let job_id = self.read_meta_u64(K_NEXT_JOB)?;
            let enqueued_at_ms = now_ms();
            let stored = StoredJob {
                payload: payload.to_vec(),
                enqueued_at_ms,
            };
            let bytes = encode(&stored).map_err(codec)?;
            let next_job_id = job_id + 1;

            let _g = self.write_lock.lock().expect("poisoned");
            let txn = self.db.begin_write().map_err(backend)?;
            {
                let mut jobs = txn.open_table(JOBS).map_err(backend)?;
                let mut pending = txn.open_table(PENDING).map_err(backend)?;
                let mut meta = txn.open_table(META).map_err(backend)?;
                jobs.insert(job_id, bytes.as_slice()).map_err(backend)?;
                pending.insert(job_id, &[] as &[u8]).map_err(backend)?;
                meta.insert(K_NEXT_JOB, encode(&next_job_id).map_err(codec)?.as_slice())
                    .map_err(backend)?;
            }
            txn.commit().map_err(backend)?;

            ops.push(QueueReplicateOp::Enqueue {
                job_id,
                payload: payload.to_vec(),
                enqueued_at_ms,
                next_job_id,
            });
            Ok((JobId(job_id), ops))
        })
    }

    fn lease<'a>(
        &'a self,
        worker: WorkerId,
        max: usize,
    ) -> BoxFuture<'a, Result<Vec<LeasedJob>, QueueError>> {
        Box::pin(async move {
            self.lease_replicated(worker, max)
                .await
                .map(|(jobs, _)| jobs)
        })
    }

    fn lease_replicated<'a>(
        &'a self,
        worker: WorkerId,
        max: usize,
    ) -> BoxFuture<'a, Result<(Vec<LeasedJob>, QueueReplicationOps), QueueError>> {
        Box::pin(async move {
            let mut ops = self.reclaim_expired_ops()?;
            let expires_at_ms = now_ms() + self.lease_timeout.as_millis() as u64;
            let mut lease_id_start = self.read_meta_u64(K_NEXT_LEASE)?;
            let mut out = Vec::new();

            let _g = self.write_lock.lock().expect("poisoned");
            let txn = self.db.begin_write().map_err(backend)?;
            {
                let jobs = txn.open_table(JOBS).map_err(backend)?;
                let mut pending = txn.open_table(PENDING).map_err(backend)?;
                let mut leases = txn.open_table(LEASES).map_err(backend)?;
                let mut meta = txn.open_table(META).map_err(backend)?;

                let mut pending_ids: Vec<u64> = pending
                    .iter()
                    .map_err(backend)?
                    .filter_map(|row| row.ok().map(|(k, _)| k.value()))
                    .collect();
                pending_ids.sort_unstable();
                pending_ids.truncate(max);

                for job_id in pending_ids {
                    let Some(job_bytes) = jobs.get(job_id).map_err(backend)? else {
                        pending.remove(job_id).map_err(backend)?;
                        continue;
                    };
                    let stored: StoredJob = decode(job_bytes.value()).map_err(codec)?;
                    pending.remove(job_id).map_err(backend)?;

                    let lease_id = lease_id_start;
                    lease_id_start += 1;
                    let lease = StoredLease {
                        job_id,
                        worker_node: worker.node.0,
                        worker_instance: worker.instance,
                        expires_at_ms,
                    };
                    leases
                        .insert(lease_id, encode(&lease).map_err(codec)?.as_slice())
                        .map_err(backend)?;

                    ops.push(QueueReplicateOp::Lease {
                        lease_id,
                        job_id,
                        worker_node: worker.node.0,
                        worker_instance: worker.instance,
                        expires_at_ms,
                        next_lease_id: lease_id_start,
                    });

                    out.push(LeasedJob {
                        lease_id: LeaseId(lease_id),
                        job_id: JobId(job_id),
                        payload: stored.payload,
                    });
                }

                meta.insert(
                    K_NEXT_LEASE,
                    encode(&lease_id_start).map_err(codec)?.as_slice(),
                )
                .map_err(backend)?;
            }
            txn.commit().map_err(backend)?;
            Ok((out, ops))
        })
    }

    fn ack<'a>(
        &'a self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'a, Result<(), QueueError>> {
        Box::pin(async move { self.ack_replicated(worker, lease_id).await.map(|_| ()) })
    }

    fn ack_replicated<'a>(
        &'a self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'a, Result<QueueReplicationOps, QueueError>> {
        Box::pin(async move {
            let job_id = {
                let _g = self.write_lock.lock().expect("poisoned");
                let txn = self.db.begin_write().map_err(backend)?;
                let job_id = {
                    let mut jobs = txn.open_table(JOBS).map_err(backend)?;
                    let mut leases = txn.open_table(LEASES).map_err(backend)?;
                    let stored: StoredLease = match leases.remove(lease_id.0).map_err(backend)? {
                        None => return Err(QueueError::InvalidLease),
                        Some(bytes) => decode(bytes.value()).map_err(codec)?,
                    };
                    if stored.worker_node != worker.node.0
                        || stored.worker_instance != worker.instance
                    {
                        leases
                            .insert(lease_id.0, encode(&stored).map_err(codec)?.as_slice())
                            .map_err(backend)?;
                        return Err(QueueError::InvalidLease);
                    }
                    jobs.remove(stored.job_id).map_err(backend)?;
                    stored.job_id
                };
                txn.commit().map_err(backend)?;
                job_id
            };
            Ok(vec![QueueReplicateOp::Ack {
                lease_id: lease_id.0,
                job_id,
            }])
        })
    }

    fn nack<'a>(
        &'a self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'a, Result<(), QueueError>> {
        Box::pin(async move { self.nack_replicated(worker, lease_id).await.map(|_| ()) })
    }

    fn nack_replicated<'a>(
        &'a self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'a, Result<QueueReplicationOps, QueueError>> {
        Box::pin(async move {
            let job_id = {
                let _g = self.write_lock.lock().expect("poisoned");
                let txn = self.db.begin_write().map_err(backend)?;
                let job_id = {
                    let mut pending = txn.open_table(PENDING).map_err(backend)?;
                    let mut leases = txn.open_table(LEASES).map_err(backend)?;
                    let stored: StoredLease = match leases.remove(lease_id.0).map_err(backend)? {
                        None => return Err(QueueError::InvalidLease),
                        Some(bytes) => decode(bytes.value()).map_err(codec)?,
                    };
                    if stored.worker_node != worker.node.0
                        || stored.worker_instance != worker.instance
                    {
                        leases
                            .insert(lease_id.0, encode(&stored).map_err(codec)?.as_slice())
                            .map_err(backend)?;
                        return Err(QueueError::InvalidLease);
                    }
                    pending
                        .insert(stored.job_id, &[] as &[u8])
                        .map_err(backend)?;
                    stored.job_id
                };
                txn.commit().map_err(backend)?;
                job_id
            };
            Ok(vec![QueueReplicateOp::Nack {
                lease_id: lease_id.0,
                job_id,
            }])
        })
    }

    fn metrics<'a>(&'a self) -> BoxFuture<'a, Result<QueueMetrics, QueueError>> {
        Box::pin(async move { self.metrics_inner() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use craft_proto::NodeId;
    use std::sync::Arc;

    fn worker(instance: u32) -> WorkerId {
        WorkerId {
            node: NodeId(1),
            instance,
        }
    }

    #[tokio::test]
    async fn redb_enqueue_lease_ack() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.redb");
        let q = RedbJobQueue::open(&path, Duration::from_secs(30)).unwrap();
        q.enqueue(b"disk").await.unwrap();
        let leased = q.lease(worker(0), 4).await.unwrap();
        assert_eq!(leased[0].payload, b"disk");
        q.ack(worker(0), leased[0].lease_id).await.unwrap();
        assert_eq!(q.metrics().await.unwrap().pending, 0);
    }

    #[tokio::test]
    async fn redb_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.redb");
        {
            let q = RedbJobQueue::open(&path, Duration::from_secs(30)).unwrap();
            q.enqueue(b"persistent").await.unwrap();
            let leased = q.lease(worker(0), 1).await.unwrap();
            q.nack(worker(0), leased[0].lease_id).await.unwrap();
        }
        let q = RedbJobQueue::open(&path, Duration::from_secs(30)).unwrap();
        let m = q.metrics().await.unwrap();
        assert_eq!(m.pending, 1);
        let leased = q.lease(worker(1), 1).await.unwrap();
        assert_eq!(leased[0].payload, b"persistent");
    }

    #[tokio::test]
    async fn apply_replicate_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let leader =
            RedbJobQueue::open(dir.path().join("leader.redb"), Duration::from_secs(30)).unwrap();
        let follower =
            RedbJobQueue::open(dir.path().join("follower.redb"), Duration::from_secs(30)).unwrap();
        let (_, ops) = leader.enqueue_replicated(b"x").await.unwrap();
        assert_eq!(ops.len(), 1);
        for q in [&leader, &follower] {
            q.apply_replicate(&ops[0]).await.unwrap();
            q.apply_replicate(&ops[0]).await.unwrap();
        }
        assert_eq!(leader.metrics().await.unwrap().pending, 1);
        assert_eq!(follower.metrics().await.unwrap().pending, 1);
    }

    #[tokio::test]
    async fn redb_shared_via_arc_trait() {
        let dir = tempfile::tempdir().unwrap();
        let q: Arc<dyn JobQueue> = Arc::new(
            RedbJobQueue::open(dir.path().join("q.redb"), Duration::from_secs(10)).unwrap(),
        );
        q.enqueue(b"a").await.unwrap();
        q.enqueue(b"b").await.unwrap();
        let w0 = q.lease(worker(0), 1).await.unwrap();
        let w1 = q.lease(worker(1), 1).await.unwrap();
        assert_ne!(w0[0].job_id, w1[0].job_id);
    }
}
