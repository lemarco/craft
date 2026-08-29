//! Durable [`JobQueue`](super::JobQueue) backed by `redb` ([job-queue](../../../docs/decisions/job-queue.md)).
//!
//! One `{data_dir}/queue-{name}.redb` file per stream; separate from Raft
//! `group-*.redb` files.

use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crafty_proto::{QueueReplicateOp, decode, encode};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use super::{
    after_failed_attempt, BoxFuture, EnqueueOptions, JobId, JobLifecycle, JobQueue, JobStatus,
    LeaseId, LeasedJob, QueueError, QueueMetrics, QueueReplicationOps, WorkerId,
};

const JOBS: TableDefinition<u64, &[u8]> = TableDefinition::new("queue_jobs");
const PENDING: TableDefinition<u64, &[u8]> = TableDefinition::new("queue_pending");
const LEASES: TableDefinition<u64, &[u8]> = TableDefinition::new("queue_leases");
const DEDUP: TableDefinition<&[u8], u64> = TableDefinition::new("queue_dedup");
const SCHEDULES: TableDefinition<&str, &[u8]> = TableDefinition::new("queue_schedules");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("queue_meta");

const K_NEXT_JOB: &str = "next_job_id";
const K_NEXT_LEASE: &str = "next_lease_id";
const COMPACT_EVERY_ACKS: u64 = 64;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct StoredJob {
    payload: Vec<u8>,
    enqueued_at_ms: u64,
    #[serde(default)]
    priority: u8,
    #[serde(default)]
    not_before_ms: u64,
    #[serde(default)]
    dedup_key: Option<Vec<u8>>,
    #[serde(default)]
    attempts: u32,
    #[serde(default)]
    max_attempts: u32,
    #[serde(default)]
    dead_letter: bool,
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
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

/// Crash-safe [`JobQueue`] in a dedicated `redb` file.
#[derive(Debug)]
pub struct RedbJobQueue {
    lease_timeout: Duration,
    db: Mutex<Database>,
    acks_since_compact: AtomicU64,
}

fn ready_pending_count(
    jobs: &redb::ReadOnlyTable<u64, &[u8]>,
    pending: &redb::ReadOnlyTable<u64, &[u8]>,
    now_ms: u64,
) -> Result<u64, QueueError> {
    let mut count = 0u64;
    for row in pending.iter().map_err(backend)? {
        let (job_id, _) = row.map_err(backend)?;
        if let Some(job_bytes) = jobs.get(job_id.value()).map_err(backend)? {
            let stored: StoredJob = decode(job_bytes.value()).map_err(codec)?;
            if stored.not_before_ms <= now_ms {
                count += 1;
            }
        }
    }
    Ok(count)
}

fn select_pending_ids(
    jobs: &redb::ReadOnlyTable<u64, &[u8]>,
    pending: &redb::ReadOnlyTable<u64, &[u8]>,
    max: usize,
    now_ms: u64,
) -> Result<Vec<u64>, QueueError> {
    let mut ready = Vec::new();
    for row in pending.iter().map_err(backend)? {
        let (job_id, _) = row.map_err(backend)?;
        let id = job_id.value();
        let Some(job_bytes) = jobs.get(id).map_err(backend)? else {
            continue;
        };
        let stored: StoredJob = decode(job_bytes.value()).map_err(codec)?;
        if stored.not_before_ms <= now_ms {
            ready.push((stored.priority, id));
        }
    }
    ready.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
    ready.truncate(max);
    Ok(ready.into_iter().map(|(_, id)| id).collect())
}

impl RedbJobQueue {
    /// Open or create the queue database at `path`.
    ///
    /// # Errors
    /// Returns [`QueueError::Backend`] if the file cannot be opened.
    pub fn open(path: impl AsRef<Path>, lease_timeout: Duration) -> Result<Self, QueueError> {
        let db = Mutex::new(Database::create(path).map_err(backend)?);
        let queue = Self {
            lease_timeout,
            db,
            acks_since_compact: AtomicU64::new(0),
        };
        queue.bootstrap()?;
        Ok(queue)
    }

    fn bootstrap(&self) -> Result<(), QueueError> {
        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_write().map_err(backend)?;
        {
            txn.open_table(JOBS).map_err(backend)?;
            txn.open_table(PENDING).map_err(backend)?;
            txn.open_table(LEASES).map_err(backend)?;
            txn.open_table(DEDUP).map_err(backend)?;
            txn.open_table(SCHEDULES).map_err(backend)?;
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
        let txn = self
            .db
            .lock()
            .expect("poisoned")
            .begin_read()
            .map_err(backend)?;
        let table = txn.open_table(META).map_err(backend)?;
        match table.get(key).map_err(backend)? {
            Some(v) => decode(v.value()).map_err(codec),
            None => Err(backend(format!("missing meta key {key}"))),
        }
    }

    fn reclaim_expired_ops(&self) -> Result<QueueReplicationOps, QueueError> {
        let now = now_ms();
        let mut ops = Vec::new();
        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_write().map_err(backend)?;
        {
            let mut jobs = txn.open_table(JOBS).map_err(backend)?;
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
                    let job_update = jobs
                        .get(job_id)
                        .map_err(backend)?
                        .map(|bytes| decode::<StoredJob>(bytes.value()).map_err(codec))
                        .transpose()?;
                    if let Some(mut stored) = job_update {
                        let outcome =
                            after_failed_attempt(stored.attempts, stored.max_attempts, now);
                        stored.attempts = outcome.attempts;
                        stored.dead_letter = outcome.dead_letter;
                        stored.not_before_ms = outcome.not_before_ms;
                        jobs.insert(job_id, encode(&stored).map_err(codec)?.as_slice())
                            .map_err(backend)?;
                        if !outcome.dead_letter {
                            pending.insert(job_id, &[] as &[u8]).map_err(backend)?;
                        }
                        ops.push(QueueReplicateOp::Reclaim {
                            lease_id,
                            job_id,
                            attempts: outcome.attempts,
                            dead_letter: outcome.dead_letter,
                            not_before_ms: outcome.not_before_ms,
                        });
                    }
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

    fn dedup_lookup_read(
        dedup: &redb::ReadOnlyTable<&[u8], u64>,
        jobs: &redb::ReadOnlyTable<u64, &[u8]>,
        key: &[u8],
    ) -> Result<Option<u64>, QueueError> {
        let Some(existing) = dedup.get(key).map_err(backend)? else {
            return Ok(None);
        };
        let job_id = existing.value();
        if jobs.get(job_id).map_err(backend)?.is_some() {
            Ok(Some(job_id))
        } else {
            Ok(None)
        }
    }

    fn remove_job_and_dedup(
        jobs: &mut redb::Table<'_, u64, &[u8]>,
        dedup: &mut redb::Table<'_, &[u8], u64>,
        job_id: u64,
    ) -> Result<(), QueueError> {
        if let Some(bytes) = jobs.remove(job_id).map_err(backend)? {
            let stored: StoredJob = decode(bytes.value()).map_err(codec)?;
            if let Some(key) = stored.dedup_key {
                dedup.remove(key.as_slice()).map_err(backend)?;
            }
        }
        Ok(())
    }

    fn maybe_compact_after_ack(&self) -> Result<(), QueueError> {
        if self.acks_since_compact.fetch_add(1, Ordering::Relaxed) + 1 < COMPACT_EVERY_ACKS {
            return Ok(());
        }
        self.acks_since_compact.store(0, Ordering::Relaxed);
        self.db
            .lock()
            .expect("poisoned")
            .compact()
            .map_err(backend)?;
        Ok(())
    }

    fn apply_replicate_inner(&self, op: &QueueReplicateOp) -> Result<(), QueueError> {
        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_write().map_err(backend)?;
        {
            let mut jobs = txn.open_table(JOBS).map_err(backend)?;
            let mut pending = txn.open_table(PENDING).map_err(backend)?;
            let mut leases = txn.open_table(LEASES).map_err(backend)?;
            let mut dedup = txn.open_table(DEDUP).map_err(backend)?;
            let mut meta = txn.open_table(META).map_err(backend)?;
            match op {
                QueueReplicateOp::Enqueue {
                    job_id,
                    payload,
                    enqueued_at_ms,
                    next_job_id,
                    priority,
                    not_before_ms,
                    dedup_key,
                    attempts,
                    max_attempts,
                } => {
                    let mut duplicate = false;
                    if let Some(key) = &dedup_key
                        && let Some(existing) = dedup.get(key.as_slice()).map_err(backend)?
                    {
                        duplicate = jobs.get(existing.value()).map_err(backend)?.is_some();
                    }
                    if duplicate {
                        drop(jobs);
                        drop(pending);
                        drop(leases);
                        drop(dedup);
                        drop(meta);
                        {
                            let mut meta = txn.open_table(META).map_err(backend)?;
                            Self::bump_meta_u64(&mut meta, K_NEXT_JOB, *next_job_id)?;
                        }
                        txn.commit().map_err(backend)?;
                        return Ok(());
                    }
                    if jobs.get(*job_id).map_err(backend)?.is_none() {
                        let stored = StoredJob {
                            payload: payload.clone(),
                            enqueued_at_ms: *enqueued_at_ms,
                            priority: *priority,
                            not_before_ms: *not_before_ms,
                            dedup_key: dedup_key.clone(),
                            attempts: *attempts,
                            max_attempts: *max_attempts,
                            dead_letter: false,
                        };
                        jobs.insert(*job_id, encode(&stored).map_err(codec)?.as_slice())
                            .map_err(backend)?;
                        pending.insert(*job_id, &[] as &[u8]).map_err(backend)?;
                        if let Some(key) = dedup_key {
                            dedup.insert(key.as_slice(), *job_id).map_err(backend)?;
                        }
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
                    Self::remove_job_and_dedup(&mut jobs, &mut dedup, *job_id)?;
                }
                QueueReplicateOp::Nack {
                    lease_id,
                    job_id,
                    attempts,
                    dead_letter,
                    not_before_ms,
                }
                | QueueReplicateOp::Reclaim {
                    lease_id,
                    job_id,
                    attempts,
                    dead_letter,
                    not_before_ms,
                } => {
                    leases.remove(*lease_id).map_err(backend)?;
                    let job_update = jobs
                        .get(*job_id)
                        .map_err(backend)?
                        .map(|bytes| decode::<StoredJob>(bytes.value()).map_err(codec))
                        .transpose()?;
                    if let Some(mut stored) = job_update {
                        stored.attempts = *attempts;
                        stored.dead_letter = *dead_letter;
                        stored.not_before_ms = *not_before_ms;
                        jobs.insert(*job_id, encode(&stored).map_err(codec)?.as_slice())
                            .map_err(backend)?;
                        if !dead_letter {
                            pending.insert(*job_id, &[] as &[u8]).map_err(backend)?;
                        }
                    }
                }
                QueueReplicateOp::RequeueDeadLetter { job_id, attempts } => {
                    let job_update = jobs
                        .get(*job_id)
                        .map_err(backend)?
                        .map(|bytes| decode::<StoredJob>(bytes.value()).map_err(codec))
                        .transpose()?;
                    if let Some(mut stored) = job_update {
                        stored.dead_letter = false;
                        stored.attempts = *attempts;
                        stored.not_before_ms = now_ms();
                        jobs.insert(*job_id, encode(&stored).map_err(codec)?.as_slice())
                            .map_err(backend)?;
                        pending.insert(*job_id, &[] as &[u8]).map_err(backend)?;
                    }
                }
                QueueReplicateOp::UpsertSchedule { schedule } => {
                    let mut schedules = txn.open_table(SCHEDULES).map_err(backend)?;
                    schedules
                        .insert(
                            schedule.name.as_str(),
                            encode(schedule).map_err(codec)?.as_slice(),
                        )
                        .map_err(backend)?;
                }
                QueueReplicateOp::UpdateScheduleNextRun { name, next_run_ms } => {
                    let mut schedules = txn.open_table(SCHEDULES).map_err(backend)?;
                    let schedule_bytes = schedules
                        .get(name.as_str())
                        .map_err(backend)?
                        .map(|bytes| {
                            let mut schedule: crafty_proto::RecurringScheduleWire =
                                decode(bytes.value()).map_err(codec)?;
                            schedule.next_run_ms = *next_run_ms;
                            encode(&schedule).map_err(codec)
                        })
                        .transpose()?;
                    if let Some(bytes) = schedule_bytes {
                        schedules
                            .insert(name.as_str(), bytes.as_slice())
                            .map_err(backend)?;
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

    fn job_status_inner(&self, job_id: JobId) -> Result<Option<JobStatus>, QueueError> {
        self.reclaim_expired()?;
        let now = now_ms();
        let txn = self
            .db
            .lock()
            .expect("poisoned")
            .begin_read()
            .map_err(backend)?;
        let jobs = txn.open_table(JOBS).map_err(backend)?;
        let pending = txn.open_table(PENDING).map_err(backend)?;
        let leases = txn.open_table(LEASES).map_err(backend)?;
        let Some(job_bytes) = jobs.get(job_id.0).map_err(backend)? else {
            return Ok(None);
        };
        let stored: StoredJob = decode(job_bytes.value()).map_err(codec)?;
        let mut leased_by = None;
        for row in leases.iter().map_err(backend)? {
            let (_, bytes) = row.map_err(backend)?;
            let lease: StoredLease = decode(bytes.value()).map_err(codec)?;
            if lease.job_id == job_id.0 {
                leased_by = Some(WorkerId {
                    node: crafty_proto::NodeId(lease.worker_node),
                    instance: lease.worker_instance,
                });
                break;
            }
        }
        let lifecycle = if stored.dead_letter {
            JobLifecycle::DeadLetter
        } else if leased_by.is_some() {
            JobLifecycle::Leased
        } else if stored.not_before_ms > now {
            JobLifecycle::Delayed
        } else if pending.get(job_id.0).map_err(backend)?.is_some() {
            JobLifecycle::Pending
        } else {
            return Ok(None);
        };
        Ok(Some(JobStatus {
            job_id,
            lifecycle,
            payload_len: u64::try_from(stored.payload.len()).unwrap_or(u64::MAX),
            priority: stored.priority,
            leased_by,
            attempts: stored.attempts,
            max_attempts: stored.max_attempts,
        }))
    }

    fn metrics_inner(&self) -> Result<QueueMetrics, QueueError> {
        self.reclaim_expired()?;
        let txn = self
            .db
            .lock()
            .expect("poisoned")
            .begin_read()
            .map_err(backend)?;
        let pending = txn.open_table(PENDING).map_err(backend)?;
        let leases = txn.open_table(LEASES).map_err(backend)?;
        let jobs = txn.open_table(JOBS).map_err(backend)?;

        let now = now_ms();
        let pending_count = ready_pending_count(&jobs, &pending, now)?;
        let leased_count = leases.iter().map_err(backend)?.count() as u64;
        let dead_letter_count = jobs
            .iter()
            .map_err(backend)?
            .filter_map(std::result::Result::ok)
            .filter(|(_, bytes)| {
                decode::<StoredJob>(bytes.value())
                    .ok()
                    .is_some_and(|stored| stored.dead_letter)
            })
            .count() as u64;

        let oldest_ms = pending
            .iter()
            .map_err(backend)?
            .filter_map(std::result::Result::ok)
            .filter_map(|(job_id, _)| {
                let job_id = job_id.value();
                let bytes = jobs.get(job_id).ok().flatten()?;
                let stored: StoredJob = decode(bytes.value()).ok()?;
                (stored.not_before_ms <= now).then_some(stored.enqueued_at_ms)
            })
            .min();

        let oldest_pending_age = oldest_ms
            .map(|ms| Duration::from_millis(now_ms().saturating_sub(ms)))
            .unwrap_or_default();

        Ok(QueueMetrics {
            pending: pending_count,
            leased: leased_count,
            dead_letter: dead_letter_count,
            oldest_pending_age,
        })
    }
}

impl JobQueue for RedbJobQueue {
    fn enqueue_opts<'a>(
        &'a self,
        payload: &'a [u8],
        options: EnqueueOptions,
    ) -> BoxFuture<'a, Result<JobId, QueueError>> {
        Box::pin(async move {
            self.enqueue_opts_replicated(payload, options)
                .await
                .map(|(id, _)| id)
        })
    }

    fn apply_replicate<'a>(
        &'a self,
        op: &'a QueueReplicateOp,
    ) -> BoxFuture<'a, Result<(), QueueError>> {
        Box::pin(async move { self.apply_replicate_inner(op) })
    }

    fn enqueue_opts_replicated<'a>(
        &'a self,
        payload: &'a [u8],
        options: EnqueueOptions,
    ) -> BoxFuture<'a, Result<(JobId, QueueReplicationOps), QueueError>> {
        Box::pin(async move {
            let mut ops = self.reclaim_expired_ops()?;
            if let Some(key) = &options.dedup_key {
                let read = self
                    .db
                    .lock()
                    .expect("poisoned")
                    .begin_read()
                    .map_err(backend)?;
                let dedup = read.open_table(DEDUP).map_err(backend)?;
                let jobs = read.open_table(JOBS).map_err(backend)?;
                if let Some(existing) = Self::dedup_lookup_read(&dedup, &jobs, key)? {
                    return Ok((JobId(existing), ops));
                }
            }
            let job_id = self.read_meta_u64(K_NEXT_JOB)?;
            let enqueued_at_ms = now_ms();
            let not_before_ms = options.not_before_ms.unwrap_or(enqueued_at_ms);
            let stored = StoredJob {
                payload: payload.to_vec(),
                enqueued_at_ms,
                priority: options.priority,
                not_before_ms,
                dedup_key: options.dedup_key.clone(),
                attempts: 0,
                max_attempts: options.max_attempts,
                dead_letter: false,
            };
            let bytes = encode(&stored).map_err(codec)?;
            let next_job_id = job_id + 1;

            let db = self.db.lock().expect("poisoned");
            let txn = db.begin_write().map_err(backend)?;
            {
                let mut jobs = txn.open_table(JOBS).map_err(backend)?;
                let mut pending = txn.open_table(PENDING).map_err(backend)?;
                let mut dedup = txn.open_table(DEDUP).map_err(backend)?;
                let mut meta = txn.open_table(META).map_err(backend)?;
                jobs.insert(job_id, bytes.as_slice()).map_err(backend)?;
                pending.insert(job_id, &[] as &[u8]).map_err(backend)?;
                if let Some(key) = &options.dedup_key {
                    dedup.insert(key.as_slice(), job_id).map_err(backend)?;
                }
                meta.insert(K_NEXT_JOB, encode(&next_job_id).map_err(codec)?.as_slice())
                    .map_err(backend)?;
            }
            txn.commit().map_err(backend)?;

            ops.push(QueueReplicateOp::Enqueue {
                job_id,
                payload: payload.to_vec(),
                enqueued_at_ms,
                next_job_id,
                priority: options.priority,
                not_before_ms,
                dedup_key: options.dedup_key.clone(),
                attempts: 0,
                max_attempts: options.max_attempts,
            });
            Ok((JobId(job_id), ops))
        })
    }

    fn enqueue_replicated<'a>(
        &'a self,
        payload: &'a [u8],
    ) -> BoxFuture<'a, Result<(JobId, QueueReplicationOps), QueueError>> {
        Box::pin(async move {
            self.enqueue_opts_replicated(payload, EnqueueOptions::default())
                .await
        })
    }

    fn lease(
        &self,
        worker: WorkerId,
        max: usize,
    ) -> BoxFuture<'_, Result<Vec<LeasedJob>, QueueError>> {
        Box::pin(async move {
            self.lease_replicated(worker, max)
                .await
                .map(|(jobs, _)| jobs)
        })
    }

    fn lease_replicated(
        &self,
        worker: WorkerId,
        max: usize,
    ) -> BoxFuture<'_, Result<(Vec<LeasedJob>, QueueReplicationOps), QueueError>> {
        Box::pin(async move {
            let mut ops = self.reclaim_expired_ops()?;
            let expires_at_ms =
                now_ms() + u64::try_from(self.lease_timeout.as_millis()).unwrap_or(u64::MAX);
            let mut lease_id_start = self.read_meta_u64(K_NEXT_LEASE)?;
            let now = now_ms();
            let pending_ids = {
                let read = self
                    .db
                    .lock()
                    .expect("poisoned")
                    .begin_read()
                    .map_err(backend)?;
                let jobs = read.open_table(JOBS).map_err(backend)?;
                let pending = read.open_table(PENDING).map_err(backend)?;
                select_pending_ids(&jobs, &pending, max, now)?
            };
            let mut out = Vec::new();

            let db = self.db.lock().expect("poisoned");
            let txn = db.begin_write().map_err(backend)?;
            {
                let jobs = txn.open_table(JOBS).map_err(backend)?;
                let mut pending = txn.open_table(PENDING).map_err(backend)?;
                let mut leases = txn.open_table(LEASES).map_err(backend)?;
                let mut meta = txn.open_table(META).map_err(backend)?;

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

    fn ack(&self, worker: WorkerId, lease_id: LeaseId) -> BoxFuture<'_, Result<(), QueueError>> {
        Box::pin(async move { self.ack_replicated(worker, lease_id).await.map(|_| ()) })
    }

    fn ack_replicated(
        &self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'_, Result<QueueReplicationOps, QueueError>> {
        Box::pin(async move {
            let job_id = {
                let db = self.db.lock().expect("poisoned");
                let txn = db.begin_write().map_err(backend)?;
                let job_id = {
                    let mut jobs = txn.open_table(JOBS).map_err(backend)?;
                    let mut leases = txn.open_table(LEASES).map_err(backend)?;
                    let mut dedup = txn.open_table(DEDUP).map_err(backend)?;
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
                    Self::remove_job_and_dedup(&mut jobs, &mut dedup, stored.job_id)?;
                    stored.job_id
                };
                txn.commit().map_err(backend)?;
                job_id
            };
            self.maybe_compact_after_ack()?;
            Ok(vec![QueueReplicateOp::Ack {
                lease_id: lease_id.0,
                job_id,
            }])
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
            let now = now_ms();
            let (job_id, outcome) = {
                let db = self.db.lock().expect("poisoned");
                let txn = db.begin_write().map_err(backend)?;
                let result = {
                    let mut jobs = txn.open_table(JOBS).map_err(backend)?;
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
                    let mut job: StoredJob = decode(
                        jobs.get(stored.job_id)
                            .map_err(backend)?
                            .ok_or(QueueError::InvalidLease)?
                            .value(),
                    )
                    .map_err(codec)?;
                    let outcome = after_failed_attempt(job.attempts, job.max_attempts, now);
                    job.attempts = outcome.attempts;
                    job.dead_letter = outcome.dead_letter;
                    job.not_before_ms = outcome.not_before_ms;
                    jobs.insert(stored.job_id, encode(&job).map_err(codec)?.as_slice())
                        .map_err(backend)?;
                    if !outcome.dead_letter {
                        pending
                            .insert(stored.job_id, &[] as &[u8])
                            .map_err(backend)?;
                    }
                    Ok((stored.job_id, outcome))
                };
                txn.commit().map_err(backend)?;
                result?
            };
            Ok(vec![QueueReplicateOp::Nack {
                lease_id: lease_id.0,
                job_id,
                attempts: outcome.attempts,
                dead_letter: outcome.dead_letter,
                not_before_ms: outcome.not_before_ms,
            }])
        })
    }

    fn requeue_dead_letter(&self, job_id: JobId) -> BoxFuture<'_, Result<(), QueueError>> {
        Box::pin(async move {
            self.requeue_dead_letter_replicated(job_id)
                .await
                .map(|_| ())
        })
    }

    fn requeue_dead_letter_replicated(
        &self,
        job_id: JobId,
    ) -> BoxFuture<'_, Result<QueueReplicationOps, QueueError>> {
        Box::pin(async move {
            let db = self.db.lock().expect("poisoned");
            let txn = db.begin_write().map_err(backend)?;
            {
                let mut jobs = txn.open_table(JOBS).map_err(backend)?;
                let mut pending = txn.open_table(PENDING).map_err(backend)?;
                let mut stored: StoredJob = decode(
                    jobs.get(job_id.0)
                        .map_err(backend)?
                        .ok_or(QueueError::NotDeadLetter)?
                        .value(),
                )
                .map_err(codec)?;
                if !stored.dead_letter {
                    return Err(QueueError::NotDeadLetter);
                }
                stored.dead_letter = false;
                stored.attempts = 0;
                stored.not_before_ms = now_ms();
                jobs.insert(job_id.0, encode(&stored).map_err(codec)?.as_slice())
                    .map_err(backend)?;
                pending
                    .insert(job_id.0, &[] as &[u8])
                    .map_err(backend)?;
            }
            txn.commit().map_err(backend)?;
            Ok(vec![QueueReplicateOp::RequeueDeadLetter {
                job_id: job_id.0,
                attempts: 0,
            }])
        })
    }

    fn metrics(&self) -> BoxFuture<'_, Result<QueueMetrics, QueueError>> {
        Box::pin(async move { self.metrics_inner() })
    }

    fn job_status(&self, job_id: JobId) -> BoxFuture<'_, Result<Option<JobStatus>, QueueError>> {
        Box::pin(async move { self.job_status_inner(job_id) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crafty_proto::NodeId;
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

    #[tokio::test]
    async fn redb_compact_runs_after_many_acks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("q.redb");
        let q = RedbJobQueue::open(&path, Duration::from_secs(30)).unwrap();
        for i in 0..COMPACT_EVERY_ACKS {
            q.enqueue(format!("job-{i}").as_bytes()).await.unwrap();
            let leased = q.lease(worker(0), 1).await.unwrap();
            q.ack(worker(0), leased[0].lease_id).await.unwrap();
        }
        assert_eq!(q.metrics().await.unwrap().pending, 0);
    }
}
