use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use trembita_proto::{BoxFuture, NodeId, QueueReplicateOp, WorkerId};
use trembita_runtime::{AttemptOutcome, after_failed_attempt};

use super::port::JobQueue;
use super::time::{instant_from_unix_ms, unix_ms_from_instant};
use super::types::{
    EnqueueOptions, JobId, JobLifecycle, JobListFilter, JobListPage, JobStatus, LeaseId, LeasedJob,
    QueueError, QueueMetrics, QueueReplicationOps, job_status_matches_filter,
};

#[derive(Debug)]
struct JobEntry {
    payload: Vec<u8>,
    enqueued_at: Instant,
    priority: u8,
    not_before_ms: u64,
    dedup_key: Option<Vec<u8>>,
    attempts: u32,
    max_attempts: u32,
    dead_letter: bool,
}

#[derive(Debug)]
struct LeaseEntry {
    job_id: JobId,
    worker: WorkerId,
    expires_at: Instant,
}

/// In-process [`JobQueue`] — not durable across restarts.
#[derive(Debug)]
pub struct InMemoryJobQueue {
    lease_timeout: Duration,
    default_max_attempts: u32,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    next_job_id: u64,
    next_lease_id: u64,
    pending: VecDeque<JobId>,
    jobs: BTreeMap<JobId, JobEntry>,
    leases: BTreeMap<LeaseId, LeaseEntry>,
    dedup: BTreeMap<Vec<u8>, JobId>,
}

impl InMemoryJobQueue {
    /// Empty queue with the given visibility timeout for leases.
    #[must_use]
    pub fn new(lease_timeout: Duration) -> Self {
        Self {
            lease_timeout,
            default_max_attempts: 0,
            inner: Mutex::new(Inner {
                next_job_id: 1,
                next_lease_id: 1,
                pending: VecDeque::new(),
                jobs: BTreeMap::new(),
                leases: BTreeMap::new(),
                dedup: BTreeMap::new(),
            }),
        }
    }

    /// Attempt ceiling applied when [`EnqueueOptions::max_attempts`] is `None` (`0` = unlimited).
    #[must_use]
    pub fn default_max_attempts(mut self, max: u32) -> Self {
        self.default_max_attempts = max;
        self
    }

    fn with_inner<R>(&self, f: impl FnOnce(&mut Inner) -> R) -> R {
        let mut inner = self.inner.lock().expect("poisoned");
        inner.reclaim_expired();
        f(&mut inner)
    }

    pub(crate) fn peek_lease_for_settle(
        &self,
        lease_id: LeaseId,
    ) -> Option<(Option<Vec<u8>>, u32)> {
        let inner = self.inner.lock().ok()?;
        let lease = inner.leases.get(&lease_id)?;
        let entry = inner.jobs.get(&lease.job_id)?;
        Some((entry.dedup_key.clone(), entry.attempts))
    }
}

impl Inner {
    fn dedup_lookup(&self, key: &[u8]) -> Option<JobId> {
        self.dedup
            .get(key)
            .copied()
            .filter(|id| self.jobs.contains_key(id))
    }

    fn remove_job(&mut self, job_id: JobId) {
        if let Some(entry) = self.jobs.remove(&job_id)
            && let Some(key) = entry.dedup_key
        {
            self.dedup.remove(&key);
        }
    }

    fn reclaim_expired(&mut self) {
        let now = Instant::now();
        let now_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX);
        let expired: Vec<LeaseId> = self
            .leases
            .iter()
            .filter(|(_, lease)| now >= lease.expires_at)
            .map(|(id, _)| *id)
            .collect();
        for lease_id in expired {
            if let Some(lease) = self.leases.remove(&lease_id)
                && let Some(entry) = self.jobs.get_mut(&lease.job_id)
            {
                let outcome = after_failed_attempt(entry.attempts, entry.max_attempts, now_ms);
                entry.attempts = outcome.attempts;
                entry.dead_letter = outcome.dead_letter;
                entry.not_before_ms = outcome.not_before_ms;
                if !outcome.dead_letter {
                    self.pending.push_back(lease.job_id);
                }
            }
        }
    }

    fn release_expired_lease(
        &mut self,
        lease_id: LeaseId,
        now_ms: u64,
    ) -> Result<(JobId, AttemptOutcome), QueueError> {
        let lease = self
            .leases
            .remove(&lease_id)
            .ok_or(QueueError::InvalidLease)?;
        let entry = self
            .jobs
            .get_mut(&lease.job_id)
            .ok_or(QueueError::InvalidLease)?;
        let outcome = after_failed_attempt(entry.attempts, entry.max_attempts, now_ms);
        entry.attempts = outcome.attempts;
        entry.dead_letter = outcome.dead_letter;
        entry.not_before_ms = outcome.not_before_ms;
        if !outcome.dead_letter {
            self.pending.push_back(lease.job_id);
        }
        Ok((lease.job_id, outcome))
    }

    fn release_lease(
        &mut self,
        lease_id: LeaseId,
        worker: WorkerId,
        now_ms: u64,
    ) -> Result<(JobId, AttemptOutcome), QueueError> {
        let lease = self
            .leases
            .remove(&lease_id)
            .ok_or(QueueError::InvalidLease)?;
        if lease.worker != worker {
            self.leases.insert(lease_id, lease);
            return Err(QueueError::InvalidLease);
        }
        let entry = self
            .jobs
            .get_mut(&lease.job_id)
            .ok_or(QueueError::InvalidLease)?;
        let outcome = after_failed_attempt(entry.attempts, entry.max_attempts, now_ms);
        entry.attempts = outcome.attempts;
        entry.dead_letter = outcome.dead_letter;
        entry.not_before_ms = outcome.not_before_ms;
        if !outcome.dead_letter {
            self.pending.push_back(lease.job_id);
        }
        Ok((lease.job_id, outcome))
    }

    fn requeue_dead_letter(&mut self, job_id: JobId, now_ms: u64) -> Result<(), QueueError> {
        let entry = self
            .jobs
            .get_mut(&job_id)
            .ok_or(QueueError::NotDeadLetter)?;
        if !entry.dead_letter {
            return Err(QueueError::NotDeadLetter);
        }
        entry.dead_letter = false;
        entry.attempts = 0;
        entry.not_before_ms = now_ms;
        self.pending.push_back(job_id);
        Ok(())
    }

    fn job_status(&self, job_id: JobId) -> Option<JobStatus> {
        let entry = self.jobs.get(&job_id)?;
        let now_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX);
        let leased_by = self
            .leases
            .values()
            .find(|lease| lease.job_id == job_id)
            .map(|lease| lease.worker);
        let lifecycle = if entry.dead_letter {
            JobLifecycle::DeadLetter
        } else if leased_by.is_some() {
            JobLifecycle::Leased
        } else if entry.not_before_ms > now_ms {
            JobLifecycle::Delayed
        } else if self.pending.contains(&job_id) {
            JobLifecycle::Pending
        } else {
            return None;
        };
        Some(JobStatus {
            job_id,
            lifecycle,
            payload_len: u64::try_from(entry.payload.len()).unwrap_or(u64::MAX),
            priority: entry.priority,
            leased_by,
            attempts: entry.attempts,
            max_attempts: entry.max_attempts,
            dedup_key: entry.dedup_key.clone(),
        })
    }

    fn list_jobs(&self, filter: &JobListFilter) -> JobListPage {
        let limit = filter.effective_limit();
        let after = filter.after_job_id.map_or(0, |id| id.0);
        let mut jobs = Vec::new();
        let mut has_more = false;
        for job_id in self.jobs.keys().copied().filter(|id| id.0 > after) {
            let Some(status) = self.job_status(job_id) else {
                continue;
            };
            if !job_status_matches_filter(&status, filter) {
                continue;
            }
            jobs.push(status);
            if jobs.len() > limit {
                jobs.pop();
                has_more = true;
                break;
            }
        }
        JobListPage { jobs, has_more }
    }

    fn requeue_dead_letter_batch(
        &mut self,
        job_ids: &[JobId],
        now_ms: u64,
    ) -> (Vec<JobId>, Vec<(JobId, QueueError)>) {
        let mut requeued = Vec::with_capacity(job_ids.len());
        let mut failures = Vec::new();
        for &job_id in job_ids {
            match self.requeue_dead_letter(job_id, now_ms) {
                Ok(()) => requeued.push(job_id),
                Err(e) => failures.push((job_id, e)),
            }
        }
        (requeued, failures)
    }

    fn metrics(&self) -> QueueMetrics {
        let now_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX);
        let oldest = self
            .pending
            .iter()
            .filter_map(|id| self.jobs.get(id))
            .filter(|entry| entry.not_before_ms <= now_ms)
            .map(|entry| entry.enqueued_at.elapsed())
            .max()
            .unwrap_or_default();
        let ready_pending = self
            .pending
            .iter()
            .filter(|id| self.jobs.get(id).is_some_and(|e| e.not_before_ms <= now_ms))
            .count();
        QueueMetrics {
            pending: ready_pending as u64,
            leased: self.leases.len() as u64,
            dead_letter: self.jobs.values().filter(|entry| entry.dead_letter).count() as u64,
            oldest_pending_age: oldest,
            redelivered: self
                .jobs
                .values()
                .filter(|entry| entry.attempts > 0 && !entry.dead_letter)
                .count() as u64,
        }
    }

    fn select_pending(&self, max: usize, now_ms: u64) -> Vec<JobId> {
        let mut ready: Vec<JobId> = self
            .pending
            .iter()
            .filter(|id| self.jobs.get(id).is_some_and(|e| e.not_before_ms <= now_ms))
            .copied()
            .collect();
        ready.sort_by(|a, b| {
            let ea = self.jobs.get(a).expect("pending job");
            let eb = self.jobs.get(b).expect("pending job");
            eb.priority.cmp(&ea.priority).then_with(|| a.cmp(b))
        });
        ready.truncate(max);
        ready
    }
}

impl JobQueue for InMemoryJobQueue {
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

    fn enqueue_opts_replicated<'a>(
        &'a self,
        payload: &'a [u8],
        options: EnqueueOptions,
    ) -> BoxFuture<'a, Result<(JobId, QueueReplicationOps), QueueError>> {
        Box::pin(async move {
            let enqueued_at_ms = u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
            )
            .unwrap_or(u64::MAX);
            let not_before_ms = options.not_before_ms.unwrap_or(enqueued_at_ms);
            let max_attempts = options.max_attempts.unwrap_or(self.default_max_attempts);
            let (job_id, ops) = self.with_inner(|inner| {
                if let Some(key) = &options.dedup_key
                    && let Some(existing) = inner.dedup_lookup(key)
                {
                    return (existing, Vec::new());
                }
                let job_id = inner.next_job_id;
                inner.next_job_id += 1;
                let dedup_key = options.dedup_key.clone();
                inner.jobs.insert(
                    JobId(job_id),
                    JobEntry {
                        payload: payload.to_vec(),
                        enqueued_at: Instant::now(),
                        priority: options.priority,
                        not_before_ms,
                        dedup_key: dedup_key.clone(),
                        attempts: 0,
                        max_attempts,
                        dead_letter: false,
                    },
                );
                inner.pending.push_back(JobId(job_id));
                if let Some(key) = dedup_key {
                    inner.dedup.insert(key, JobId(job_id));
                }
                (
                    JobId(job_id),
                    vec![QueueReplicateOp::Enqueue {
                        job_id,
                        payload: payload.to_vec(),
                        enqueued_at_ms,
                        next_job_id: inner.next_job_id,
                        priority: options.priority,
                        not_before_ms,
                        dedup_key: options.dedup_key.clone(),
                        attempts: 0,
                        max_attempts,
                    }],
                )
            });
            Ok((job_id, ops))
        })
    }

    #[allow(clippy::too_many_lines)] // large replicate-op match
    fn apply_replicate<'a>(
        &'a self,
        op: &'a QueueReplicateOp,
    ) -> BoxFuture<'a, Result<(), QueueError>> {
        Box::pin(async move {
            self.with_inner(|inner| match op {
                QueueReplicateOp::Enqueue {
                    job_id,
                    payload,
                    enqueued_at_ms: _,
                    next_job_id,
                    priority,
                    not_before_ms,
                    dedup_key,
                    attempts,
                    max_attempts,
                } => {
                    if let Some(key) = dedup_key
                        && inner.dedup_lookup(key).is_some()
                    {
                        inner.next_job_id = inner.next_job_id.max(*next_job_id);
                        return Ok(());
                    }
                    if let std::collections::btree_map::Entry::Vacant(entry) =
                        inner.jobs.entry(JobId(*job_id))
                    {
                        entry.insert(JobEntry {
                            payload: payload.clone(),
                            enqueued_at: Instant::now(),
                            priority: *priority,
                            not_before_ms: *not_before_ms,
                            dedup_key: dedup_key.clone(),
                            attempts: *attempts,
                            max_attempts: *max_attempts,
                            dead_letter: false,
                        });
                        inner.pending.push_back(JobId(*job_id));
                        if let Some(key) = dedup_key {
                            inner.dedup.insert(key.clone(), JobId(*job_id));
                        }
                    }
                    inner.next_job_id = inner.next_job_id.max(*next_job_id);
                    Ok(())
                }
                QueueReplicateOp::Lease {
                    lease_id,
                    job_id,
                    worker_node,
                    worker_instance,
                    expires_at_ms: _,
                    next_lease_id,
                } => {
                    inner.pending.retain(|id| id.0 != *job_id);
                    inner
                        .leases
                        .entry(LeaseId(*lease_id))
                        .or_insert(LeaseEntry {
                            job_id: JobId(*job_id),
                            worker: WorkerId {
                                node: NodeId(*worker_node),
                                instance: *worker_instance,
                            },
                            expires_at: Instant::now() + Duration::from_secs(3600),
                        });
                    inner.next_lease_id = inner.next_lease_id.max(*next_lease_id);
                    Ok(())
                }
                QueueReplicateOp::Ack { lease_id, job_id } => {
                    inner.leases.remove(&LeaseId(*lease_id));
                    inner.remove_job(JobId(*job_id));
                    Ok(())
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
                    inner.leases.remove(&LeaseId(*lease_id));
                    if let Some(entry) = inner.jobs.get_mut(&JobId(*job_id)) {
                        entry.attempts = *attempts;
                        entry.dead_letter = *dead_letter;
                        entry.not_before_ms = *not_before_ms;
                        if !dead_letter {
                            inner.pending.push_back(JobId(*job_id));
                        }
                    }
                    Ok(())
                }
                QueueReplicateOp::RequeueDeadLetter { job_id, attempts } => {
                    if let Some(entry) = inner.jobs.get_mut(&JobId(*job_id)) {
                        entry.dead_letter = false;
                        entry.attempts = *attempts;
                        entry.not_before_ms = u64::try_from(
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis(),
                        )
                        .unwrap_or(u64::MAX);
                        inner.pending.push_back(JobId(*job_id));
                    }
                    Ok(())
                }
                QueueReplicateOp::ExtendLease {
                    lease_id,
                    worker_node,
                    worker_instance,
                    expires_at_ms,
                } => {
                    if let Some(lease) = inner.leases.get_mut(&LeaseId(*lease_id))
                        && lease.worker.node.0 == *worker_node
                        && lease.worker.instance == *worker_instance
                    {
                        lease.expires_at = instant_from_unix_ms(*expires_at_ms);
                    }
                    Ok(())
                }
                QueueReplicateOp::UpsertSchedule { .. }
                | QueueReplicateOp::UpdateScheduleNextRun { .. }
                | QueueReplicateOp::RemoveSchedule { .. } => Ok(()),
            })
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
            let mut ops = Vec::new();
            let now = Instant::now();
            let expired: Vec<(LeaseId, JobId)> = {
                let inner = self.inner.lock().expect("poisoned");
                inner
                    .leases
                    .iter()
                    .filter(|(_, lease)| now >= lease.expires_at)
                    .map(|(id, lease)| (*id, lease.job_id))
                    .collect()
            };
            let now_ms = u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
            )
            .unwrap_or(u64::MAX);
            for (lease_id, _job_id) in expired {
                let op = self.with_inner(|inner| {
                    let (job_id, outcome) = inner.release_expired_lease(lease_id, now_ms)?;
                    Ok::<_, QueueError>(QueueReplicateOp::Reclaim {
                        lease_id: lease_id.0,
                        job_id: job_id.0,
                        attempts: outcome.attempts,
                        dead_letter: outcome.dead_letter,
                        not_before_ms: outcome.not_before_ms,
                    })
                });
                if let Ok(op) = op {
                    ops.push(op);
                }
            }

            let (jobs, lease_ops) = self.with_inner(|inner| {
                let mut out = Vec::new();
                let mut lease_ops = Vec::new();
                let deadline = Instant::now() + self.lease_timeout;
                let now_ms = u64::try_from(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis(),
                )
                .unwrap_or(u64::MAX);
                for job_id in inner.select_pending(max, now_ms) {
                    inner.pending.retain(|id| *id != job_id);
                    let Some(entry) = inner.jobs.get(&job_id) else {
                        continue;
                    };
                    let lease_id = inner.next_lease_id;
                    inner.next_lease_id += 1;
                    inner.leases.insert(
                        LeaseId(lease_id),
                        LeaseEntry {
                            job_id,
                            worker,
                            expires_at: deadline,
                        },
                    );
                    lease_ops.push(QueueReplicateOp::Lease {
                        lease_id,
                        job_id: job_id.0,
                        worker_node: worker.node.0,
                        worker_instance: worker.instance,
                        expires_at_ms: 0,
                        next_lease_id: inner.next_lease_id,
                    });
                    out.push(LeasedJob {
                        lease_id: LeaseId(lease_id),
                        job_id,
                        payload: entry.payload.clone(),
                        // `entry.attempts` counts attempts that already failed;
                        // this delivery is the next one.
                        attempts: entry.attempts + 1,
                        dedup_key: entry.dedup_key.clone(),
                    });
                }
                (out, lease_ops)
            });
            ops.extend(lease_ops);
            Ok((jobs, ops))
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
            let job_id = self.with_inner(|inner| {
                let lease = inner
                    .leases
                    .remove(&lease_id)
                    .ok_or(QueueError::InvalidLease)?;
                if lease.worker != worker {
                    inner.leases.insert(lease_id, lease);
                    return Err(QueueError::InvalidLease);
                }
                inner.remove_job(lease.job_id);
                Ok(lease.job_id.0)
            })?;
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
            let now_ms = u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
            )
            .unwrap_or(u64::MAX);
            let (job_id, outcome) =
                self.with_inner(|inner| inner.release_lease(lease_id, worker, now_ms))?;
            Ok(vec![QueueReplicateOp::Nack {
                lease_id: lease_id.0,
                job_id: job_id.0,
                attempts: outcome.attempts,
                dead_letter: outcome.dead_letter,
                not_before_ms: outcome.not_before_ms,
            }])
        })
    }

    fn extend_lease_replicated(
        &self,
        worker: WorkerId,
        lease_id: LeaseId,
    ) -> BoxFuture<'_, Result<QueueReplicationOps, QueueError>> {
        Box::pin(async move {
            let expires_at = Instant::now() + self.lease_timeout;
            let expires_at_ms = unix_ms_from_instant(expires_at);
            self.with_inner(|inner| {
                let lease = inner
                    .leases
                    .get_mut(&lease_id)
                    .ok_or(QueueError::InvalidLease)?;
                if lease.worker != worker {
                    return Err(QueueError::InvalidLease);
                }
                lease.expires_at = expires_at;
                Ok(())
            })?;
            Ok(vec![QueueReplicateOp::ExtendLease {
                lease_id: lease_id.0,
                worker_node: worker.node.0,
                worker_instance: worker.instance,
                expires_at_ms,
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
            let now_ms = u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
            )
            .unwrap_or(u64::MAX);
            self.with_inner(|inner| inner.requeue_dead_letter(job_id, now_ms))?;
            Ok(vec![QueueReplicateOp::RequeueDeadLetter {
                job_id: job_id.0,
                attempts: 0,
            }])
        })
    }

    fn metrics(&self) -> BoxFuture<'_, Result<QueueMetrics, QueueError>> {
        Box::pin(async move { Ok(self.with_inner(|inner| inner.metrics())) })
    }

    fn job_status(&self, job_id: JobId) -> BoxFuture<'_, Result<Option<JobStatus>, QueueError>> {
        Box::pin(async move { Ok(self.with_inner(|inner| inner.job_status(job_id))) })
    }

    fn list_jobs(&self, filter: JobListFilter) -> BoxFuture<'_, Result<JobListPage, QueueError>> {
        Box::pin(async move { Ok(self.with_inner(|inner| inner.list_jobs(&filter))) })
    }

    fn requeue_dead_letter_batch_replicated<'a>(
        &'a self,
        job_ids: &'a [JobId],
    ) -> BoxFuture<
        'a,
        Result<(Vec<JobId>, Vec<(JobId, QueueError)>, QueueReplicationOps), QueueError>,
    > {
        Box::pin(async move {
            let now_ms = u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
            )
            .unwrap_or(u64::MAX);
            let (requeued, failures) =
                self.with_inner(|inner| Ok(inner.requeue_dead_letter_batch(job_ids, now_ms)))?;
            let ops: QueueReplicationOps = requeued
                .iter()
                .map(|job_id| QueueReplicateOp::RequeueDeadLetter {
                    job_id: job_id.0,
                    attempts: 0,
                })
                .collect();
            Ok((requeued, failures, ops))
        })
    }

    fn peek_lease_meta(&self, lease_id: LeaseId) -> BoxFuture<'_, Option<(Option<Vec<u8>>, u32)>> {
        Box::pin(async move { self.peek_lease_for_settle(lease_id) })
    }
}
