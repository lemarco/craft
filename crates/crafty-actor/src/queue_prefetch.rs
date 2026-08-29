//! In-memory prefetch on the queue leader — recent enqueues stay in RAM so
//! [`lease`](super::JobQueue::lease) skips re-reading payloads from `redb`.

/// Default max jobs per batch enqueue / ack RPC ([job-queue](../../../docs/decisions/job-queue.md)).
pub const DEFAULT_QUEUE_BATCH_MAX: usize = 256;

/// Default in-memory prefetch depth per stream on the queue leader.
pub const DEFAULT_QUEUE_PREFETCH: usize = 256;

/// A pending job cached on the leader after enqueue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CachedPendingJob {
    pub job_id: u64,
    pub payload: Vec<u8>,
    pub priority: u8,
    pub not_before_ms: u64,
}

/// Leader-side RAM buffer of recently enqueued jobs (not durable across failover).
#[derive(Debug)]
pub(crate) struct QueuePrefetchCache {
    pending: Vec<CachedPendingJob>,
    capacity: usize,
}

impl QueuePrefetchCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            pending: Vec::new(),
            capacity: capacity.max(1),
        }
    }

    pub fn insert_enqueued(&mut self, job: CachedPendingJob) {
        if self.pending.len() >= self.capacity {
            return;
        }
        let pos = self
            .pending
            .binary_search_by(|probe| {
                probe
                    .priority
                    .cmp(&job.priority)
                    .reverse()
                    .then_with(|| probe.job_id.cmp(&job.job_id))
            })
            .unwrap_or_else(|i| i);
        self.pending.insert(pos, job);
    }

    pub fn select_for_lease(&mut self, max: usize, now_ms: u64) -> Vec<CachedPendingJob> {
        let mut out = Vec::new();
        self.pending.retain(|job| {
            if out.len() >= max {
                return true;
            }
            if job.not_before_ms <= now_ms {
                out.push(job.clone());
                false
            } else {
                true
            }
        });
        out
    }

    pub fn remove_job(&mut self, job_id: u64) {
        self.pending.retain(|j| j.job_id != job_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn higher_priority_leased_first() {
        let mut cache = QueuePrefetchCache::new(8);
        cache.insert_enqueued(CachedPendingJob {
            job_id: 1,
            payload: b"low".to_vec(),
            priority: 0,
            not_before_ms: 0,
        });
        cache.insert_enqueued(CachedPendingJob {
            job_id: 2,
            payload: b"high".to_vec(),
            priority: 9,
            not_before_ms: 0,
        });
        let leased = cache.select_for_lease(1, 0);
        assert_eq!(leased[0].job_id, 2);
    }
}
