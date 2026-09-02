//! Dynamic recurring-job schedules via a [`ScheduleSource`] port
//! ([schedule-source](../../../docs/decisions/schedule-source.md)).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use trembita_proto::RecurringScheduleWire;

use crate::{QueueError, QueueReplicationOps, RecurringJob, RedbJobQueue};
use trembita_actor_store::BoxFuture;

/// Why a [`ScheduleSource`] poll failed.
#[derive(Debug, thiserror::Error)]
pub enum ScheduleError {
    /// Backend (database, network) error.
    #[error("schedule source error: {0}")]
    Backend(String),
}

/// Supplies the current recurring-job set for a queue stream.
pub trait ScheduleSource: Send + Sync {
    /// Return the desired schedules for this poll.
    fn schedules(&self) -> BoxFuture<'_, Result<Vec<RecurringJob>, ScheduleError>>;
}

/// Fixed schedule list (used by [`.cron()`](../../crates/trembita/src/app.rs) at boot).
#[derive(Debug, Clone)]
pub struct StaticScheduleSource {
    jobs: Vec<RecurringJob>,
}

impl StaticScheduleSource {
    /// Never changes — safe to poll at any interval.
    #[must_use]
    pub fn new(jobs: Vec<RecurringJob>) -> Self {
        Self { jobs }
    }
}

impl ScheduleSource for StaticScheduleSource {
    fn schedules(&self) -> BoxFuture<'_, Result<Vec<RecurringJob>, ScheduleError>> {
        let jobs = self.jobs.clone();
        Box::pin(async move { Ok(jobs) })
    }
}

/// Merge several sources (static `.cron()` + external store, …).
#[derive(Clone)]
pub struct CompositeScheduleSource {
    sources: Vec<Arc<dyn ScheduleSource>>,
}

impl CompositeScheduleSource {
    /// Poll each source in order; fail fast on the first error.
    #[must_use]
    pub fn new(sources: Vec<Arc<dyn ScheduleSource>>) -> Self {
        Self { sources }
    }
}

impl ScheduleSource for CompositeScheduleSource {
    fn schedules(&self) -> BoxFuture<'_, Result<Vec<RecurringJob>, ScheduleError>> {
        let sources = self.sources.clone();
        Box::pin(async move {
            let mut jobs = Vec::new();
            for source in sources {
                jobs.extend(source.schedules().await?);
            }
            Ok(jobs)
        })
    }
}

/// Leader poll interval for a [`ScheduleSource`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulePoll(Duration);

impl SchedulePoll {
    /// Poll every `secs` seconds on the queue leader.
    #[must_use]
    pub fn secs(secs: u64) -> Self {
        Self(Duration::from_secs(secs.max(1)))
    }

    /// Poll every `millis` milliseconds (minimum 100 ms).
    #[must_use]
    pub fn millis(millis: u64) -> Self {
        Self(Duration::from_millis(millis.max(100)))
    }

    /// Inner duration.
    #[must_use]
    pub fn duration(self) -> Duration {
        self.0
    }
}

/// Planned mutations from [`plan_schedule_reconcile`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ScheduleReconcilePlan {
    /// Upsert these jobs (new or changed).
    pub upsert: Vec<RecurringJob>,
    /// Remove schedules with these names.
    pub remove: Vec<String>,
}

/// Diff `loaded` vs `desired` recurring jobs.
#[must_use]
pub fn plan_schedule_reconcile(
    loaded: &[RecurringJob],
    desired: &[RecurringJob],
) -> ScheduleReconcilePlan {
    let loaded_map: HashMap<&str, &RecurringJob> =
        loaded.iter().map(|job| (job.name.as_str(), job)).collect();
    let desired_map: HashMap<&str, &RecurringJob> =
        desired.iter().map(|job| (job.name.as_str(), job)).collect();

    let mut upsert = Vec::new();
    for job in desired {
        let needs_upsert = match loaded_map.get(job.name.as_str()) {
            None => true,
            Some(existing) => **existing != *job,
        };
        if needs_upsert {
            upsert.push(job.clone());
        }
    }

    let mut remove = Vec::new();
    for name in loaded_map.keys() {
        if !desired_map.contains_key(name) {
            remove.push((*name).to_string());
        }
    }

    ScheduleReconcilePlan { upsert, remove }
}

/// Convert persisted wire to user-facing job (drops leader `next_run_ms`).
#[must_use]
pub fn wire_to_recurring_job(wire: &RecurringScheduleWire) -> RecurringJob {
    RecurringJob {
        name: wire.name.clone(),
        cron: wire.cron.clone(),
        payload: wire.payload.clone(),
        priority: wire.priority,
        max_attempts: wire.max_attempts,
        enabled: wire.enabled,
    }
}

impl RedbJobQueue {
    /// List recurring schedules stored in this queue file.
    ///
    /// # Errors
    /// Returns [`QueueError::Backend`] or [`QueueError::Codec`] on failure.
    ///
    /// # Panics
    /// If the redb mutex is poisoned.
    pub fn list_schedules(&self) -> Result<Vec<RecurringJob>, QueueError> {
        use super::queue_schedule::SCHEDULES_TABLE;
        use redb::{ReadableDatabase, ReadableTable};
        use trembita_proto::decode;

        fn backend(e: impl std::fmt::Display) -> QueueError {
            QueueError::Backend(e.to_string())
        }
        fn codec(e: impl std::fmt::Display) -> QueueError {
            QueueError::Codec(e.to_string())
        }

        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_read().map_err(backend)?;
        let schedules = txn.open_table(SCHEDULES_TABLE).map_err(backend)?;
        schedules
            .iter()
            .map_err(backend)?
            .map(|row| {
                let (_, bytes) = row.map_err(backend)?;
                let wire: RecurringScheduleWire = decode(bytes.value()).map_err(codec)?;
                Ok(wire_to_recurring_job(&wire))
            })
            .collect()
    }

    /// Remove a schedule by name.
    ///
    /// # Errors
    /// Returns [`QueueError::Backend`] on failure.
    ///
    /// # Panics
    /// If the redb mutex is poisoned.
    pub fn remove_schedule(&self, name: &str) -> Result<QueueReplicationOps, QueueError> {
        use super::queue_schedule::SCHEDULES_TABLE;
        use trembita_proto::QueueReplicateOp;

        fn backend(e: impl std::fmt::Display) -> QueueError {
            QueueError::Backend(e.to_string())
        }

        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_write().map_err(backend)?;
        {
            let mut schedules = txn.open_table(SCHEDULES_TABLE).map_err(backend)?;
            schedules.remove(name).map_err(backend)?;
        }
        txn.commit().map_err(backend)?;
        Ok(vec![QueueReplicateOp::RemoveSchedule {
            name: name.to_string(),
        }])
    }

    /// Apply a reconcile plan against local redb.
    ///
    /// # Errors
    /// Returns [`QueueError::Backend`] or [`QueueError::Codec`] on failure.
    pub fn reconcile_schedules(
        &self,
        desired: &[RecurringJob],
    ) -> Result<QueueReplicationOps, QueueError> {
        let loaded = self.list_schedules()?;
        let plan = plan_schedule_reconcile(&loaded, desired);
        let mut ops = Vec::new();
        for job in plan.upsert {
            ops.extend(self.upsert_schedule(&job)?);
        }
        for name in plan.remove {
            ops.extend(self.remove_schedule(&name)?);
        }
        Ok(ops)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    struct FlakySource {
        calls: AtomicUsize,
        fail_until: usize,
        jobs: Vec<RecurringJob>,
    }

    impl ScheduleSource for FlakySource {
        fn schedules(&self) -> BoxFuture<'_, Result<Vec<RecurringJob>, ScheduleError>> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            let jobs = self.jobs.clone();
            Box::pin(async move {
                if n < self.fail_until {
                    Err(ScheduleError::Backend("down".into()))
                } else {
                    Ok(jobs)
                }
            })
        }
    }

    #[test]
    fn plan_upserts_new_and_changed() {
        let loaded = vec![RecurringJob::new("a", "0 9 * * *", b"1")];
        let desired = vec![
            RecurringJob::new("a", "0 10 * * *", b"1"),
            RecurringJob::new("b", "0 9 * * *", b"2"),
        ];
        let plan = plan_schedule_reconcile(&loaded, &desired);
        assert_eq!(plan.remove, Vec::<String>::new());
        assert_eq!(plan.upsert.len(), 2);
    }

    #[test]
    fn plan_removes_missing_names() {
        let loaded = vec![
            RecurringJob::new("a", "0 9 * * *", b"1"),
            RecurringJob::new("b", "0 9 * * *", b"2"),
        ];
        let desired = vec![RecurringJob::new("a", "0 9 * * *", b"1")];
        let plan = plan_schedule_reconcile(&loaded, &desired);
        assert!(plan.upsert.is_empty());
        assert_eq!(plan.remove, vec!["b".to_string()]);
    }

    #[tokio::test]
    async fn reconcile_applies_diff_to_redb() {
        let dir = tempfile::tempdir().unwrap();
        let queue = RedbJobQueue::open(dir.path().join("q.redb"), Duration::from_secs(30)).unwrap();
        queue
            .upsert_schedule(&RecurringJob::new("keep", "0 9 * * *", b"k"))
            .unwrap();
        queue
            .upsert_schedule(&RecurringJob::new("drop", "0 9 * * *", b"d"))
            .unwrap();

        let desired = vec![
            RecurringJob::new("keep", "0 9 * * *", b"k"),
            RecurringJob::new("new", "0 9 * * *", b"n"),
        ];
        queue.reconcile_schedules(&desired).unwrap();
        let names: Vec<_> = queue
            .list_schedules()
            .unwrap()
            .into_iter()
            .map(|j| j.name)
            .collect();
        assert_eq!(names, vec!["keep".to_string(), "new".to_string()]);
    }

    #[tokio::test]
    async fn source_error_does_not_wipe_redb() {
        let dir = tempfile::tempdir().unwrap();
        let queue = RedbJobQueue::open(dir.path().join("q.redb"), Duration::from_secs(30)).unwrap();
        queue
            .upsert_schedule(&RecurringJob::new("daily", "0 9 * * *", b"x"))
            .unwrap();

        let source = Arc::new(FlakySource {
            calls: AtomicUsize::new(0),
            fail_until: 5,
            jobs: vec![],
        }) as Arc<dyn ScheduleSource>;
        let mut last_good: Option<Vec<RecurringJob>> = None;
        for _ in 0..3 {
            match source.schedules().await {
                Err(_) => {}
                Ok(desired) => {
                    let apply = !(desired.is_empty() && last_good.is_none());
                    if apply {
                        queue.reconcile_schedules(&desired).unwrap();
                        last_good = Some(desired);
                    }
                }
            }
        }
        assert_eq!(queue.list_schedules().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn empty_first_poll_does_not_wipe_redb() {
        let dir = tempfile::tempdir().unwrap();
        let queue = RedbJobQueue::open(dir.path().join("q.redb"), Duration::from_secs(30)).unwrap();
        queue
            .upsert_schedule(&RecurringJob::new("daily", "0 9 * * *", b"x"))
            .unwrap();

        let source = Arc::new(StaticScheduleSource::new(vec![])) as Arc<dyn ScheduleSource>;
        let last_good: Option<Vec<RecurringJob>> = None;
        let desired = source.schedules().await.unwrap();
        let apply = !(desired.is_empty() && last_good.is_none());
        assert!(!apply);
        assert_eq!(queue.list_schedules().unwrap().len(), 1);
    }
}
