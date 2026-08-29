//! Cron-driven recurring jobs on a [`RedbJobQueue`](super::redb_queue::RedbJobQueue).

use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crafty_proto::{QueueReplicateOp, RecurringScheduleWire, decode, encode};
use cron::Schedule;
use redb::{ReadableTable, TableDefinition};

use super::redb_queue::RedbJobQueue;
use super::{EnqueueOptions, JobQueue, QueueError, QueueReplicationOps};

const SCHEDULES: TableDefinition<&str, &[u8]> = TableDefinition::new("queue_schedules");

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

/// User-facing recurring job registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecurringJob {
    /// Unique name within the queue stream.
    pub name: String,
    /// Cron expression (5-field or 6-field; see [`parse_cron`]).
    pub cron: String,
    /// Payload enqueued on each tick.
    pub payload: Vec<u8>,
    /// Enqueue priority.
    pub priority: u8,
    /// Retry ceiling passed to each enqueued job (`0` = unlimited).
    pub max_attempts: u32,
    /// When false the schedule is stored but does not fire.
    pub enabled: bool,
}

impl RecurringJob {
    /// Recurring job with defaults (enabled, priority 0, unlimited retries).
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        cron: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            name: name.into(),
            cron: cron.into(),
            payload: payload.into(),
            priority: 0,
            max_attempts: 0,
            enabled: true,
        }
    }

    /// Set enqueue priority for each tick.
    #[must_use]
    pub fn priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Cap retries for jobs produced by this schedule.
    #[must_use]
    pub fn max_attempts(mut self, max: u32) -> Self {
        self.max_attempts = max;
        self
    }

    fn to_wire(&self, next_run_ms: u64) -> RecurringScheduleWire {
        RecurringScheduleWire {
            name: self.name.clone(),
            cron: self.cron.clone(),
            payload: self.payload.clone(),
            priority: self.priority,
            max_attempts: self.max_attempts,
            enabled: self.enabled,
            next_run_ms,
        }
    }
}

/// Parse a cron expression — accepts 5-field (`min hour dom month dow`) or
/// 6-field (`sec min hour dom month dow`) syntax.
///
/// # Errors
/// Returns [`QueueError::Codec`] when the expression is invalid.
pub fn parse_cron(expr: &str) -> Result<Schedule, QueueError> {
    let normalized = normalize_cron(expr)?;
    Schedule::from_str(&normalized).map_err(codec)
}

fn normalize_cron(expr: &str) -> Result<String, QueueError> {
    let fields: Vec<&str> = expr.split_whitespace().collect();
    match fields.len() {
        5 => Ok(format!("0 {}", fields.join(" "))),
        6 => Ok(fields.join(" ")),
        n => Err(codec(format!("cron expression needs 5 or 6 fields, got {n}"))),
    }
}

fn next_run_after(schedule: &Schedule, after_ms: u64) -> Result<u64, QueueError> {
    let after = UNIX_EPOCH
        + Duration::from_millis(after_ms.min(i64::MAX as u64))
        + Duration::from_millis(1);
    let next = schedule
        .after(&after)
        .next()
        .ok_or_else(|| codec("cron schedule has no upcoming fire time"))?;
    Ok(u64::try_from(
        next.duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX))
}

impl RedbJobQueue {
    /// Upsert a recurring schedule into the queue redb file.
    ///
    /// # Errors
    /// Returns [`QueueError::Backend`] or [`QueueError::Codec`] on failure.
    pub fn upsert_schedule(&self, job: &RecurringJob) -> Result<QueueReplicationOps, QueueError> {
        let schedule = parse_cron(&job.cron)?;
        let next_run_ms = next_run_after(&schedule, now_ms())?;
        let wire = job.to_wire(next_run_ms);
        let bytes = encode(&wire).map_err(codec)?;
        let db = self.db.lock().expect("poisoned");
        let txn = db.begin_write().map_err(backend)?;
        {
            let mut schedules = txn.open_table(SCHEDULES).map_err(backend)?;
            schedules
                .insert(job.name.as_str(), bytes.as_slice())
                .map_err(backend)?;
        }
        txn.commit().map_err(backend)?;
        Ok(vec![QueueReplicateOp::UpsertSchedule { schedule: wire }])
    }

    /// Fire due schedules and enqueue their payloads.
    ///
    /// # Errors
    /// Returns [`QueueError::Backend`] or [`QueueError::Codec`] on failure.
    pub async fn tick_schedules(&self) -> Result<QueueReplicationOps, QueueError> {
        let now = now_ms();
        let due: Vec<RecurringScheduleWire> = {
            let db = self.db.lock().expect("poisoned");
            let txn = db.begin_read().map_err(backend)?;
            let schedules = txn.open_table(SCHEDULES).map_err(backend)?;
            schedules
                .iter()
                .map_err(backend)?
                .filter_map(std::result::Result::ok)
                .filter_map(|(_name, bytes)| {
                    let schedule: RecurringScheduleWire = decode(bytes.value()).ok()?;
                    (schedule.enabled && schedule.next_run_ms <= now).then_some(schedule)
                })
                .collect()
        };

        let mut ops = Vec::new();
        for mut schedule in due {
            let cron = parse_cron(&schedule.cron)?;
            let dedup_key = format!("recurring:{}", schedule.name).into_bytes();
            let (_, enqueue_ops) = self
                .enqueue_opts_replicated(
                    &schedule.payload,
                    EnqueueOptions {
                        priority: schedule.priority,
                        dedup_key: Some(dedup_key),
                        max_attempts: schedule.max_attempts,
                        ..EnqueueOptions::default()
                    },
                )
                .await?;
            ops.extend(enqueue_ops);

            schedule.next_run_ms = next_run_after(&cron, now)?;
            let bytes = encode(&schedule).map_err(codec)?;
            {
                let db = self.db.lock().expect("poisoned");
                let txn = db.begin_write().map_err(backend)?;
                {
                    let mut schedules = txn.open_table(SCHEDULES).map_err(backend)?;
                    schedules
                        .insert(schedule.name.as_str(), bytes.as_slice())
                        .map_err(backend)?;
                }
                txn.commit().map_err(backend)?;
            }
            ops.push(QueueReplicateOp::UpdateScheduleNextRun {
                name: schedule.name.clone(),
                next_run_ms: schedule.next_run_ms,
            });
        }
        Ok(ops)
    }
}

/// Leader-only loop: fire due cron schedules with voter replication.
pub async fn run_queue_schedule_ticker(
    service: Arc<crate::queue_service::QueueService>,
    poll_interval: Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        if *stop.borrow() {
            break;
        }
        let _ = service.tick_schedules().await;
        tokio::select! {
            () = tokio::time::sleep(poll_interval) => {}
            _ = stop.changed() => {
                if *stop.borrow() {
                    break;
                }
            }
        }
    }
}

/// Fire due cron schedules on a single-node [`RedbJobQueue`] (tests / dev).
pub async fn run_recurring_job_ticker(
    queue: Arc<RedbJobQueue>,
    poll_interval: Duration,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        if *stop.borrow() {
            break;
        }
        let _ = queue.tick_schedules().await;
        tokio::select! {
            () = tokio::time::sleep(poll_interval) => {}
            _ = stop.changed() => {
                if *stop.borrow() {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JobQueue;

    #[test]
    fn normalize_five_field_cron() {
        let schedule = parse_cron("0 9 * * *").unwrap();
        let next = next_run_after(&schedule, now_ms()).unwrap();
        assert!(next > now_ms());
    }

    #[tokio::test]
    async fn recurring_job_enqueues_on_tick() {
        let dir = tempfile::tempdir().unwrap();
        let queue = RedbJobQueue::open(dir.path().join("q.redb"), Duration::from_secs(30)).unwrap();
        let mut wire = RecurringJob::new("daily", "* * * * * *", b"tick")
            .max_attempts(3)
            .to_wire(0);
        wire.next_run_ms = now_ms().saturating_sub(1);
        queue
            .apply_replicate(&QueueReplicateOp::UpsertSchedule { schedule: wire })
            .await
            .unwrap();

        let ops = queue.tick_schedules().await.unwrap();
        assert!(!ops.is_empty());
        assert!(queue.metrics().await.unwrap().pending >= 1);
    }
}
