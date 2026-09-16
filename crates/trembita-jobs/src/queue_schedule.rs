//! Cron-driven recurring jobs on a [`RedbJobQueue`](super::redb_queue::RedbJobQueue).

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, NaiveTime, TimeZone, Utc};
use cron::Schedule;
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use trembita_proto::{
    JobPriority, MaxAttempts, QueueReplicateOp, RecurringScheduleWire, decode, encode,
};
use trembita_storage::now_ms;

use super::redb_queue::RedbJobQueue;
use super::work_trigger::WorkTrigger;
use super::{EnqueueOptions, JobQueue, QueueError, QueueReplicationOps};

const SCHEDULES: TableDefinition<&str, &[u8]> = TableDefinition::new("queue_schedules");

/// Redb table name for recurring schedules (shared with [`crate::schedule_source`]).
pub(crate) const SCHEDULES_TABLE: TableDefinition<&str, &[u8]> = SCHEDULES;

fn backend(e: impl std::fmt::Display) -> QueueError {
    QueueError::Backend(e.to_string())
}

fn codec(e: impl std::fmt::Display) -> QueueError {
    QueueError::Codec(e.to_string())
}

/// User-facing recurring job registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecurringJob {
    /// Unique name within the queue stream.
    pub name: String,
    /// Cron expression (5-field or 6-field; see [`parse_cron`]).
    ///
    /// Leave empty when using [`every_days`](Self::every_days) calendar interval mode.
    pub cron: String,
    /// Fire every N calendar days from [`anchor_ms`](Self::anchor_ms) (`0` = cron mode).
    pub every_days: u32,
    /// First fire instant (unix ms) when `every_days > 0`.
    pub anchor_ms: u64,
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
            every_days: 0,
            anchor_ms: 0,
        }
    }

    /// Recurring job every `every_days` **calendar** days from a fixed anchor instant.
    ///
    /// Use this instead of cron `*/N` on day-of-month (which is not “every N days”).
    #[must_use]
    pub fn every_calendar_days(
        name: impl Into<String>,
        every_days: u32,
        anchor_ms: u64,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            name: name.into(),
            cron: String::new(),
            every_days,
            anchor_ms,
            payload: payload.into(),
            priority: 0,
            max_attempts: 0,
            enabled: true,
        }
    }

    /// First fire at `hour:minute` UTC (on or after now), then every `every_days` calendar days.
    ///
    /// # Errors
    /// Returns [`QueueError::Codec`] when `hour`/`minute` are invalid.
    pub fn every_calendar_days_at_utc(
        name: impl Into<String>,
        every_days: u32,
        hour: u32,
        minute: u32,
        payload: impl Into<Vec<u8>>,
    ) -> Result<Self, QueueError> {
        let anchor_ms = anchor_utc_wall_clock(hour, minute, now_ms())?;
        Ok(Self::every_calendar_days(
            name, every_days, anchor_ms, payload,
        ))
    }

    /// Cron tick enqueues a [`WorkTrigger::Workflow`] bootstrap payload.
    #[must_use]
    pub fn trigger_workflow(
        name: impl Into<String>,
        cron: impl Into<String>,
        saga_id: impl Into<String>,
    ) -> Self {
        let payload = WorkTrigger::Workflow {
            saga_id: saga_id.into(),
        }
        .to_payload();
        Self::new(name, cron, payload)
    }

    /// Cron tick enqueues a [`WorkTrigger::Enqueue`] bootstrap payload.
    #[must_use]
    pub fn trigger_enqueue(
        name: impl Into<String>,
        cron: impl Into<String>,
        stream: impl Into<String>,
        follow_up_payload: impl Into<Vec<u8>>,
    ) -> Self {
        let payload = WorkTrigger::Enqueue {
            stream: stream.into(),
            payload: follow_up_payload.into(),
        }
        .to_payload();
        Self::new(name, cron, payload)
    }

    /// Calendar interval schedule that starts a workflow on each tick.
    ///
    /// # Errors
    /// Returns [`QueueError::Codec`] when `hour`/`minute` are invalid.
    pub fn trigger_workflow_every_calendar_days_at_utc(
        name: impl Into<String>,
        every_days: u32,
        hour: u32,
        minute: u32,
        saga_id: impl Into<String>,
    ) -> Result<Self, QueueError> {
        let payload = WorkTrigger::Workflow {
            saga_id: saga_id.into(),
        }
        .to_payload();
        let anchor_ms = anchor_utc_wall_clock(hour, minute, now_ms())?;
        Ok(Self::every_calendar_days(
            name, every_days, anchor_ms, payload,
        ))
    }

    /// Set enqueue priority for each tick.
    #[must_use]
    pub fn priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Cap retries for jobs produced by this schedule (`0` = inherit the stream default).
    #[must_use]
    pub fn max_attempts(mut self, max: u32) -> Self {
        self.max_attempts = max;
        self
    }

    fn to_wire(&self, next_run_ms: u64) -> RecurringScheduleWire {
        RecurringScheduleWire {
            name: self.name.clone(),
            cron: self.cron.clone(),
            every_days: self.every_days,
            anchor_ms: self.anchor_ms,
            payload: self.payload.clone(),
            priority: JobPriority(self.priority),
            max_attempts: MaxAttempts(self.max_attempts),
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
        n => Err(codec(format!(
            "cron expression needs 5 or 6 fields, got {n}"
        ))),
    }
}

/// Next fire strictly after `after_ms` for a calendar-day interval from `anchor_ms`.
///
/// # Errors
/// Returns [`QueueError::Codec`] when timestamps or `every_days` are invalid.
pub fn next_calendar_interval_run_after(
    anchor_ms: u64,
    every_days: u32,
    after_ms: u64,
) -> Result<u64, QueueError> {
    if every_days == 0 {
        return Err(codec(
            "every_days must be > 0 for calendar interval schedules",
        ));
    }
    let step = ChronoDuration::days(i64::from(every_days));
    let anchor_ms_i64 = i64::try_from(anchor_ms).unwrap_or(i64::MAX);
    let after_ms_i64 = i64::try_from(after_ms).unwrap_or(i64::MAX);
    let anchor = Utc
        .timestamp_millis_opt(anchor_ms_i64)
        .single()
        .ok_or_else(|| codec("invalid anchor_ms"))?;
    let after = Utc
        .timestamp_millis_opt(after_ms_i64)
        .single()
        .ok_or_else(|| codec("invalid timestamp"))?;
    let mut next = anchor;
    while next <= after {
        next = next
            .checked_add_signed(step)
            .ok_or_else(|| codec("calendar interval overflow"))?;
    }
    Ok(u64::try_from(next.timestamp_millis().max(0)).unwrap_or(u64::MAX))
}

/// Next UTC instant at `hour:minute` on or strictly after `after_ms`.
///
/// # Errors
/// Returns [`QueueError::Codec`] when `hour`/`minute` or `after_ms` are invalid.
pub fn anchor_utc_wall_clock(hour: u32, minute: u32, after_ms: u64) -> Result<u64, QueueError> {
    let time =
        NaiveTime::from_hms_opt(hour, minute, 0).ok_or_else(|| codec("invalid hour or minute"))?;
    let after_ms_i64 = i64::try_from(after_ms).unwrap_or(i64::MAX);
    let after = Utc
        .timestamp_millis_opt(after_ms_i64)
        .single()
        .ok_or_else(|| codec("invalid timestamp"))?;
    let date = after.date_naive();
    let mut candidate = date.and_time(time).and_utc();
    if candidate.timestamp_millis() <= after_ms_i64 {
        candidate = (date + chrono::Days::new(1)).and_time(time).and_utc();
    }
    Ok(u64::try_from(candidate.timestamp_millis().max(0)).unwrap_or(u64::MAX))
}

fn validate_recurring_job(job: &RecurringJob) -> Result<(), QueueError> {
    if job.every_days > 0 {
        if job.anchor_ms == 0 {
            return Err(codec("anchor_ms required when every_days is set"));
        }
        if !job.cron.is_empty() {
            return Err(codec("set either cron or every_days, not both"));
        }
        return Ok(());
    }
    if job.cron.is_empty() {
        return Err(codec("cron required when every_days is not set"));
    }
    Ok(())
}

fn next_run_for_job(job: &RecurringJob, after_ms: u64) -> Result<u64, QueueError> {
    validate_recurring_job(job)?;
    if job.every_days > 0 {
        return next_calendar_interval_run_after(job.anchor_ms, job.every_days, after_ms);
    }
    let schedule = parse_cron(&job.cron)?;
    next_run_after(&schedule, after_ms)
}

fn next_run_for_wire(wire: &RecurringScheduleWire, after_ms: u64) -> Result<u64, QueueError> {
    if wire.every_days > 0 {
        if wire.anchor_ms == 0 {
            return Err(codec("anchor_ms required when every_days is set"));
        }
        return next_calendar_interval_run_after(wire.anchor_ms, wire.every_days, after_ms);
    }
    let schedule = parse_cron(&wire.cron)?;
    next_run_after(&schedule, after_ms)
}

fn next_run_after(schedule: &Schedule, after_ms: u64) -> Result<u64, QueueError> {
    let after_ms_i64 = i64::try_from(after_ms).unwrap_or(i64::MAX);
    let after = Utc
        .timestamp_millis_opt(after_ms_i64.saturating_add(1))
        .single()
        .ok_or_else(|| codec("invalid timestamp"))?;
    let next = schedule
        .after(&after)
        .next()
        .ok_or_else(|| codec("cron schedule has no upcoming fire time"))?;
    Ok(u64::try_from(next.timestamp_millis().max(0)).unwrap_or(u64::MAX))
}

impl RedbJobQueue {
    /// Upsert a recurring schedule into the queue redb file.
    ///
    /// # Errors
    /// Returns [`QueueError::Backend`] or [`QueueError::Codec`] on failure.
    ///
    /// # Panics
    /// If the redb mutex is poisoned.
    pub fn upsert_schedule(&self, job: &RecurringJob) -> Result<QueueReplicationOps, QueueError> {
        let next_run_ms = next_run_for_job(job, now_ms())?;
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
    ///
    /// # Panics
    /// If the redb mutex is poisoned.
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
            let dedup_key = format!("recurring:{}", schedule.name).into_bytes();
            let (_, enqueue_ops) = self
                .enqueue_opts_replicated(
                    &schedule.payload,
                    EnqueueOptions {
                        priority: schedule.priority,
                        dedup_key: Some(dedup_key),
                        // Schedules persist the ceiling as a plain `u32`, so `0`
                        // (never configured) inherits the stream default rather
                        // than forcing unlimited retries.
                        max_attempts: (schedule.max_attempts != MaxAttempts(0))
                            .then_some(schedule.max_attempts),
                        ..EnqueueOptions::default()
                    },
                )
                .await?;
            ops.extend(enqueue_ops);

            schedule.next_run_ms = next_run_for_wire(&schedule, now)?;
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

/// Leader-only loop: reconcile [`crate::ScheduleSource`]s and fire due cron schedules.
pub async fn run_queue_schedule_ticker(
    service: Arc<crate::QueueService>,
    poll_interval: Duration,
    stop: tokio::sync::watch::Receiver<bool>,
) {
    let state = service.cluster_state();
    let service_tick = Arc::clone(&service);
    trembita_runtime::run_leader_loop(
        state,
        trembita_runtime::LeaderLoopOpts::new(poll_interval),
        stop,
        move |_| {
            let service = Arc::clone(&service_tick);
            async move {
                let _ = service.poll_schedule_sources().await;
                let _ = service.tick_schedules().await;
            }
        },
    )
    .await;
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

    #[test]
    fn calendar_interval_advances_by_days() {
        let anchor = Utc.with_ymd_and_hms(2024, 1, 1, 4, 0, 0).unwrap();
        let anchor_ms = u64::try_from(anchor.timestamp_millis()).unwrap();
        let jan4 = Utc.with_ymd_and_hms(2024, 1, 4, 4, 0, 0).unwrap();
        let jan7 = Utc.with_ymd_and_hms(2024, 1, 7, 4, 0, 0).unwrap();
        let next = next_calendar_interval_run_after(anchor_ms, 3, anchor_ms).unwrap();
        assert_eq!(next, u64::try_from(jan4.timestamp_millis()).unwrap());
        let next2 = next_calendar_interval_run_after(
            anchor_ms,
            3,
            u64::try_from(jan4.timestamp_millis()).unwrap(),
        )
        .unwrap();
        assert_eq!(next2, u64::try_from(jan7.timestamp_millis()).unwrap());
    }

    #[tokio::test]
    async fn calendar_interval_enqueues_and_advances() {
        let dir = tempfile::tempdir().unwrap();
        let queue = RedbJobQueue::open(dir.path().join("q.redb"), Duration::from_secs(30)).unwrap();
        let anchor = Utc.with_ymd_and_hms(2024, 1, 1, 4, 0, 0).unwrap();
        let anchor_ms = u64::try_from(anchor.timestamp_millis()).unwrap();
        let mut wire =
            RecurringJob::every_calendar_days("cleanup", 3, anchor_ms, b"tick").to_wire(0);
        wire.next_run_ms = anchor_ms;
        queue
            .apply_replicate(&QueueReplicateOp::UpsertSchedule { schedule: wire })
            .await
            .unwrap();

        let now_before = now_ms();
        let ops = queue.tick_schedules().await.unwrap();
        assert!(!ops.is_empty());
        assert!(queue.metrics().await.unwrap().pending >= 1);

        let wires = queue.list_schedule_wires().unwrap();
        let stored = wires.iter().find(|w| w.name == "cleanup").unwrap();
        let expected = next_calendar_interval_run_after(anchor_ms, 3, now_before).unwrap();
        assert_eq!(stored.next_run_ms, expected);
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
