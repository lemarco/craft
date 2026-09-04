//! Dynamic schedule sources and recurring job ticks on the queue leader.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::{RecurringJob, RedbJobQueue, ScheduleSource};
use trembita_runtime::ClusterState;

use super::QueueService;

pub(super) struct ScheduleSourceEntry {
    source: Arc<dyn ScheduleSource>,
    poll: Duration,
    last_good: Option<Vec<RecurringJob>>,
    next_poll_at: Option<Instant>,
}

impl QueueService {
    /// Poll a dynamic [`ScheduleSource`] on the leader and reconcile redb + voters.
    ///
    /// Source errors and bootstrap `Ok([])` never clear persisted schedules.
    ///
    /// # Errors
    /// Propagates queue or replication failures as strings.
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub async fn poll_schedule_sources(&self) -> Result<(), String> {
        if !self.state.is_leader() {
            return Ok(());
        }
        let now = Instant::now();
        let due: Vec<String> = self
            .registry
            .lock()
            .expect("poisoned")
            .schedule_sources
            .iter()
            .filter(|(_, entry)| entry.next_poll_at.is_none_or(|at| now >= at))
            .map(|(stream, _)| stream.clone())
            .collect();
        for stream in due {
            self.poll_schedule_source(&stream).await?;
        }
        Ok(())
    }

    pub(super) async fn poll_schedule_source(&self, stream: &str) -> Result<(), String> {
        let source = {
            self.registry
                .lock()
                .expect("poisoned")
                .schedule_sources
                .get(stream)
                .map(|entry| Arc::clone(&entry.source))
        };
        let Some(source) = source else {
            return Ok(());
        };

        let desired = match source.schedules().await {
            Err(e) => {
                eprintln!("trembita: schedule source {stream:?}: {e}");
                return Ok(());
            }
            Ok(jobs) => jobs,
        };

        let apply = {
            let mut registry = self.registry.lock().expect("poisoned");
            let Some(entry) = registry.schedule_sources.get_mut(stream) else {
                return Ok(());
            };
            entry.next_poll_at = Some(Instant::now() + entry.poll);
            if desired.is_empty() && entry.last_good.is_none() {
                false
            } else {
                entry.last_good = Some(desired.clone());
                true
            }
        };

        if apply {
            self.reconcile_schedules(stream, &desired).await?;
        }
        Ok(())
    }

    /// Diff `desired` against live redb schedules and replicate mutations.
    ///
    /// # Errors
    /// Propagates queue or replication failures as strings.
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub async fn reconcile_schedules(
        &self,
        stream: &str,
        desired: &[RecurringJob],
    ) -> Result<(), String> {
        if !self.state.is_leader() {
            return Ok(());
        }
        let queue = self
            .registry
            .lock()
            .expect("poisoned")
            .redb_streams
            .get(stream)
            .cloned()
            .ok_or_else(|| format!("unknown queue stream {stream:?}"))?;
        let ops = queue
            .reconcile_schedules(desired)
            .map_err(|e| e.to_string())?;
        if !ops.is_empty() {
            self.replicate_ops(stream, &ops).await?;
        }
        Ok(())
    }

    /// Register a [`ScheduleSource`] polled on the queue leader.
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub fn register_schedule_source(
        &self,
        stream: impl Into<String>,
        source: Arc<dyn ScheduleSource>,
        poll: Duration,
    ) {
        let stream = stream.into();
        self.registry
            .lock()
            .expect("poisoned")
            .schedule_sources
            .insert(
                stream,
                ScheduleSourceEntry {
                    source,
                    poll,
                    last_good: None,
                    next_poll_at: None,
                },
            );
    }

    /// Whether any [`ScheduleSource`] is registered.
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub fn has_schedule_sources(&self) -> bool {
        !self
            .registry
            .lock()
            .expect("poisoned")
            .schedule_sources
            .is_empty()
    }

    /// Shared cluster facts for leader-only background loops.
    #[must_use]
    pub fn cluster_state(&self) -> Arc<dyn ClusterState> {
        Arc::clone(&self.state)
    }

    /// Fire due recurring schedules on the leader and replicate mutations.
    ///
    /// # Errors
    /// Propagates queue or replication failures as strings.
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub async fn tick_schedules(&self) -> Result<(), String> {
        if !self.state.is_leader() {
            return Ok(());
        }
        let backends: Vec<(String, Arc<RedbJobQueue>)> = self
            .registry
            .lock()
            .expect("poisoned")
            .redb_streams
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        for (stream, queue) in backends {
            let ops = queue.tick_schedules().await.map_err(|e| e.to_string())?;
            if !ops.is_empty() {
                self.replicate_ops(&stream, &ops).await?;
            }
        }
        Ok(())
    }
}
