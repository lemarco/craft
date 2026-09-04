//! Leader-gated queue wire service ([job-queue](../../../docs/decisions/job-queue.md)).
//!
//! Mutations run on the Raft leader and are **synchronously replicated** to every
//! other reachable voter before the client receives success — so a newly elected
//! leader serves the same backlog.

mod dispatch;
mod handlers;
mod lifecycle;
mod prefetch;
mod replication;
mod schedule;
mod wire;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use trembita_net::transport::Transport;
use trembita_proto::NodeId;
use trembita_runtime::ClusterState;

use crate::backlog_settle_outbox::BacklogSettleOutbox;
use crate::queue_lifecycle::QueueLifecycleEvent;
use crate::queue_prefetch::QueuePrefetchCache;
use crate::{JobQueue, RecurringJob, RedbJobQueue, ScheduleSource, ShardedJobQueue};

/// Serves `/raft/v1/queue/*` on the leader; followers transparently forward.
pub struct QueueService {
    pub(super) node_id: NodeId,
    pub(super) streams: Mutex<HashMap<String, Arc<dyn JobQueue>>>,
    pub(super) redb_streams: Mutex<HashMap<String, Arc<RedbJobQueue>>>,
    pub(super) prefetch: Mutex<HashMap<String, QueuePrefetchCache>>,
    pub(super) sharded: Mutex<HashMap<String, Arc<ShardedJobQueue>>>,
    pub(super) schedule_sources: Mutex<HashMap<String, schedule::ScheduleSourceEntry>>,
    pub(super) state: Arc<dyn ClusterState>,
    pub(super) transport: Arc<dyn Transport>,
    pub(super) lifecycle_hook: Option<Arc<dyn Fn(QueueLifecycleEvent) + Send + Sync>>,
    pub(super) backlog_settle_outbox: Option<Arc<dyn BacklogSettleOutbox>>,
}

impl QueueService {
    /// Empty service; register streams before accepting traffic.
    #[must_use]
    pub fn new(
        node_id: NodeId,
        state: Arc<dyn ClusterState>,
        transport: Arc<dyn Transport>,
    ) -> Self {
        Self {
            node_id,
            streams: Mutex::new(HashMap::new()),
            redb_streams: Mutex::new(HashMap::new()),
            prefetch: Mutex::new(HashMap::new()),
            sharded: Mutex::new(HashMap::new()),
            schedule_sources: Mutex::new(HashMap::new()),
            state,
            transport,
            lifecycle_hook: None,
            backlog_settle_outbox: None,
        }
    }

    /// Persist terminal jobs with dedup keys to the settle outbox ([`crate::run_backlog_settle_drainer`]).
    #[must_use]
    pub fn with_backlog_settle_outbox(mut self, outbox: Arc<dyn BacklogSettleOutbox>) -> Self {
        self.backlog_settle_outbox = Some(outbox);
        self
    }

    /// Emit [`QueueLifecycleEvent`]s to the dashboard / user sinks (observability).
    #[must_use]
    pub fn with_lifecycle_hook(
        mut self,
        hook: Arc<dyn Fn(QueueLifecycleEvent) + Send + Sync>,
    ) -> Self {
        self.lifecycle_hook = Some(hook);
        self
    }

    /// Register a local redb-backed stream and optional prefetch depth.
    ///
    /// Recurring schedules are loaded via [`Self::register_schedule_source`].
    ///
    /// `prefetch` controls the leader in-memory cache for recently enqueued jobs
    /// (`0` disables prefetch).
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub fn register_redb_stream(
        &self,
        name: impl Into<String>,
        queue: &Arc<RedbJobQueue>,
        prefetch: usize,
    ) {
        let name = name.into();
        self.streams
            .lock()
            .expect("poisoned")
            .insert(name.clone(), Arc::clone(queue) as Arc<dyn JobQueue>);
        self.redb_streams
            .lock()
            .expect("poisoned")
            .insert(name.clone(), Arc::clone(queue));
        if prefetch > 0 {
            self.prefetch
                .lock()
                .expect("poisoned")
                .insert(name, QueuePrefetchCache::new(prefetch));
        }
    }

    /// Register a federated sharded stream (logical name → local [`ShardedJobQueue`]).
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub fn register_sharded_stream(&self, name: impl Into<String>, queue: Arc<ShardedJobQueue>) {
        self.sharded
            .lock()
            .expect("poisoned")
            .insert(name.into(), queue);
    }

    /// Register a local backing queue for `stream` (opened on every node; kept
    /// in sync via leader replication).
    ///
    /// # Panics
    /// If an internal mutex is poisoned.
    pub fn register_stream(&self, stream: impl Into<String>, queue: Arc<dyn JobQueue>) {
        self.streams
            .lock()
            .expect("poisoned")
            .insert(stream.into(), queue);
    }
}
