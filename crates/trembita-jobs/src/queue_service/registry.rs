//! In-memory queue stream tables guarded by a single lock.

use std::collections::HashMap;
use std::sync::Arc;

use crate::queue_prefetch::QueuePrefetchCache;
use crate::{JobQueue, RedbJobQueue, ShardedJobQueue};

use super::schedule::ScheduleSourceEntry;

/// Leader-local queue stream registry (streams, prefetch, schedules).
pub(super) struct QueueStreamRegistry {
    pub streams: HashMap<String, Arc<dyn JobQueue>>,
    pub redb_streams: HashMap<String, Arc<RedbJobQueue>>,
    pub prefetch: HashMap<String, QueuePrefetchCache>,
    pub sharded: HashMap<String, Arc<ShardedJobQueue>>,
    pub schedule_sources: HashMap<String, ScheduleSourceEntry>,
}

impl QueueStreamRegistry {
    pub(super) fn new() -> Self {
        Self {
            streams: HashMap::new(),
            redb_streams: HashMap::new(),
            prefetch: HashMap::new(),
            sharded: HashMap::new(),
            schedule_sources: HashMap::new(),
        }
    }
}
