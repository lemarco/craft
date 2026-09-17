//! Lazy physical shard open and leader-side expansion.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::{JobQueue, QueueError, RedbJobQueue};

use super::QueueService;

impl QueueService {
    pub(super) fn resolve_local_stream(
        &self,
        stream: &str,
    ) -> Result<Arc<dyn JobQueue>, trembita_proto::ProductWireError> {
        match self.local_stream(stream) {
            Ok(queue) => Ok(queue),
            Err(e) => self.try_lazy_open_physical_stream(stream).ok_or(e),
        }
    }

    fn try_lazy_open_physical_stream(&self, stream: &str) -> Option<Arc<dyn JobQueue>> {
        let spec = {
            let specs = self.auto_shard_specs.lock().expect("poisoned");
            specs.iter().find_map(|spec| {
                let shard_index = parse_sharded_physical(stream, &spec.logical)?;
                (shard_index < spec.policy.max_shards).then(|| spec.clone())
            })?
        };
        let path = queue_path(&spec.data_dir, stream);
        let local = Arc::new(
            RedbJobQueue::open(&path, spec.lease_timeout)
                .ok()?
                .default_max_attempts(spec.default_max_attempts),
        );
        self.register_redb_stream(stream, &local, spec.prefetch);
        Some(local as Arc<dyn JobQueue>)
    }

    /// Add the next physical shard for a logical auto-shard queue (leader-only).
    ///
    /// # Errors
    /// Returns [`QueueError`] when the logical stream is missing or already at `max_shards`.
    pub fn try_expand_sharded_stream(
        &self,
        logical: &str,
        data_dir: &Path,
        lease_timeout: Duration,
        prefetch: usize,
        default_max_attempts: u32,
        max_shards: usize,
    ) -> Result<(), QueueError> {
        let sharded =
            {
                let registry = self.registry.lock().expect("poisoned");
                registry.sharded.get(logical).cloned().ok_or_else(|| {
                    QueueError::Backend(format!("unknown sharded stream {logical:?}"))
                })?
            };
        let next = sharded.shard_count();
        if next >= max_shards {
            return Err(QueueError::Backend(format!(
                "auto-shard max_shards reached for {logical:?}"
            )));
        }
        let physical = format!("{logical}~{next}");
        let path = queue_path(data_dir, &physical);
        let local = Arc::new(
            RedbJobQueue::open(&path, lease_timeout)
                .map_err(|e| QueueError::Backend(e.to_string()))?
                .default_max_attempts(default_max_attempts),
        );
        self.register_redb_stream(&physical, &local, prefetch);
        sharded.add_shard(Arc::clone(&local) as Arc<dyn JobQueue>);
        Ok(())
    }
}

fn queue_path(data_dir: &Path, stream: &str) -> PathBuf {
    data_dir.join(format!("queue-{stream}.redb"))
}

fn parse_sharded_physical(stream: &str, logical: &str) -> Option<usize> {
    let suffix = stream.strip_prefix(logical)?.strip_prefix('~')?;
    suffix.parse().ok()
}
