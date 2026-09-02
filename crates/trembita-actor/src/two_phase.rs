//! In-memory prepare store for optional cross-shard 2PC on each Raft group leader.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Why a prepare could not be recorded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PrepareError {
    #[error("prepare already exists for this transaction key with a different command")]
    Conflict,
}

type PrepareKey = (Vec<u8>, Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq)]
struct PrepareEntry {
    command: Vec<u8>,
    prepared_at_tick: u64,
}

/// Leader-only staging area for 2PC prepare commands.
#[derive(Debug, Default)]
pub struct PrepareStore {
    entries: HashMap<PrepareKey, PrepareEntry>,
}

/// Wall-clock millis for durable prepare log metadata.
#[must_use]
pub fn unix_now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

impl PrepareStore {
    /// Stage `command` for `(tx_id, key)`. Idempotent when the same command is replayed.
    pub fn prepare(
        &mut self,
        tx_id: Vec<u8>,
        key: Vec<u8>,
        command: Vec<u8>,
        prepared_at_tick: u64,
    ) -> Result<(), PrepareError> {
        let entry_key = (tx_id, key);
        if let Some(existing) = self.entries.get(&entry_key)
            && existing.command != command
        {
            return Err(PrepareError::Conflict);
        }
        self.entries.insert(
            entry_key,
            PrepareEntry {
                command,
                prepared_at_tick,
            },
        );
        Ok(())
    }

    /// Borrow a staged command without removing it.
    #[must_use]
    pub fn get(&self, tx_id: &[u8], key: &[u8]) -> Option<&Vec<u8>> {
        self.entries
            .get(&(tx_id.to_vec(), key.to_vec()))
            .map(|e| &e.command)
    }

    /// `(tx_id, route_key)` pairs staged longer than `timeout_ticks` logical ticks.
    #[must_use]
    pub fn expired_ticks(&self, now_tick: u64, timeout_ticks: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        if timeout_ticks == 0 {
            return Vec::new();
        }
        self.entries
            .iter()
            .filter(|(_, entry)| now_tick.saturating_sub(entry.prepared_at_tick) >= timeout_ticks)
            .map(|((tx_id, key), _)| (tx_id.clone(), key.clone()))
            .collect()
    }

    /// Drop a staged command if present.
    pub fn abort(&mut self, tx_id: &[u8], key: &[u8]) -> bool {
        self.entries
            .remove(&(tx_id.to_vec(), key.to_vec()))
            .is_some()
    }

    /// Clear all staged prepares (leadership loss on ephemeral 2PC).
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expired_ticks_selects_stale_prepares_only() {
        let mut store = PrepareStore::default();
        store
            .prepare(b"tx1".to_vec(), b"a".to_vec(), vec![1], 1)
            .unwrap();
        store
            .prepare(b"tx2".to_vec(), b"b".to_vec(), vec![2], 8)
            .unwrap();
        let expired = store.expired_ticks(10, 5);
        assert_eq!(expired, vec![(b"tx1".to_vec(), b"a".to_vec())]);
    }
}
