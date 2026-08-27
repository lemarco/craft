//! In-memory prepare store for optional cross-shard 2PC on each Raft group leader.

use std::collections::HashMap;

/// Why a prepare could not be recorded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PrepareError {
    #[error("prepare already exists for this transaction key with a different command")]
    Conflict,
}

type PrepareKey = (Vec<u8>, Vec<u8>);

/// Leader-only staging area for 2PC prepare commands.
#[derive(Debug, Default)]
pub struct PrepareStore {
    entries: HashMap<PrepareKey, Vec<u8>>,
}

impl PrepareStore {
    /// Stage `command` for `(tx_id, key)`. Idempotent when the same command is replayed.
    pub fn prepare(
        &mut self,
        tx_id: Vec<u8>,
        key: Vec<u8>,
        command: Vec<u8>,
    ) -> Result<(), PrepareError> {
        let entry_key = (tx_id, key);
        if let Some(existing) = self.entries.get(&entry_key)
            && existing != &command
        {
            return Err(PrepareError::Conflict);
        }
        self.entries.insert(entry_key, command);
        Ok(())
    }

    /// Remove and return a staged command.
    pub fn take(&mut self, tx_id: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        self.entries.remove(&(tx_id.to_vec(), key.to_vec()))
    }

    /// Drop a staged command if present.
    pub fn abort(&mut self, tx_id: &[u8], key: &[u8]) -> bool {
        self.entries
            .remove(&(tx_id.to_vec(), key.to_vec()))
            .is_some()
    }

    /// Clear all staged prepares (leadership loss).
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}
