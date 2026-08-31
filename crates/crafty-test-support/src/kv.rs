//! Reference key/value [`StateMachine`](crafty_core::StateMachine) types shared across tests.
//!
//! Canonical implementation: [`crafty_core::kv`](crafty_core::kv). This module re-exports it
//! and adds test-only helpers (`TrackedKv`, short type aliases).

pub use crafty_core::kv::{Kv, KvCommand, KvError, KvMachine, KvQuery, KvResponse};

use std::collections::BTreeMap;

use crafty_core::StateMachine;
use crafty_proto::LogIndex;
use serde::{Deserialize, Serialize};

/// Shorter command alias used by facade/quic/client tests.
pub type Cmd = KvCommand;
/// Shorter query alias used by facade/quic/client tests.
pub type Qry = KvQuery;
/// Shorter response alias used by facade/quic/client tests.
pub type Resp = KvResponse;

/// Like [`Kv`], but records the highest applied log index and asserts commands
/// apply in strictly ascending order (for driver-level persistence tests).
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TrackedKv {
    map: BTreeMap<String, String>,
    /// Highest index passed to [`StateMachine::apply`].
    #[doc(hidden)]
    pub applied_through: u64,
}

impl TrackedKv {
    /// Highest log index applied so far.
    #[must_use]
    pub fn applied_through(&self) -> u64 {
        self.applied_through
    }
}

impl StateMachine for TrackedKv {
    type Command = KvCommand;
    type Query = KvQuery;
    type Response = KvResponse;
    type Error = KvError;

    fn apply(&mut self, index: LogIndex, command: &KvCommand) -> Result<KvResponse, KvError> {
        assert!(
            index.0 > self.applied_through,
            "commands must apply in strictly ascending index order exactly once \
             (index {} <= applied_through {})",
            index.0,
            self.applied_through
        );
        self.applied_through = index.0;
        Ok(match command {
            KvCommand::Set { key, value } => {
                let previous = self.map.insert(key.clone(), value.clone());
                KvResponse::Set { previous }
            }
            KvCommand::Delete { key } => {
                let existed = self.map.remove(key).is_some();
                KvResponse::Deleted { existed }
            }
        })
    }

    fn query(&self, query: &KvQuery) -> Result<KvResponse, KvError> {
        Ok(match query {
            KvQuery::Get { key } => KvResponse::Value(self.map.get(key).cloned()),
            KvQuery::Len => KvResponse::Len(self.map.len() as u64),
        })
    }

    fn snapshot(&self) -> Result<Vec<u8>, KvError> {
        crafty_proto::encode(self).map_err(|_| KvError)
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), KvError> {
        *self = crafty_proto::decode(snapshot).map_err(|_| KvError)?;
        Ok(())
    }
}
