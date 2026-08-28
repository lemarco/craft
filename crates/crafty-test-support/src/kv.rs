//! Reference key/value [`StateMachine`] types shared across integration tests.

use std::collections::BTreeMap;

use crafty_core::StateMachine;
use crafty_proto::{self, LogIndex};
use serde::{Deserialize, Serialize};

/// Write side of the reference KV machine.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum KvCommand {
    /// Insert or overwrite a key.
    Set { key: String, value: String },
    /// Remove a key.
    Delete { key: String },
}

/// Read side of the reference KV machine.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum KvQuery {
    /// Look up a key.
    Get { key: String },
    /// Number of keys stored.
    Len,
}

/// Responses from apply/query on the reference KV machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvResponse {
    /// A successful write, with the prior value if any.
    Set { previous: Option<String> },
    /// Result of a delete.
    Deleted { existed: bool },
    /// Result of a get (missing keys are `None`).
    Value(Option<String>),
    /// Key count.
    Len(u64),
}

/// Errors from the reference KV machine (all operations succeed today).
#[derive(Debug, thiserror::Error)]
#[error("kv error")]
pub struct KvError;

/// In-memory key/value store wired into Raft as a user state machine.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Kv {
    map: BTreeMap<String, String>,
}

/// Like [`Kv`], but records the highest applied log index and asserts commands
/// apply in strictly ascending order (for driver-level persistence tests).
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TrackedKv {
    map: BTreeMap<String, String>,
    /// Highest index passed to [`StateMachine::apply`].
    #[doc(hidden)]
    pub applied_through: u64,
}

/// Shorter names used by facade/quic/client tests.
pub type Cmd = KvCommand;
pub type Qry = KvQuery;
pub type Resp = KvResponse;

/// Alias used by multi-Raft / persistence tests.
pub type KvMachine = Kv;

impl TrackedKv {
    /// Highest log index applied so far.
    #[must_use]
    pub fn applied_through(&self) -> u64 {
        self.applied_through
    }
}

impl StateMachine for Kv {
    type Command = KvCommand;
    type Query = KvQuery;
    type Response = KvResponse;
    type Error = KvError;

    fn apply(&mut self, _index: LogIndex, command: &KvCommand) -> Result<KvResponse, KvError> {
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
