//! Reference key/value [`StateMachine`] for tutorials, tests, and showcases.
//!
//! A minimal tier **A** machine: in-memory map, snapshot via [`proto::encode`](crate::proto::encode).
//! Use with any crafty cluster builder when you need a working SM without writing your own.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::proto::LogIndex;
use crate::state_machine::StateMachine;

/// Write side of the reference KV machine.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum KvCommand {
    /// Insert or overwrite a key.
    Set {
        /// Key to insert or overwrite.
        key: String,
        /// Value to store.
        value: String,
    },
    /// Remove a key.
    Delete {
        /// Key to remove.
        key: String,
    },
}

/// Read side of the reference KV machine.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum KvQuery {
    /// Look up a key.
    Get {
        /// Key to look up.
        key: String,
    },
    /// Number of keys stored.
    Len,
}

/// Responses from apply/query on the reference KV machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvResponse {
    /// A successful write, with the prior value if any.
    Set {
        /// Prior value for the key, if any.
        previous: Option<String>,
    },
    /// Result of a delete.
    Deleted {
        /// Whether the key existed before removal.
        existed: bool,
    },
    /// Result of a get (missing keys are `None`).
    Value(Option<String>),
    /// Key count.
    Len(u64),
}

/// Errors from the reference KV machine (all operations succeed today).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KvError;

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("kv error")
    }
}

impl std::error::Error for KvError {}

/// In-memory key/value store wired into Raft as a user state machine.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Kv {
    map: BTreeMap<String, String>,
}

/// Shorter alias used in docs and integration tests.
pub type KvMachine = Kv;

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
        crate::proto::encode(self).map_err(|_| KvError)
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), KvError> {
        *self = crate::proto::decode(snapshot).map_err(|_| KvError)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_get_and_snapshot_round_trip() {
        let mut kv = Kv::default();
        assert_eq!(
            kv.apply(
                LogIndex(1),
                &KvCommand::Set {
                    key: "a".into(),
                    value: "1".into(),
                },
            )
            .unwrap(),
            KvResponse::Set { previous: None }
        );
        assert_eq!(
            kv.query(&KvQuery::Get { key: "a".into() }).unwrap(),
            KvResponse::Value(Some("1".into()))
        );

        let snap = kv.snapshot().unwrap();
        let mut restored = Kv::default();
        restored.restore(&snap).unwrap();
        assert_eq!(restored, kv);
    }
}
