//! Meta-Raft coordinator state machine — no user application state.

use crafty_core::StateMachine;
use crafty_proto::LogIndex;
use serde::{Deserialize, Serialize};

/// Opaque command for the Meta-Raft group (user commands are rejected at runtime).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaCommand;

/// Opaque query for the Meta-Raft group.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaQuery;

/// Empty response from the Meta-Raft state machine.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaResponse;

/// Errors from the Meta-Raft state machine (currently unused).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("meta raft state machine error")]
pub struct MetaError;

/// No-op application state for the Meta-Raft coordinator group.
///
/// Cluster metadata (membership, catalog, saga journal) is applied by the
/// runtime, not through this machine.
#[derive(Debug, Clone, Copy, Default)]
pub struct MetaStateMachine;

impl StateMachine for MetaStateMachine {
    type Command = MetaCommand;
    type Query = MetaQuery;
    type Response = MetaResponse;
    type Error = MetaError;

    fn apply(
        &mut self,
        _index: LogIndex,
        _command: &MetaCommand,
    ) -> Result<MetaResponse, MetaError> {
        Ok(MetaResponse)
    }

    fn query(&self, _query: &MetaQuery) -> Result<MetaResponse, MetaError> {
        Ok(MetaResponse)
    }

    fn snapshot(&self) -> Result<Vec<u8>, MetaError> {
        Ok(Vec::new())
    }

    fn restore(&mut self, _snapshot: &[u8]) -> Result<(), MetaError> {
        Ok(())
    }
}
