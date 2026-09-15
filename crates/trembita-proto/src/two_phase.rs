//! Cross-shard two-phase commit metadata replicated through each Raft group log.

use serde::{Deserialize, Serialize};

use crate::{RouteKey, TransactionId, UnixMillis};

/// Prepare staging entry (durable 2PC; not applied to the user state machine).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwoPhasePrepareCommand {
    /// Opaque transaction id shared across prepare/commit/abort calls.
    pub tx_id: TransactionId,
    /// Shard routing key for this prepare step.
    pub route_key: RouteKey,
    /// Application-encoded command staged at prepare time.
    pub command: Vec<u8>,
    /// Unix epoch millis when the prepare was first staged (timeout GC).
    #[serde(default)]
    pub prepared_at_ms: UnixMillis,
}

/// Abort/tombstone for a staged prepare (durable 2PC; not applied to the user SM).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwoPhaseAbortCommand {
    /// Opaque transaction id.
    pub tx_id: TransactionId,
    /// Shard routing key for this prepare step.
    pub route_key: RouteKey,
}
