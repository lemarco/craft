//! Actor workflow store wire types ([actor-state-store](../../../docs/decisions/actor-state-store.md)).

use serde::{Deserialize, Serialize};

/// Set a workflow key on the leader (`POST /raft/v1/actor-store/set`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreSetRequest {
    /// UTF-8 key.
    pub key: String,
    /// Opaque value bytes.
    pub value: Vec<u8>,
    /// TTL in seconds (`0` = no expiry).
    #[serde(default)]
    pub ttl_secs: u64,
}

/// Response to [`StoreSetRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreSetReply {
    /// Set when the mutation failed.
    pub error: Option<String>,
}

/// Delete a workflow key on the leader (`POST /raft/v1/actor-store/delete`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreDeleteRequest {
    /// Key to remove.
    pub key: String,
}

/// Response to [`StoreDeleteRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreDeleteReply {
    /// Set when the mutation failed.
    pub error: Option<String>,
}

/// Compare-and-set on the leader (`POST /raft/v1/actor-store/compare-and-set`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreCompareAndSetRequest {
    /// Key to update.
    pub key: String,
    /// Expected current value (`None` = key must be absent).
    pub expected: Option<Vec<u8>>,
    /// New value when the precondition holds.
    pub value: Vec<u8>,
    /// TTL in seconds for the new value (`0` = no expiry).
    #[serde(default)]
    pub ttl_secs: u64,
}

/// Response to [`StoreCompareAndSetRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreCompareAndSetReply {
    /// Whether the swap happened.
    pub applied: bool,
    /// Set when the RPC failed (not when the precondition did not hold).
    pub error: Option<String>,
}

/// Idempotent mutation replicated from the store leader to every voter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoreReplicateOp {
    /// Upsert a key.
    Set {
        /// Key.
        key: String,
        /// Value bytes.
        value: Vec<u8>,
        /// Expiry unix ms (`0` = never).
        #[serde(default)]
        expires_at_ms: u64,
    },
    /// Remove a key (no-op when absent).
    Delete {
        /// Key.
        key: String,
    },
}

/// Batch of store replication ops from the leader (`POST /raft/v1/actor-store/replicate`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreReplicateRequest {
    /// Idempotent mutations to apply in order.
    pub ops: Vec<StoreReplicateOp>,
}

/// Response to [`StoreReplicateRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreReplicateReply {
    /// Set when replication apply failed.
    pub error: Option<String>,
}
