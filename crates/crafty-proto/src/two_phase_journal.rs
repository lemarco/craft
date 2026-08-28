//! Cross-shard 2PC client journal metadata replicated through Meta-Raft (Tier 2).

use serde::{Deserialize, Serialize};

/// Client-side 2PC journal upsert replicated via Meta-Raft (not the user state machine).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwoPhaseJournalCommand {
    /// Transaction identifier (journal key).
    pub tx_id: Vec<u8>,
    /// Postcard-encoded 2PC journal record bytes (see `crafty_client::TwoPhaseJournalRecord`).
    pub record: Vec<u8>,
}
