//! Cross-shard saga journal metadata replicated through group 0 (Meta-Raft saga journal).

use serde::{Deserialize, Serialize};

/// Saga journal upsert replicated via group 0 Raft (not the user state machine).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SagaJournalCommand {
    /// Saga identifier (journal key).
    pub saga_id: Vec<u8>,
    /// Postcard-encoded saga journal record bytes (see `trembita_client::SagaJournalRecord`).
    pub record: Vec<u8>,
}
