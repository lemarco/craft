//! `craft-proto` — wire types and [`postcard`] codec shared across all craft crates.
//!
//! Defines the on-the-wire representation for Raft peer RPCs, the client API,
//! cluster join handshakes, and actor messaging. All bodies are encoded with
//! `postcard` (wire-transport, serialization). Nothing here performs I/O.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub mod actor;
pub mod catalog;
pub mod client;
pub mod group;
pub mod group_migrate;
pub mod join;
pub mod leave;
pub mod raft;
pub mod saga_journal;
pub mod two_phase;
pub mod two_phase_journal;

pub use actor::{
    ActorEnvelope, ActorId, ActorRef, ActorRegistration, ActorTypeId, DeliverAck, DirectoryUpdate,
    MigrateReply, MigrateRequest, RegisterAck, ScaleReply, ScaleRequest, SpawnReply, SpawnRequest,
    StopReply, StopRequest,
};
pub use catalog::{CatalogAddRequest, CatalogAddResponse, CatalogCommand, CatalogRejection};
pub use client::{ClientRequest, ClientResponse};
pub use group::GroupPeerEnvelope;
pub use group_migrate::{
    GroupMigrateReply, GroupMigrateRequest, GroupMigrationBundle, GroupMigrationHardState,
    GroupMigrationSnapshot, GroupMigrationSnapshotMeta,
};
pub use join::{JoinRejection, JoinRequest, JoinResponse, PeerBook, PeerEntry};
pub use leave::{LeaveRejection, LeaveRequest, LeaveResponse};
pub use raft::{
    AppendEntries, AppendEntriesReply, EntryPayload, InstallSnapshot, InstallSnapshotReply,
    LogEntry, Membership, RaftRpc, RaftRpcReply, RequestVote, RequestVoteReply,
};
pub use saga_journal::SagaJournalCommand;
pub use two_phase::{TwoPhaseAbortCommand, TwoPhasePrepareCommand};
pub use two_phase_journal::TwoPhaseJournalCommand;

/// Wire/protocol version negotiated on join (join-version-skew: hard reject on mismatch).
pub const PROTOCOL_VERSION: u32 = 1;

/// Oldest wire protocol this release accepts during rolling upgrades (N/N−1).
pub const MIN_COMPATIBLE_PROTOCOL_VERSION: u32 = 1;

/// Whether `got` is in the supported compatibility band `[MIN..=PROTOCOL]`.
#[must_use]
pub fn protocol_version_compatible(got: u32) -> bool {
    got >= MIN_COMPATIBLE_PROTOCOL_VERSION && got <= PROTOCOL_VERSION
}

/// Stable identifier for a cluster node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct NodeId(pub u64);

/// Raft term (monotonic election epoch).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize,
)]
pub struct Term(pub u64);

/// 1-based index into the Raft log.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize,
)]
pub struct LogIndex(pub u64);

/// A leader replication/heartbeat round, used to confirm leadership for
/// linearizable ReadIndex reads (read-consistency). Monotonic per leader term.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize,
)]
pub struct Round(pub u64);

/// A position in the Raft log: the `(term, index)` pair that uniquely
/// identifies an entry. Ordering is lexicographic on `(term, index)`, which is
/// exactly Raft's log "up-to-date" comparison (§5.4.1), so `LogId` values can
/// be compared directly instead of juggling two primitives.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Serialize, Deserialize,
)]
pub struct LogId {
    /// Term of the entry at `index`.
    pub term: Term,
    /// Log index.
    pub index: LogIndex,
}

impl Term {
    /// Term zero (before any election).
    pub const ZERO: Term = Term(0);

    /// The next term.
    #[must_use]
    pub fn next(self) -> Term {
        Term(self.0 + 1)
    }
}

impl LogIndex {
    /// Index zero (empty log sentinel).
    pub const ZERO: LogIndex = LogIndex(0);

    /// The next index.
    #[must_use]
    pub fn next(self) -> LogIndex {
        LogIndex(self.0 + 1)
    }
}

impl Round {
    /// The zeroth round (no heartbeat sent yet).
    pub const ZERO: Round = Round(0);

    /// The next round.
    #[must_use]
    pub fn next(self) -> Round {
        Round(self.0 + 1)
    }
}

impl LogId {
    /// The empty-log sentinel `(term 0, index 0)`.
    pub const ZERO: LogId = LogId {
        term: Term::ZERO,
        index: LogIndex::ZERO,
    };

    /// Construct a [`LogId`] from a term and index.
    #[must_use]
    pub fn new(term: Term, index: LogIndex) -> Self {
        Self { term, index }
    }
}

/// The wire codec in effect for this build: `"postcard"` by default, or
/// `"json"` when the dev-only `json-wire` feature is enabled (future-work-and-risks item 4).
/// Surfaced so a node can log/advertise its wire format at startup.
pub const WIRE_CODEC: &str = if cfg!(feature = "json-wire") {
    "json"
} else {
    "postcard"
};

/// Errors from encoding/decoding wire bodies.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// Serialization failed.
    #[error("wire encode failed: {0}")]
    Encode(String),
    /// Deserialization failed.
    #[error("wire decode failed: {0}")]
    Decode(String),
}

/// Encode a value to a wire byte vector.
///
/// Uses the compact `postcard` binary format (wire-transport, serialization) unless the dev-only
/// `json-wire` feature is enabled, in which case bodies are human-readable JSON.
///
/// # Errors
/// Returns [`CodecError::Encode`] if serialization fails.
#[cfg(not(feature = "json-wire"))]
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    postcard::to_stdvec(value).map_err(|e| CodecError::Encode(e.to_string()))
}

/// Decode a value from a wire byte slice. See [`encode`] for the format.
///
/// # Errors
/// Returns [`CodecError::Decode`] if deserialization fails.
#[cfg(not(feature = "json-wire"))]
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    postcard::from_bytes(bytes).map_err(|e| CodecError::Decode(e.to_string()))
}

/// Encode a value as JSON (dev-only `json-wire` build). See [`WIRE_CODEC`].
///
/// # Errors
/// Returns [`CodecError::Encode`] if serialization fails.
#[cfg(feature = "json-wire")]
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    serde_json::to_vec(value).map_err(|e| CodecError::Encode(e.to_string()))
}

/// Decode a value from JSON (dev-only `json-wire` build). See [`WIRE_CODEC`].
///
/// # Errors
/// Returns [`CodecError::Decode`] if deserialization fails.
#[cfg(feature = "json-wire")]
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    serde_json::from_slice(bytes).map_err(|e| CodecError::Decode(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn term_and_index_advance() {
        assert_eq!(Term::ZERO.next(), Term(1));
        assert_eq!(LogIndex::ZERO.next(), LogIndex(1));
    }

    #[test]
    fn roundtrip_log_entry() {
        let entry = LogEntry {
            term: Term(3),
            index: LogIndex(7),
            payload: EntryPayload::Command(vec![1, 2, 3]),
        };
        let bytes = encode(&entry).expect("encode");
        let back: LogEntry = decode(&bytes).expect("decode");
        assert_eq!(entry, back);
    }

    #[test]
    fn roundtrip_saga_journal_entry() {
        let entry = LogEntry {
            term: Term(2),
            index: LogIndex(4),
            payload: EntryPayload::SagaJournal(SagaJournalCommand {
                saga_id: b"saga-1".to_vec(),
                record: vec![1, 2, 3],
            }),
        };
        let bytes = encode(&entry).expect("encode");
        let back: LogEntry = decode(&bytes).expect("decode");
        assert_eq!(entry, back);
    }

    #[test]
    fn roundtrip_two_phase_prepare_entry() {
        let entry = LogEntry {
            term: Term(2),
            index: LogIndex(5),
            payload: EntryPayload::TwoPhasePrepare(TwoPhasePrepareCommand {
                tx_id: b"tx".to_vec(),
                route_key: b"key".to_vec(),
                command: vec![1, 2],
                prepared_at_ms: 0,
            }),
        };
        let bytes = encode(&entry).expect("encode");
        let back: LogEntry = decode(&bytes).expect("decode");
        assert_eq!(entry, back);
    }

    #[test]
    fn roundtrip_raft_rpc() {
        let rpc = RaftRpc::AppendEntries(AppendEntries {
            term: Term(2),
            leader_id: NodeId(1),
            prev_log: LogId::new(Term(1), LogIndex(4)),
            entries: vec![LogEntry {
                term: Term(2),
                index: LogIndex(5),
                payload: EntryPayload::Noop,
            }],
            leader_commit: LogIndex(4),
            round: Round(7),
        });
        let bytes = encode(&rpc).expect("encode");
        let back: RaftRpc = decode(&bytes).expect("decode");
        assert_eq!(rpc, back);
    }

    #[test]
    fn log_id_orders_by_term_then_index() {
        // Up-to-dateness: higher term wins regardless of index; same term
        // compares by index (Raft §5.4.1).
        assert!(LogId::new(Term(2), LogIndex(1)) > LogId::new(Term(1), LogIndex(9)));
        assert!(LogId::new(Term(1), LogIndex(5)) > LogId::new(Term(1), LogIndex(4)));
        assert_eq!(
            LogEntry {
                term: Term(3),
                index: LogIndex(7),
                payload: EntryPayload::Noop
            }
            .id(),
            LogId::new(Term(3), LogIndex(7))
        );
    }

    #[test]
    fn decode_rejects_garbage() {
        let err = decode::<LogEntry>(&[0xff, 0xff, 0xff, 0xff]);
        assert!(err.is_err());
    }

    #[test]
    fn protocol_version_compatible_accepts_current_and_min() {
        assert!(protocol_version_compatible(PROTOCOL_VERSION));
        assert!(protocol_version_compatible(MIN_COMPATIBLE_PROTOCOL_VERSION));
        assert!(!protocol_version_compatible(0));
        assert!(!protocol_version_compatible(PROTOCOL_VERSION + 1));
    }

    #[test]
    fn wire_codec_matches_build_feature() {
        // The default build is postcard; `--features json-wire` flips it.
        if cfg!(feature = "json-wire") {
            assert_eq!(WIRE_CODEC, "json");
            // JSON is human-readable: the encoded RPC contains field names.
            let bytes = encode(&RequestVote {
                term: Term(1),
                candidate_id: NodeId(2),
                last_log: LogId::ZERO,
                pre_vote: true,
            })
            .unwrap();
            let text = String::from_utf8(bytes).unwrap();
            assert!(text.contains("candidate_id"), "json body: {text}");
        } else {
            assert_eq!(WIRE_CODEC, "postcard");
        }
    }
}
