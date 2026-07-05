//! `craft-proto` — wire types and [`postcard`] codec shared across all craft crates.
//!
//! Defines the on-the-wire representation for Raft peer RPCs, the client API,
//! cluster join handshakes, and actor messaging. All bodies are encoded with
//! `postcard` (ADR 010, ADR 011). Nothing here performs I/O.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub mod actor;
pub mod client;
pub mod join;
pub mod raft;

pub use actor::{ActorEnvelope, ActorRef};
pub use client::{ClientRequest, ClientResponse};
pub use join::{JoinRejection, JoinRequest, JoinResponse};
pub use raft::{
    AppendEntries, AppendEntriesReply, EntryPayload, InstallSnapshot, InstallSnapshotReply,
    LogEntry, Membership, RaftRpc, RaftRpcReply, RequestVote, RequestVoteReply,
};

/// Wire/protocol version negotiated on join (ADR 020: hard reject on mismatch).
pub const PROTOCOL_VERSION: u32 = 1;

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

/// Errors from encoding/decoding wire bodies.
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    /// `postcard` serialization failed.
    #[error("postcard encode failed: {0}")]
    Encode(String),
    /// `postcard` deserialization failed.
    #[error("postcard decode failed: {0}")]
    Decode(String),
}

/// Encode a value to a `postcard` byte vector.
///
/// # Errors
/// Returns [`CodecError::Encode`] if serialization fails.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    postcard::to_stdvec(value).map_err(|e| CodecError::Encode(e.to_string()))
}

/// Decode a value from a `postcard` byte slice.
///
/// # Errors
/// Returns [`CodecError::Decode`] if deserialization fails.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
    postcard::from_bytes(bytes).map_err(|e| CodecError::Decode(e.to_string()))
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
    fn roundtrip_raft_rpc() {
        let rpc = RaftRpc::AppendEntries(AppendEntries {
            term: Term(2),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(4),
            prev_log_term: Term(1),
            entries: vec![LogEntry {
                term: Term(2),
                index: LogIndex(5),
                payload: EntryPayload::Noop,
            }],
            leader_commit: LogIndex(4),
        });
        let bytes = encode(&rpc).expect("encode");
        let back: RaftRpc = decode(&bytes).expect("decode");
        assert_eq!(rpc, back);
    }

    #[test]
    fn decode_rejects_garbage() {
        let err = decode::<LogEntry>(&[0xff, 0xff, 0xff, 0xff]);
        assert!(err.is_err());
    }
}
