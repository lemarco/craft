//! Raft peer-to-peer RPC wire types (ADR 010, ADR 016).

use serde::{Deserialize, Serialize};

use crate::{LogIndex, NodeId, Term};

/// A single Raft log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Term in which the entry was created.
    pub term: Term,
    /// Position in the log.
    pub index: LogIndex,
    /// What the entry carries.
    pub payload: EntryPayload,
}

/// The contents of a [`LogEntry`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryPayload {
    /// No-op appended by a new leader to commit prior-term entries.
    Noop,
    /// Opaque, application-encoded command bytes applied to the state machine.
    Command(Vec<u8>),
    /// Cluster membership change entry (joint consensus, ADR 016).
    Membership(Membership),
}

/// A cluster configuration. During joint consensus both the incoming
/// (`voters`) and outgoing (`voters_outgoing`) configurations are active.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Membership {
    /// Incoming/current voting members (C_new).
    pub voters: Vec<NodeId>,
    /// Outgoing voting members during a joint config (C_old); empty when stable.
    pub voters_outgoing: Vec<NodeId>,
    /// Non-voting members that only receive the log (catch-up / observers).
    pub learners: Vec<NodeId>,
}

impl Membership {
    /// Whether this configuration is a joint (transitional) configuration.
    #[must_use]
    pub fn is_joint(&self) -> bool {
        !self.voters_outgoing.is_empty()
    }
}

/// A Raft RPC request sent over `/peer/wire`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftRpc {
    /// Candidate requesting votes.
    RequestVote(RequestVote),
    /// Leader replicating entries / heartbeat.
    AppendEntries(AppendEntries),
    /// Leader shipping a snapshot to a lagging follower.
    InstallSnapshot(InstallSnapshot),
}

/// A Raft RPC reply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftRpcReply {
    /// Reply to [`RequestVote`].
    RequestVote(RequestVoteReply),
    /// Reply to [`AppendEntries`].
    AppendEntries(AppendEntriesReply),
    /// Reply to [`InstallSnapshot`].
    InstallSnapshot(InstallSnapshotReply),
}

/// `RequestVote` RPC.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestVote {
    /// Candidate's term.
    pub term: Term,
    /// Candidate requesting the vote.
    pub candidate_id: NodeId,
    /// Index of the candidate's last log entry.
    pub last_log_index: LogIndex,
    /// Term of the candidate's last log entry.
    pub last_log_term: Term,
    /// Pre-vote probe (does not increment term when set).
    pub pre_vote: bool,
}

/// Reply to a [`RequestVote`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestVoteReply {
    /// Responder's current term.
    pub term: Term,
    /// Whether the vote was granted.
    pub vote_granted: bool,
    /// Echoes the request's pre-vote flag so a pre-candidate can distinguish
    /// pre-vote replies from real-vote replies (Raft thesis §9.6).
    pub pre_vote: bool,
}

/// `AppendEntries` RPC (also used as heartbeat when `entries` is empty).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendEntries {
    /// Leader's term.
    pub term: Term,
    /// Leader sending the entries.
    pub leader_id: NodeId,
    /// Index of the log entry immediately preceding new ones.
    pub prev_log_index: LogIndex,
    /// Term of `prev_log_index`.
    pub prev_log_term: Term,
    /// Entries to store (empty for heartbeat).
    pub entries: Vec<LogEntry>,
    /// Leader's commit index.
    pub leader_commit: LogIndex,
}

/// Reply to an [`AppendEntries`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendEntriesReply {
    /// Responder's current term.
    pub term: Term,
    /// Whether the entries were accepted.
    pub success: bool,
    /// Conflict hint for fast log backtracking.
    pub conflict_index: Option<LogIndex>,
    /// Term of the conflicting entry, if any.
    pub conflict_term: Option<Term>,
}

/// `InstallSnapshot` RPC (chunked).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallSnapshot {
    /// Leader's term.
    pub term: Term,
    /// Leader sending the snapshot.
    pub leader_id: NodeId,
    /// Snapshot replaces all entries up to and including this index.
    pub last_included_index: LogIndex,
    /// Term of `last_included_index`.
    pub last_included_term: Term,
    /// Byte offset of this chunk within the snapshot.
    pub offset: u64,
    /// Raw snapshot chunk bytes.
    pub data: Vec<u8>,
    /// Whether this is the final chunk.
    pub done: bool,
}

/// Reply to an [`InstallSnapshot`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallSnapshotReply {
    /// Responder's current term.
    pub term: Term,
}
