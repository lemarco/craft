//! The pure Raft consensus state machine.
//!
//! [`RaftNode`] performs no I/O: it consumes events (`tick`, `receive`,
//! `receive_reply`, `propose`, `propose_membership`, `read_index`) and
//! accumulates [`Output`] effects that an outer runtime executes (send
//! messages, apply commands, complete reads). Time is logical — the runtime
//! calls [`RaftNode::tick`] once per logical unit — so a given seed replays
//! deterministically (testing-strategy, architecture-style).
//!
//! * Membership uses **joint consensus** (membership-early): a change appends a
//!   transitional `C_old,new` entry that requires majorities in *both* voter
//!   sets; once it commits, the leader appends the final `C_new`.
//! * Elections use **Pre-Vote** (Raft thesis §9.6) so isolated nodes cannot
//!   disrupt a live leader by inflating terms.
//! * Linearizable reads use **`ReadIndex`** (read-consistency): the leader confirms it is
//!   still leader via a heartbeat round to a quorum before serving the read.

use std::collections::{BTreeMap, BTreeSet};

use trembita_proto::{
    AppendEntries, AppendEntriesReply, CatalogCommand, EntryPayload, InstallSnapshot,
    InstallSnapshotReply, LogEntry, LogId, LogIndex, Membership, NodeId,
    QueueAutoscalePolicyCommand, RaftRpc, RaftRpcReply, RequestVote, RequestVoteReply, Round,
    SagaJournalCommand, Term, TwoPhaseAbortCommand, TwoPhaseJournalCommand, TwoPhasePrepareCommand,
};

use crate::config::Configuration;
use crate::failure_detector::{
    AckWindowLiveness, FailureDetectorKind, PhiAccrualLiveness, ReachabilityConfig,
};
use crate::log::Log;
use crate::rng::Rng;

mod types;

pub use types::{
    CatalogProposeError, Committed, Config, MembershipError, NotLeader, Output, Persist, ReadId,
    Role, SnapshotState,
};

include!("struct.inc.rs");
include!("bootstrap.inc.rs");
include!("accessors.inc.rs");
include!("log_track.inc.rs");
include!("events.inc.rs");
include!("role.inc.rs");
include!("vote.inc.rs");
include!("append.inc.rs");
include!("replicate.inc.rs");
include!("read.inc.rs");
include!("membership.inc.rs");
include!("snapshot.inc.rs");
