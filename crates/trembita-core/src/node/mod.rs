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

use trembita_proto::{LogIndex, Membership, NodeId, Round, Term};

use crate::failure_detector::{AckWindowLiveness, PhiAccrualLiveness};
use crate::log::Log;
use crate::rng::Rng;

mod accessors;
mod append;
mod bootstrap;
mod events;
mod log_track;
mod membership;
mod prelude;
mod read;
mod replicate;
mod role;
mod snapshot;
mod types;
mod vote;

pub use types::{
    CatalogProposeError, Committed, Config, MembershipError, NotLeader, Output, Persist, ReadId,
    Role, SnapshotState,
};

#[derive(Debug, Clone)]
struct PendingRead {
    id: ReadId,
    index: LogIndex,
    round: Round,
    acks: BTreeSet<NodeId>,
}

/// The most recent snapshot this node holds — enough to ship to a lagging
/// follower and to recover the configuration after log compaction (Raft §7).
#[derive(Debug, Clone)]
struct StoredSnapshot {
    last_index: LogIndex,
    last_term: Term,
    membership: Membership,
    data: Vec<u8>,
}

/// A single Raft participant: a deterministic, I/O-free state machine.
#[derive(Debug, Clone)]
pub struct RaftNode {
    id: NodeId,
    initial: Membership,
    config: Config,

    // Persistent state (runtime is responsible for durability).
    current_term: Term,
    voted_for: Option<NodeId>,
    log: Log,

    // Durability watermarks (B4): the term/vote last handed to the storage
    // adapter and the lowest log index changed since then, so `take_persist`
    // can emit just the delta an outer runtime must fsync before acting on any
    // network effect from the same step (Raft §5.1–§5.3).
    persisted_term: Term,
    persisted_vote: Option<NodeId>,
    log_dirty_from: Option<LogIndex>,

    // Volatile state.
    role: Role,
    leader_id: Option<NodeId>,
    commit_index: LogIndex,
    last_applied: LogIndex,

    // Candidate state.
    votes: BTreeSet<NodeId>,

    // Leader state.
    next_index: BTreeMap<NodeId, LogIndex>,
    match_index: BTreeMap<NodeId, LogIndex>,
    sent_upper: BTreeMap<NodeId, LogIndex>,
    heartbeat_round: Round,
    pending_reads: Vec<PendingRead>,
    snapshot: Option<StoredSnapshot>,

    // Failure detection (liveness-vs-membership liveness): the `logical_clock` tick at which
    // each peer last acked an AppendEntries. Only the leader populates this (it
    // is the only role that solicits acks); it underpins `reachable`, a liveness
    // signal distinct from committed voter membership, so crash detection need
    // not wait for a `ConfChange`.
    last_ack_clock: BTreeMap<NodeId, u64>,
    ack_liveness: AckWindowLiveness,
    phi_liveness: PhiAccrualLiveness,

    // Leader lease (read-consistency lease reads): the leader may serve a read locally,
    // with no fresh quorum round, while it holds a valid lease. The lease is
    // extended to `lease_round_clock + lease_ticks` whenever a quorum acks the
    // heartbeat round broadcast at `lease_round_clock`; `lease_acks` accumulates
    // the acks for the current `lease_round`.
    lease_round: Round,
    lease_round_clock: u64,
    lease_acks: BTreeSet<NodeId>,
    lease_expiry: u64,

    // Timing (logical ticks).
    elapsed: u64,
    heartbeat_elapsed: u64,
    election_timeout: u64,
    /// Monotonic logical clock (never reset): the tick count since construction,
    /// used as the time base for the leader lease.
    logical_clock: u64,
    rng: Rng,

    outbox: Vec<Output>,
}
