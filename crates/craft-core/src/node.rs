//! The pure Raft consensus state machine.
//!
//! [`RaftNode`] performs no I/O: it consumes events (`tick`, `receive`,
//! `receive_reply`, `propose`, `propose_membership`, `read_index`) and
//! accumulates [`Output`] effects that an outer runtime executes (send
//! messages, apply commands, complete reads). Time is logical — the runtime
//! calls [`RaftNode::tick`] once per logical unit — so a given seed replays
//! deterministically (ADR 029, ADR 030).
//!
//! * Membership uses **joint consensus** (ADR 016): a change appends a
//!   transitional `C_old,new` entry that requires majorities in *both* voter
//!   sets; once it commits, the leader appends the final `C_new`.
//! * Elections use **Pre-Vote** (Raft thesis §9.6) so isolated nodes cannot
//!   disrupt a live leader by inflating terms.
//! * Linearizable reads use **ReadIndex** (ADR 005): the leader confirms it is
//!   still leader via a heartbeat round to a quorum before serving the read.

use std::collections::{BTreeMap, BTreeSet};

use craft_proto::{
    AppendEntries, AppendEntriesReply, EntryPayload, InstallSnapshot, InstallSnapshotReply,
    LogEntry, LogId, LogIndex, Membership, NodeId, RaftRpc, RaftRpcReply, RequestVote,
    RequestVoteReply, Round, Term,
};

use crate::config::Configuration;
use crate::log::Log;
use crate::rng::Rng;

/// The role a node currently plays in its term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Passive; redirects clients and waits for heartbeats.
    Follower,
    /// Running a pre-vote round (no term bump yet) to avoid disrupting a
    /// live leader (Raft thesis §9.6).
    PreCandidate,
    /// Seeking votes for a new term.
    Candidate,
    /// Elected; replicates the log and serves clients.
    Leader,
}

/// Timing and determinism configuration, in logical ticks.
#[derive(Debug, Clone)]
pub struct Config {
    /// Lower bound of the randomized election timeout (ticks).
    pub election_timeout_min: u64,
    /// Upper bound of the randomized election timeout (ticks).
    pub election_timeout_max: u64,
    /// Ticks between leader heartbeats.
    pub heartbeat_interval: u64,
    /// Seed mixed with the node id for deterministic timeout jitter.
    pub seed: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            election_timeout_min: 10,
            election_timeout_max: 20,
            heartbeat_interval: 3,
            seed: 0,
        }
    }
}

/// A committed application command ready to apply to the state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Committed {
    /// Log index of the command.
    pub index: LogIndex,
    /// The application-encoded command bytes.
    pub command: Vec<u8>,
}

/// Client-supplied token identifying a linearizable read request (ADR 005).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadId(pub u64);

/// An effect produced by the core for the runtime to execute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    /// Send a request RPC to a peer.
    Send(NodeId, RaftRpc),
    /// Reply to a peer's request RPC.
    Reply(NodeId, RaftRpcReply),
    /// A committed command to apply, in index order.
    Apply(Committed),
    /// The node changed role (useful for observability and tests).
    RoleChanged(Role),
    /// A ReadIndex read is safe to serve: the state machine at `index` (or
    /// later) reflects everything committed before the request (ADR 005).
    ReadReady {
        /// The client's read token.
        id: ReadId,
        /// The confirmed read index.
        index: LogIndex,
    },
    /// A pending read could not be honored (leadership was lost); retry it
    /// against the new leader.
    ReadFailed {
        /// The client's read token.
        id: ReadId,
    },
}

/// Returned by [`RaftNode::propose`] / [`RaftNode::read_index`] when the node
/// is not the leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotLeader {
    /// Best-known current leader, if any, for client redirection.
    pub leader: Option<NodeId>,
}

/// Why a membership change could not be started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipError {
    /// This node is not the leader.
    NotLeader {
        /// Best-known current leader, if any.
        leader: Option<NodeId>,
    },
    /// A previous membership change has not finished committing yet.
    InProgress,
    /// The requested configuration has no voters.
    EmptyVoters,
}

/// A ReadIndex request awaiting leadership confirmation and apply catch-up.
#[derive(Debug, Clone)]
struct PendingRead {
    id: ReadId,
    index: LogIndex,
    round: Round,
    acks: BTreeSet<NodeId>,
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

    // Timing (logical ticks).
    elapsed: u64,
    heartbeat_elapsed: u64,
    election_timeout: u64,
    rng: Rng,

    outbox: Vec<Output>,
}

impl RaftNode {
    /// Create a node whose initial voting set is `members` (including `id`).
    #[must_use]
    pub fn new(id: NodeId, members: impl IntoIterator<Item = NodeId>, config: Config) -> Self {
        let mut voters: Vec<NodeId> = members.into_iter().collect();
        voters.sort();
        voters.dedup();
        let membership = Membership {
            voters,
            voters_outgoing: Vec::new(),
            learners: Vec::new(),
        };
        Self::with_membership(id, membership, config)
    }

    /// Create a node with an explicit initial [`Membership`] (voters +
    /// learners), used to bootstrap clusters that grow from a subset.
    #[must_use]
    pub fn with_membership(id: NodeId, membership: Membership, config: Config) -> Self {
        let mut rng = Rng::new(config.seed ^ id.0 ^ 0x9E37_79B9_7F4A_7C15);
        let election_timeout = rng.range(config.election_timeout_min, config.election_timeout_max);
        Self {
            id,
            initial: membership,
            config,
            current_term: Term::ZERO,
            voted_for: None,
            log: Log::default(),
            role: Role::Follower,
            leader_id: None,
            commit_index: LogIndex::ZERO,
            last_applied: LogIndex::ZERO,
            votes: BTreeSet::new(),
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            sent_upper: BTreeMap::new(),
            heartbeat_round: Round::ZERO,
            pending_reads: Vec::new(),
            elapsed: 0,
            heartbeat_elapsed: 0,
            election_timeout,
            rng,
            outbox: Vec::new(),
        }
    }

    // ---- Accessors -------------------------------------------------------

    /// This node's id.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }
    /// Current role.
    #[must_use]
    pub fn role(&self) -> Role {
        self.role
    }
    /// Whether this node currently believes it is leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
    /// Current term.
    #[must_use]
    pub fn current_term(&self) -> Term {
        self.current_term
    }
    /// Best-known leader.
    #[must_use]
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }
    /// Highest committed index.
    #[must_use]
    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }
    /// Highest applied index.
    #[must_use]
    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }
    /// Index of the last log entry.
    #[must_use]
    pub fn last_log_index(&self) -> LogIndex {
        self.log.last_index()
    }
    /// Who this node voted for in the current term.
    #[must_use]
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }
    /// Term stored at `idx`, if present (for tests/introspection).
    #[must_use]
    pub fn term_at(&self, idx: LogIndex) -> Option<Term> {
        self.log.term_at(idx)
    }
    /// The active configuration (per the last config entry in the log).
    #[must_use]
    pub fn configuration(&self) -> Configuration {
        let membership = self
            .log
            .last_membership()
            .map(|(_, m)| m)
            .unwrap_or(&self.initial);
        Configuration::from_membership(membership)
    }
    /// The active voting set (sorted).
    #[must_use]
    pub fn voters(&self) -> Vec<NodeId> {
        self.configuration().voters()
    }
    /// Whether the active configuration is a joint (transitional) config.
    #[must_use]
    pub fn is_joint(&self) -> bool {
        self.configuration().is_joint()
    }

    /// Drain accumulated effects. The runtime calls this after every event.
    #[must_use]
    pub fn take_outputs(&mut self) -> Vec<Output> {
        std::mem::take(&mut self.outbox)
    }

    // ---- Configuration helpers ------------------------------------------

    fn config_index(&self) -> LogIndex {
        self.log
            .last_membership()
            .map(|(idx, _)| idx)
            .unwrap_or(LogIndex::ZERO)
    }

    fn is_voter(&self, id: NodeId) -> bool {
        self.configuration().is_voter(id)
    }

    fn peers(&self) -> Vec<NodeId> {
        self.configuration().peers(self.id)
    }

    /// Whether `acked` satisfies quorum in the current (possibly joint) config.
    fn quorum_ok(&self, acked: &BTreeSet<NodeId>) -> bool {
        self.configuration().has_quorum(acked)
    }

    fn quorum_of_votes(&self) -> bool {
        let votes = self.votes.clone();
        self.quorum_ok(&votes)
    }

    // ---- Events ----------------------------------------------------------

    /// Advance logical time by one tick (election / heartbeat timers).
    pub fn tick(&mut self) {
        if self.role == Role::Leader {
            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.config.heartbeat_interval {
                self.heartbeat_elapsed = 0;
                self.broadcast_append();
            }
        } else {
            self.elapsed += 1;
            if self.elapsed >= self.election_timeout {
                self.start_pre_election();
            }
        }
    }

    /// Force a real election immediately, skipping the pre-vote round (used
    /// for tests and leadership transfer, which bypass pre-vote by design).
    pub fn campaign(&mut self) {
        self.start_real_election();
    }

    /// Handle an inbound request RPC from `from`.
    pub fn receive(&mut self, from: NodeId, rpc: RaftRpc) {
        match rpc {
            RaftRpc::RequestVote(rv) => self.handle_request_vote(from, rv),
            RaftRpc::AppendEntries(ae) => self.handle_append_entries(from, ae),
            RaftRpc::InstallSnapshot(is) => self.handle_install_snapshot(from, is),
        }
    }

    /// Handle an inbound reply RPC from `from`.
    pub fn receive_reply(&mut self, from: NodeId, reply: RaftRpcReply) {
        let term = match &reply {
            RaftRpcReply::RequestVote(r) => r.term,
            RaftRpcReply::AppendEntries(r) => r.term,
            RaftRpcReply::InstallSnapshot(r) => r.term,
        };
        if term > self.current_term {
            self.become_follower(term);
            return;
        }
        match reply {
            RaftRpcReply::RequestVote(r) => self.handle_vote_reply(from, r),
            RaftRpcReply::AppendEntries(r) => self.handle_append_reply(from, r),
            RaftRpcReply::InstallSnapshot(_) => {}
        }
    }

    /// Propose a new command. Succeeds only on the leader; effects (log append
    /// and replication) are drained via [`RaftNode::take_outputs`].
    ///
    /// # Errors
    /// Returns [`NotLeader`] with a redirect hint if this node is not leader.
    pub fn propose(&mut self, command: Vec<u8>) -> Result<LogIndex, NotLeader> {
        if self.role != Role::Leader {
            return Err(NotLeader {
                leader: self.leader_id,
            });
        }
        let idx = self
            .log
            .append(self.current_term, EntryPayload::Command(command));
        self.broadcast_append();
        self.maybe_advance_commit();
        Ok(idx)
    }

    /// Request a linearizable read (ReadIndex, ADR 005). The leader captures
    /// its commit index and confirms it still leads by a heartbeat round to a
    /// quorum; once confirmed and applied, an [`Output::ReadReady`] is emitted.
    /// If leadership is lost first, an [`Output::ReadFailed`] is emitted.
    ///
    /// # Errors
    /// Returns [`NotLeader`] with a redirect hint if this node is not leader.
    pub fn read_index(&mut self, id: ReadId) -> Result<(), NotLeader> {
        if self.role != Role::Leader {
            return Err(NotLeader {
                leader: self.leader_id,
            });
        }
        // A fresh heartbeat round whose quorum of acks proves we still lead.
        self.broadcast_append();
        let round = self.heartbeat_round;
        let mut acks = BTreeSet::new();
        acks.insert(self.id);
        self.pending_reads.push(PendingRead {
            id,
            index: self.commit_index,
            round,
            acks,
        });
        self.try_complete_reads();
        Ok(())
    }

    /// Begin a joint-consensus membership change to `new_voters` (+ optional
    /// `learners`). Only the leader may call this, and only when no other
    /// change is in flight (ADR 016).
    ///
    /// # Errors
    /// Returns [`MembershipError`] if not leader, a change is in progress, or
    /// the new voter set is empty.
    pub fn propose_membership(
        &mut self,
        new_voters: impl IntoIterator<Item = NodeId>,
        learners: impl IntoIterator<Item = NodeId>,
    ) -> Result<LogIndex, MembershipError> {
        if self.role != Role::Leader {
            return Err(MembershipError::NotLeader {
                leader: self.leader_id,
            });
        }
        let current = self.configuration();
        if current.is_joint() || self.config_index() > self.commit_index {
            return Err(MembershipError::InProgress);
        }
        let mut voters: Vec<NodeId> = new_voters.into_iter().collect();
        voters.sort();
        voters.dedup();
        if voters.is_empty() {
            return Err(MembershipError::EmptyVoters);
        }
        let mut learners: Vec<NodeId> = learners.into_iter().collect();
        learners.sort();
        learners.dedup();
        learners.retain(|l| !voters.contains(l));

        let joint = Membership {
            voters,
            voters_outgoing: current.voters(),
            learners,
        };
        let idx = self
            .log
            .append(self.current_term, EntryPayload::Membership(joint));
        self.broadcast_append();
        self.maybe_advance_commit();
        Ok(idx)
    }

    // ---- Role transitions ------------------------------------------------

    fn set_role(&mut self, role: Role) {
        if self.role != role {
            self.role = role;
            self.outbox.push(Output::RoleChanged(role));
        }
    }

    fn become_follower(&mut self, term: Term) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        self.votes.clear();
        self.fail_pending_reads();
        self.set_role(Role::Follower);
    }

    /// Pre-vote round: probe whether a real election could succeed *without*
    /// bumping our term, so an isolated/removed node cannot disrupt a live
    /// leader by forcing term inflation (Raft thesis §9.6).
    fn start_pre_election(&mut self) {
        if !self.is_voter(self.id) {
            self.reset_election_timer();
            return;
        }
        self.set_role(Role::PreCandidate);
        self.votes.clear();
        self.votes.insert(self.id);
        self.reset_election_timer();

        if self.quorum_of_votes() {
            self.start_real_election();
            return;
        }

        // Advertise the term we *would* run in, without adopting it.
        let rv = RequestVote {
            term: self.current_term.next(),
            candidate_id: self.id,
            last_log: self.log.last_id(),
            pre_vote: true,
        };
        self.send_vote_requests(&rv);
    }

    fn start_real_election(&mut self) {
        if !self.is_voter(self.id) {
            self.reset_election_timer();
            return;
        }
        self.current_term = self.current_term.next();
        self.set_role(Role::Candidate);
        self.voted_for = Some(self.id);
        self.votes.clear();
        self.votes.insert(self.id);
        self.leader_id = None;
        self.reset_election_timer();

        if self.quorum_of_votes() {
            self.become_leader();
            return;
        }

        let rv = RequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log: self.log.last_id(),
            pre_vote: false,
        };
        self.send_vote_requests(&rv);
    }

    fn send_vote_requests(&mut self, rv: &RequestVote) {
        for p in self.configuration().voter_peers(self.id) {
            self.outbox
                .push(Output::Send(p, RaftRpc::RequestVote(rv.clone())));
        }
    }

    fn become_leader(&mut self) {
        self.set_role(Role::Leader);
        self.leader_id = Some(self.id);
        let next = self.log.last_index().next();
        self.next_index.clear();
        self.match_index.clear();
        self.sent_upper.clear();
        for p in self.peers() {
            self.next_index.insert(p, next);
            self.match_index.insert(p, LogIndex::ZERO);
        }
        // A no-op in the new term lets prior-term entries commit safely.
        self.log.append(self.current_term, EntryPayload::Noop);
        self.heartbeat_elapsed = 0;
        self.broadcast_append();
        self.maybe_advance_commit();
    }

    fn reset_election_timer(&mut self) {
        self.elapsed = 0;
        self.election_timeout = self.rng.range(
            self.config.election_timeout_min,
            self.config.election_timeout_max,
        );
    }

    // ---- RequestVote -----------------------------------------------------

    fn handle_request_vote(&mut self, from: NodeId, rv: RequestVote) {
        let up_to_date = rv.last_log >= self.log.last_id();

        if rv.pre_vote {
            // Pre-vote never changes our term or vote. Refuse if we still
            // believe a leader is alive (heard from it within the min timeout),
            // which is what neutralizes disruptive removed servers.
            let leader_recent =
                self.leader_id.is_some() && self.elapsed < self.config.election_timeout_min;
            let granted = rv.term >= self.current_term && up_to_date && !leader_recent;
            self.reply_vote(from, granted, true);
            return;
        }

        if rv.term > self.current_term {
            self.become_follower(rv.term);
        }

        let mut granted = false;
        if rv.term >= self.current_term {
            let can_vote = self.voted_for.is_none() || self.voted_for == Some(rv.candidate_id);
            if can_vote && up_to_date {
                granted = true;
                self.voted_for = Some(rv.candidate_id);
                self.reset_election_timer();
            }
        }
        self.reply_vote(from, granted, false);
    }

    fn reply_vote(&mut self, to: NodeId, vote_granted: bool, pre_vote: bool) {
        let reply = RequestVoteReply {
            term: self.current_term,
            vote_granted,
            pre_vote,
        };
        self.outbox
            .push(Output::Reply(to, RaftRpcReply::RequestVote(reply)));
    }

    fn handle_vote_reply(&mut self, from: NodeId, reply: RequestVoteReply) {
        if reply.pre_vote {
            if self.role == Role::PreCandidate && reply.vote_granted {
                self.votes.insert(from);
                if self.quorum_of_votes() {
                    self.start_real_election();
                }
            }
            return;
        }
        if self.role != Role::Candidate || reply.term != self.current_term {
            return;
        }
        if reply.vote_granted {
            self.votes.insert(from);
            if self.quorum_of_votes() {
                self.become_leader();
            }
        }
    }

    // ---- AppendEntries ---------------------------------------------------

    fn handle_append_entries(&mut self, from: NodeId, ae: AppendEntries) {
        if ae.term < self.current_term {
            self.reply_append(from, false, None, None, ae.round);
            return;
        }

        if ae.term > self.current_term {
            self.become_follower(ae.term);
        } else if self.role != Role::Follower {
            self.set_role(Role::Follower);
        }
        self.leader_id = Some(ae.leader_id);
        self.reset_election_timer();

        // Log-matching check on the entry preceding the new ones.
        if ae.prev_log.index.0 > 0 {
            match self.log.term_at(ae.prev_log.index) {
                None => {
                    let hint = self.log.last_index().next();
                    self.reply_append(from, false, Some(hint), None, ae.round);
                    return;
                }
                Some(t) if t != ae.prev_log.term => {
                    let first = self.log.first_index_of_term(t).unwrap_or(ae.prev_log.index);
                    self.reply_append(from, false, Some(first), Some(t), ae.round);
                    return;
                }
                _ => {}
            }
        }

        // Append, truncating on the first conflicting index.
        let mut idx = ae.prev_log.index;
        for entry in &ae.entries {
            idx = idx.next();
            match self.log.term_at(idx) {
                Some(t) if t == entry.term => {}
                Some(_) => {
                    self.log.truncate_from(idx);
                    self.log.push_entry(LogEntry {
                        term: entry.term,
                        index: idx,
                        payload: entry.payload.clone(),
                    });
                }
                None => {
                    self.log.push_entry(LogEntry {
                        term: entry.term,
                        index: idx,
                        payload: entry.payload.clone(),
                    });
                }
            }
        }

        if ae.leader_commit > self.commit_index {
            self.commit_index = ae.leader_commit.min(idx);
            self.apply_committed();
        }
        self.reply_append(from, true, None, None, ae.round);
    }

    fn reply_append(
        &mut self,
        to: NodeId,
        success: bool,
        conflict_index: Option<LogIndex>,
        conflict_term: Option<Term>,
        round: Round,
    ) {
        let reply = AppendEntriesReply {
            term: self.current_term,
            success,
            conflict_index,
            conflict_term,
            round,
        };
        self.outbox
            .push(Output::Reply(to, RaftRpcReply::AppendEntries(reply)));
    }

    fn handle_append_reply(&mut self, from: NodeId, reply: AppendEntriesReply) {
        if self.role != Role::Leader || reply.term != self.current_term {
            return;
        }
        if reply.success {
            let upper = self
                .sent_upper
                .get(&from)
                .copied()
                .unwrap_or(LogIndex::ZERO);
            let current = self
                .match_index
                .get(&from)
                .copied()
                .unwrap_or(LogIndex::ZERO);
            if upper > current {
                self.match_index.insert(from, upper);
            }
            self.next_index.insert(from, upper.next());
            self.confirm_reads(from, reply.round);
            self.maybe_advance_commit();
            self.try_complete_reads();
        } else {
            let ni = match reply.conflict_index {
                Some(ci) => LogIndex(ci.0.max(1)),
                None => {
                    let cur = self.next_index.get(&from).copied().unwrap_or(LogIndex(1)).0;
                    LogIndex(cur.saturating_sub(1).max(1))
                }
            };
            self.next_index.insert(from, ni);
            self.send_append(from);
        }
    }

    // ---- Replication helpers --------------------------------------------

    fn broadcast_append(&mut self) {
        // Each broadcast opens a new heartbeat round; acks echoing this round
        // (or later) confirm leadership for any read registered before it.
        self.heartbeat_round = self.heartbeat_round.next();
        for p in self.peers() {
            self.send_append(p);
        }
    }

    fn send_append(&mut self, peer: NodeId) {
        let ni = self
            .next_index
            .get(&peer)
            .copied()
            .unwrap_or_else(|| self.log.last_index().next());
        let prev_index = LogIndex(ni.0.saturating_sub(1));
        let prev_term = self.log.term_at(prev_index).unwrap_or(Term::ZERO);
        let entries = self.log.entries_from(ni).to_vec();
        let upper = LogIndex(prev_index.0 + entries.len() as u64);
        self.sent_upper.insert(peer, upper);
        let ae = AppendEntries {
            term: self.current_term,
            leader_id: self.id,
            prev_log: LogId::new(prev_term, prev_index),
            entries,
            leader_commit: self.commit_index,
            round: self.heartbeat_round,
        };
        self.outbox
            .push(Output::Send(peer, RaftRpc::AppendEntries(ae)));
    }

    fn maybe_advance_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let last = self.log.last_index().0;
        let mut new_commit = self.commit_index;
        for n in (self.commit_index.0 + 1)..=last {
            let idx = LogIndex(n);
            // Safety: a leader only commits entries from its own term directly.
            if self.log.term_at(idx) != Some(self.current_term) {
                continue;
            }
            let mut acked: BTreeSet<NodeId> = BTreeSet::new();
            acked.insert(self.id);
            for (peer, m) in &self.match_index {
                if m.0 >= n {
                    acked.insert(*peer);
                }
            }
            if self.quorum_ok(&acked) {
                new_commit = idx;
            }
        }
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.apply_committed();
            self.maybe_finalize_membership();
            self.maybe_step_down_if_removed();
            self.try_complete_reads();
        }
    }

    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            let next = self.last_applied.next();
            let command = self.log.get(next).and_then(|e| match &e.payload {
                EntryPayload::Command(c) => Some(c.clone()),
                _ => None,
            });
            self.last_applied = next;
            if let Some(command) = command {
                self.outbox.push(Output::Apply(Committed {
                    index: next,
                    command,
                }));
            }
        }
    }

    // ---- ReadIndex (ADR 005) --------------------------------------------

    /// Record that `from` acked a heartbeat at `round`, confirming leadership
    /// for every pending read registered no later than that round.
    fn confirm_reads(&mut self, from: NodeId, round: Round) {
        for r in &mut self.pending_reads {
            if round >= r.round {
                r.acks.insert(from);
            }
        }
    }

    /// Complete reads that are both leadership-confirmed (a quorum acked the
    /// read's round) and applied (`last_applied >= index`). A read is only
    /// served once the leader has committed an entry of its current term, so
    /// its commit index is authoritative.
    fn try_complete_reads(&mut self) {
        if self.role != Role::Leader || self.pending_reads.is_empty() {
            return;
        }
        if self.log.term_at(self.commit_index) != Some(self.current_term) {
            return;
        }
        let conf = self.configuration();
        let applied = self.last_applied;
        let mut ready = Vec::new();
        self.pending_reads.retain(|r| {
            if conf.has_quorum(&r.acks) && applied >= r.index {
                ready.push((r.id, r.index));
                false
            } else {
                true
            }
        });
        for (id, index) in ready {
            self.outbox.push(Output::ReadReady { id, index });
        }
    }

    fn fail_pending_reads(&mut self) {
        for r in std::mem::take(&mut self.pending_reads) {
            self.outbox.push(Output::ReadFailed { id: r.id });
        }
    }

    // ---- Membership finalization (ADR 016) ------------------------------

    /// Once a joint `C_old,new` entry commits, the leader appends the final
    /// `C_new` to leave the transitional configuration.
    fn maybe_finalize_membership(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let conf = self.configuration();
        let cfg_idx = self.config_index();
        if conf.is_joint() && cfg_idx.0 != 0 && cfg_idx <= self.commit_index {
            let final_config = Membership {
                voters: conf.voters(),
                voters_outgoing: Vec::new(),
                learners: conf.to_membership().learners,
            };
            self.log
                .append(self.current_term, EntryPayload::Membership(final_config));
            self.broadcast_append();
        }
    }

    /// If a committed, non-joint configuration excludes this leader, step down.
    fn maybe_step_down_if_removed(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let conf = self.configuration();
        if !conf.is_joint() && self.config_index() <= self.commit_index && !conf.is_voter(self.id) {
            self.become_follower(self.current_term);
            self.leader_id = None;
        }
    }

    // ---- InstallSnapshot (minimal term handling; full impl deferred) -----

    fn handle_install_snapshot(&mut self, from: NodeId, is: InstallSnapshot) {
        if is.term >= self.current_term {
            if is.term > self.current_term {
                self.become_follower(is.term);
            } else if self.role != Role::Follower {
                self.set_role(Role::Follower);
            }
            self.leader_id = Some(is.leader_id);
            self.reset_election_timer();
        }
        let reply = InstallSnapshotReply {
            term: self.current_term,
        };
        self.outbox
            .push(Output::Reply(from, RaftRpcReply::InstallSnapshot(reply)));
    }
}
