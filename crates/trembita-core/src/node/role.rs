use super::RaftNode;
use super::prelude::*;

impl RaftNode {
    // ---- Role transitions ------------------------------------------------

    pub(in crate::node) fn set_role(&mut self, role: Role) {
        if self.role != role {
            tracing::debug!(
                target: "trembita::raft",
                node = self.id.0,
                term = self.current_term.0,
                ?role,
                "raft role changed"
            );
            self.role = role;
            self.outbox.push(Output::RoleChanged(role));
        }
    }

    pub(in crate::node) fn become_follower(&mut self, term: Term) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        self.votes.clear();
        self.fail_pending_reads();
        // Surrender the lease immediately on step-down: a follower must never
        // serve a lease read, and a stale lease could otherwise linger.
        self.lease_expiry = 0;
        self.lease_acks.clear();
        self.set_role(Role::Follower);
    }

    /// Pre-vote round: probe whether a real election could succeed *without*
    /// bumping our term, so an isolated/removed node cannot disrupt a live
    /// leader by forcing term inflation (Raft thesis §9.6).
    pub(in crate::node) fn start_pre_election(&mut self) {
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

    pub(in crate::node) fn start_real_election(&mut self) {
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

    pub(in crate::node) fn become_leader(&mut self) {
        self.set_role(Role::Leader);
        self.leader_id = Some(self.id);
        // A fresh term starts with no lease; it is earned once a heartbeat round
        // in this term is acked by a quorum (via `broadcast_append` below).
        self.lease_expiry = 0;
        let next = self.log.last_index().next();
        self.next_index.clear();
        self.match_index.clear();
        self.sent_upper.clear();
        // Reachability is earned afresh each term from this leader's own acks;
        // stale observations from a prior leadership must not count (liveness-vs-membership).
        self.last_ack_clock.clear();
        for p in self.peers() {
            self.next_index.insert(p, next);
            self.match_index.insert(p, LogIndex::ZERO);
        }
        // A no-op in the new term lets prior-term entries commit safely.
        self.log_append(self.current_term, EntryPayload::Noop);
        self.heartbeat_elapsed = 0;
        self.broadcast_append();
        self.maybe_advance_commit();
    }

    pub(in crate::node) fn reset_election_timer(&mut self) {
        self.elapsed = 0;
        self.election_timeout = self.rng.range(
            self.config.election_timeout_min,
            self.config.election_timeout_max,
        );
    }
}
