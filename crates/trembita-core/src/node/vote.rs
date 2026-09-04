use super::RaftNode;
use super::prelude::*;

impl RaftNode {
    // ---- RequestVote -----------------------------------------------------

    pub(in crate::node) fn handle_request_vote(&mut self, from: NodeId, rv: &RequestVote) {
        if !self.is_voter(self.id) {
            self.reply_vote(from, false, rv.pre_vote);
            return;
        }

        let up_to_date = rv.last_log >= self.log.last_id();

        if rv.pre_vote {
            // Pre-vote never changes our term or vote. Refuse if we still
            // believe a leader is alive (heard from it within the min timeout),
            // which is what neutralizes disruptive removed servers. Use
            // `last_leader_contact`, not the election timer: the timer resets
            // when campaigning and would otherwise livelock elections.
            let leader_recent = self.leader_id.is_some()
                && self.logical_clock.saturating_sub(self.last_leader_contact)
                    < self.config.election_timeout_min;
            let granted = rv.term >= self.current_term && up_to_date && !leader_recent;
            if !granted {
                tracing::debug!(
                    target: "trembita::raft",
                    node = self.id.0,
                    from = from.0,
                    term = self.current_term.0,
                    candidate_term = rv.term.0,
                    leader_recent,
                    up_to_date,
                    "pre-vote rejected"
                );
            }
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

    pub(in crate::node) fn handle_vote_reply(&mut self, from: NodeId, reply: &RequestVoteReply) {
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
}
