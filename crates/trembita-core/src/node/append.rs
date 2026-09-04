use super::RaftNode;
use super::prelude::*;

impl RaftNode {
    // ---- AppendEntries ---------------------------------------------------

    pub(in crate::node) fn handle_append_entries(&mut self, from: NodeId, ae: &AppendEntries) {
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
                    self.log_truncate_from(idx);
                    self.log_push(LogEntry {
                        term: entry.term,
                        index: idx,
                        payload: entry.payload.clone(),
                    });
                }
                None => {
                    self.log_push(LogEntry {
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

    pub(in crate::node) fn handle_append_reply(
        &mut self,
        from: NodeId,
        reply: &AppendEntriesReply,
    ) {
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
            // A successful ack is our freshest proof the peer is alive (liveness-vs-membership).
            self.last_ack_clock.insert(from, self.logical_clock);
            if self.config.reachability.detector == FailureDetectorKind::PhiAccrual {
                self.phi_liveness.record_heartbeat(from, self.logical_clock);
            }
            self.confirm_reads(from, reply.round);
            if reply.round >= self.lease_round {
                self.lease_acks.insert(from);
                self.maybe_extend_lease();
            }
            self.maybe_advance_commit();
            self.try_complete_reads();
        } else {
            let ni = if let Some(ci) = reply.conflict_index {
                LogIndex(ci.0.max(1))
            } else {
                let cur = self.next_index.get(&from).copied().unwrap_or(LogIndex(1)).0;
                LogIndex(cur.saturating_sub(1).max(1))
            };
            self.next_index.insert(from, ni);
            self.send_append(from);
        }
    }
}
