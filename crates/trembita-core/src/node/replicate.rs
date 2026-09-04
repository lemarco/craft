use super::RaftNode;
use super::prelude::*;

impl RaftNode {
    // ---- Replication helpers --------------------------------------------

    pub(in crate::node) fn broadcast_append(&mut self) {
        // Each broadcast opens a new heartbeat round; acks echoing this round
        // (or later) confirm leadership for any read registered before it.
        self.heartbeat_round = self.heartbeat_round.next();
        // Open a fresh lease-confirmation round: a quorum of acks for it extends
        // the leader lease, measured from *now* (before any follower has even
        // received the heartbeat), which keeps the lease conservative (read-consistency).
        self.lease_round = self.heartbeat_round;
        self.lease_round_clock = self.logical_clock;
        self.lease_acks.clear();
        self.lease_acks.insert(self.id);
        self.maybe_extend_lease();
        for p in self.peers() {
            self.send_append(p);
        }
    }

    pub(in crate::node) fn send_append(&mut self, peer: NodeId) {
        let ni = self
            .next_index
            .get(&peer)
            .copied()
            .unwrap_or_else(|| self.log.last_index().next());
        // If the entries the follower needs have been compacted away, ship the
        // snapshot instead of an AppendEntries it could never match against.
        if ni.0 <= self.log.snapshot_index().0 && self.snapshot.is_some() {
            self.send_snapshot(peer);
            return;
        }
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

    fn send_snapshot(&mut self, peer: NodeId) {
        let Some(snap) = self.snapshot.as_ref() else {
            return;
        };
        let is = InstallSnapshot {
            term: self.current_term,
            leader_id: self.id,
            last_included: LogId::new(snap.last_term, snap.last_index),
            last_config: snap.membership.clone(),
            offset: 0,
            data: snap.data.clone(),
            done: true,
        };
        self.sent_upper.insert(peer, snap.last_index);
        self.outbox
            .push(Output::Send(peer, RaftRpc::InstallSnapshot(is)));
    }

    pub(in crate::node) fn maybe_advance_commit(&mut self) {
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

    pub(in crate::node) fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            let next = self.last_applied.next();
            match self.log.get(next).map(|e| &e.payload) {
                Some(EntryPayload::Command(c)) => {
                    self.outbox.push(Output::Apply(Committed {
                        index: next,
                        command: c.clone(),
                    }));
                }
                Some(EntryPayload::Catalog(command)) => {
                    self.outbox.push(Output::CatalogApplied {
                        index: next,
                        command: command.clone(),
                    });
                }
                Some(EntryPayload::SagaJournal(command)) => {
                    self.outbox.push(Output::SagaJournalApplied {
                        index: next,
                        command: command.clone(),
                    });
                }
                Some(EntryPayload::TwoPhasePrepare(command)) => {
                    self.outbox.push(Output::TwoPhasePrepareApplied {
                        index: next,
                        command: command.clone(),
                    });
                }
                Some(EntryPayload::TwoPhaseAbort(command)) => {
                    self.outbox.push(Output::TwoPhaseAbortApplied {
                        index: next,
                        command: command.clone(),
                    });
                }
                Some(EntryPayload::TwoPhaseJournal(command)) => {
                    self.outbox.push(Output::TwoPhaseJournalApplied {
                        index: next,
                        command: command.clone(),
                    });
                }
                Some(EntryPayload::QueueAutoscalePolicy(command)) => {
                    self.outbox.push(Output::QueueAutoscalePolicyApplied {
                        index: next,
                        command: command.clone(),
                    });
                }
                _ => {}
            }
            self.last_applied = next;
        }
    }
}
