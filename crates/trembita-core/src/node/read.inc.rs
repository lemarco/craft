impl RaftNode {
    // ---- ReadIndex (read-consistency) --------------------------------------------

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

    /// The leader lease duration, in logical ticks. Deliberately a fraction of
    /// the *minimum* election timeout so the lease is guaranteed to expire on
    /// the leader before any follower — which reset its election timer when it
    /// received the acked heartbeat — could time out and start an election.
    /// Halving leaves generous headroom for cross-node clock drift (read-consistency;
    /// this is why lease reads were originally deferred as "clock-sensitive").
    fn lease_ticks(&self) -> u64 {
        self.config.election_timeout_min / 2
    }

    /// Extend the leader lease if a quorum has acked the current lease round.
    /// Measured from when the round was broadcast (`lease_round_clock`), so the
    /// lease is always conservative relative to when followers last heard us.
    fn maybe_extend_lease(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        if self.configuration().has_quorum(&self.lease_acks) {
            let candidate = self.lease_round_clock.saturating_add(self.lease_ticks());
            if candidate > self.lease_expiry {
                self.lease_expiry = candidate;
            }
        }
    }

    /// Whether this node currently holds a valid leadership lease (leader, and
    /// within the lease window). Observability / test hook.
    #[must_use]
    pub fn lease_valid(&self) -> bool {
        self.role == Role::Leader && self.logical_clock < self.lease_expiry
    }

    /// Attempt a **lease read** (read-consistency): if this leader holds a valid lease and
    /// has committed an entry in its current term, return `Ok(Some(index))` — the
    /// read may be served by running `query` once the state machine has applied
    /// through `index`, with **no** `ReadIndex` round-trip. Returns `Ok(None)` when
    /// no valid lease is held (the caller should fall back to
    /// [`read_index`](Self::read_index)).
    ///
    /// # Errors
    /// Returns [`NotLeader`] with a redirect hint if this node is not the leader.
    pub fn lease_read(&self) -> Result<Option<LogIndex>, NotLeader> {
        if self.role != Role::Leader {
            return Err(NotLeader {
                leader: self.leader_id,
            });
        }
        // The commit index is only authoritative once an entry of the current
        // term has committed (leader completeness); until then, fall back.
        let authoritative = self.log.term_at(self.commit_index) == Some(self.current_term);
        if self.logical_clock < self.lease_expiry && authoritative {
            Ok(Some(self.commit_index))
        } else {
            Ok(None)
        }
    }

    /// The voters this node currently considers **reachable** — a liveness
    /// signal distinct from committed membership (liveness-vs-membership).
    ///
    /// On the leader this is itself plus every voter that acked an
    /// `AppendEntries` within the last `window` logical ticks; a voter silent for
    /// longer is treated as crashed/partitioned even though it is still a
    /// committed voter. A non-leader has no first-hand ack data, so it
    /// conservatively reports the full voter set and leaves crash detection to
    /// the leader (which is where reconcile runs anyway, supervisor-leader).
    ///
    /// `window` should comfortably exceed the heartbeat interval so a healthy
    /// follower is never flagged; [`reachable_now`](Self::reachable_now) applies
    /// a sensible default derived from the election timeout.
    #[must_use]
    pub fn reachable(&self, window: u64) -> Vec<NodeId> {
        let voters = self.configuration().voters();
        if self.role != Role::Leader {
            return voters;
        }
        let now = self.logical_clock;
        voters
            .into_iter()
            .filter(|&v| {
                v == self.id
                    || self
                        .last_ack_clock
                        .get(&v)
                        .is_some_and(|&acked| now.saturating_sub(acked) <= window)
            })
            .collect()
    }

    /// [`reachable`](Self::reachable) with configured window, hysteresis, or
    /// phi-accrual (reachability tuning). Updated every leader
    /// [`tick`](Self::tick).
    #[must_use]
    pub fn reachable_now(&self) -> Vec<NodeId> {
        let voters = self.configuration().voters();
        if self.role != Role::Leader {
            return voters;
        }
        let now = self.logical_clock;
        voters
            .into_iter()
            .filter(|&v| match self.config.reachability.detector {
                FailureDetectorKind::AckWindow => v == self.id || self.ack_liveness.is_reachable(v),
                FailureDetectorKind::PhiAccrual => {
                    v == self.id || self.phi_liveness.is_reachable(v, now)
                }
            })
            .collect()
    }

    /// Voters plus learners that acked recently — used for worker placement and
    /// auto-spawn on elastic joiners. Queue replication still uses
    /// [`reachable_now`](Self::reachable_now) (voters only).
    #[must_use]
    pub fn reachable_members_now(&self) -> Vec<NodeId> {
        let membership = self.configuration().to_membership();
        let mut members = membership.voters;
        members.extend(membership.learners);
        members.sort();
        members.dedup();
        if self.role != Role::Leader {
            return members;
        }
        let now = self.logical_clock;
        members
            .into_iter()
            .filter(|&id| self.is_member_reachable(id, now))
            .collect()
    }

    fn is_member_reachable(&self, peer: NodeId, now: u64) -> bool {
        if peer == self.id {
            return true;
        }
        let conf = self.configuration();
        match self.config.reachability.detector {
            FailureDetectorKind::AckWindow => {
                if conf.is_voter(peer) {
                    self.ack_liveness.is_reachable(peer)
                } else {
                    let window = self
                        .config
                        .reachability
                        .window(self.config.election_timeout_max);
                    self.last_ack_clock
                        .get(&peer)
                        .is_some_and(|&acked| now.saturating_sub(acked) <= window)
                }
            }
            FailureDetectorKind::PhiAccrual => self.phi_liveness.is_reachable(peer, now),
        }
    }

    fn update_liveness(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let voters = self.configuration().voters();
        let now = self.logical_clock;
        match self.config.reachability.detector {
            FailureDetectorKind::AckWindow => {
                let window = self
                    .config
                    .reachability
                    .window(self.config.election_timeout_max);
                let hysteresis = self
                    .config
                    .reachability
                    .hysteresis(self.config.election_timeout_min);
                self.ack_liveness.update(
                    now,
                    self.id,
                    &voters,
                    &self.last_ack_clock,
                    window,
                    hysteresis,
                );
            }
            FailureDetectorKind::PhiAccrual => {}
        }
    }
}
