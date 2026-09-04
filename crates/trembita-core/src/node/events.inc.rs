impl RaftNode {
    // ---- Events ----------------------------------------------------------

    /// Advance logical time by one tick (election / heartbeat timers).
    pub fn tick(&mut self) {
        self.logical_clock += 1;
        if self.role == Role::Leader {
            self.update_liveness();
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
            RaftRpc::RequestVote(rv) => self.handle_request_vote(from, &rv),
            RaftRpc::AppendEntries(ae) => self.handle_append_entries(from, &ae),
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
            RaftRpcReply::RequestVote(r) => self.handle_vote_reply(from, &r),
            RaftRpcReply::AppendEntries(r) => self.handle_append_reply(from, &r),
            RaftRpcReply::InstallSnapshot(r) => self.handle_snapshot_reply(from, &r),
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
        let idx = self.log_append(self.current_term, EntryPayload::Command(command));
        self.broadcast_append();
        self.maybe_advance_commit();
        Ok(idx)
    }

    /// Request a linearizable read (`ReadIndex`, read-consistency). The leader captures
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

    /// Compact the log up to and including `up_to`, replacing that prefix with
    /// a snapshot whose application state is `data` (Raft §7). The runtime
    /// supplies `data` from its state machine after applying through `up_to`.
    ///
    /// Returns `false` if `up_to` is not a compactable applied index
    /// (`snapshot_index < up_to <= last_applied`).
    #[must_use]
    pub fn compact(&mut self, up_to: LogIndex, data: Vec<u8>) -> bool {
        if up_to <= self.log.snapshot_index() || up_to > self.last_applied {
            return false;
        }
        let Some(term) = self.log.term_at(up_to) else {
            return false;
        };
        let membership = self.membership_at(up_to);
        self.log.compact(up_to, term);
        self.snapshot = Some(StoredSnapshot {
            last_index: up_to,
            last_term: term,
            membership,
            data,
        });
        true
    }

    /// The configuration in effect at log index `idx`: the last membership
    /// entry at or before `idx`, else the snapshot's, else the bootstrap one.
    fn membership_at(&self, idx: LogIndex) -> Membership {
        for i in (self.log.snapshot_index().0 + 1..=idx.0).rev() {
            if let Some(EntryPayload::Membership(m)) = self.log.get(LogIndex(i)).map(|e| &e.payload)
            {
                return m.clone();
            }
        }
        self.snapshot
            .as_ref()
            .map_or_else(|| self.initial.clone(), |s| s.membership.clone())
    }

    /// Begin a joint-consensus membership change to `new_voters` (+ optional
    /// `learners`). Only the leader may call this, and only when no other
    /// change is in flight (membership-early).
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
        let idx = self.log_append(self.current_term, EntryPayload::Membership(joint));
        self.broadcast_append();
        self.maybe_advance_commit();
        Ok(idx)
    }

    /// Append a catalog metadata entry to the log (group 0 only, dynamic catalog).
    ///
    /// # Errors
    /// Returns [`CatalogProposeError::NotLeader`] when this node is not leader.
    pub fn propose_catalog(
        &mut self,
        command: CatalogCommand,
    ) -> Result<LogIndex, CatalogProposeError> {
        if self.role != Role::Leader {
            return Err(CatalogProposeError::NotLeader {
                leader: self.leader_id,
            });
        }
        let idx = self.log_append(self.current_term, EntryPayload::Catalog(command));
        self.broadcast_append();
        self.maybe_advance_commit();
        Ok(idx)
    }

    /// Append a saga journal metadata entry to the log (group 0 only, Meta-Raft saga journal).
    ///
    /// # Errors
    /// Returns [`CatalogProposeError::NotLeader`] when this node is not leader.
    pub fn propose_saga_journal(
        &mut self,
        command: SagaJournalCommand,
    ) -> Result<LogIndex, CatalogProposeError> {
        if self.role != Role::Leader {
            return Err(CatalogProposeError::NotLeader {
                leader: self.leader_id,
            });
        }
        let idx = self.log_append(self.current_term, EntryPayload::SagaJournal(command));
        self.broadcast_append();
        self.maybe_advance_commit();
        Ok(idx)
    }

    /// Append a durable 2PC prepare entry to the log (any Raft group leader).
    ///
    /// # Errors
    /// Returns [`CatalogProposeError::NotLeader`] when this node is not leader.
    pub fn propose_two_phase_prepare(
        &mut self,
        command: TwoPhasePrepareCommand,
    ) -> Result<LogIndex, CatalogProposeError> {
        if self.role != Role::Leader {
            return Err(CatalogProposeError::NotLeader {
                leader: self.leader_id,
            });
        }
        let idx = self.log_append(self.current_term, EntryPayload::TwoPhasePrepare(command));
        self.broadcast_append();
        self.maybe_advance_commit();
        Ok(idx)
    }

    /// Append a durable 2PC abort entry to the log (any Raft group leader).
    ///
    /// # Errors
    /// Returns [`CatalogProposeError::NotLeader`] when this node is not leader.
    pub fn propose_two_phase_abort(
        &mut self,
        command: TwoPhaseAbortCommand,
    ) -> Result<LogIndex, CatalogProposeError> {
        if self.role != Role::Leader {
            return Err(CatalogProposeError::NotLeader {
                leader: self.leader_id,
            });
        }
        let idx = self.log_append(self.current_term, EntryPayload::TwoPhaseAbort(command));
        self.broadcast_append();
        self.maybe_advance_commit();
        Ok(idx)
    }

    /// Append a 2PC client journal metadata entry to the log (group 0 / Meta-Raft).
    ///
    /// # Errors
    /// Returns [`CatalogProposeError::NotLeader`] when this node is not leader.
    pub fn propose_two_phase_journal(
        &mut self,
        command: TwoPhaseJournalCommand,
    ) -> Result<LogIndex, CatalogProposeError> {
        if self.role != Role::Leader {
            return Err(CatalogProposeError::NotLeader {
                leader: self.leader_id,
            });
        }
        let idx = self.log_append(self.current_term, EntryPayload::TwoPhaseJournal(command));
        self.broadcast_append();
        self.maybe_advance_commit();
        Ok(idx)
    }

    /// Append a queue autoscale policy metadata entry to the log (Meta-Raft / group 0).
    ///
    /// # Errors
    /// Returns [`CatalogProposeError::NotLeader`] when this node is not leader.
    pub fn propose_queue_autoscale_policy(
        &mut self,
        command: QueueAutoscalePolicyCommand,
    ) -> Result<LogIndex, CatalogProposeError> {
        if self.role != Role::Leader {
            return Err(CatalogProposeError::NotLeader {
                leader: self.leader_id,
            });
        }
        let idx = self.log_append(
            self.current_term,
            EntryPayload::QueueAutoscalePolicy(command),
        );
        self.broadcast_append();
        self.maybe_advance_commit();
        Ok(idx)
    }
}
