impl RaftNode {
    // ---- InstallSnapshot (Raft §7) ---------------------------------------

    fn handle_install_snapshot(&mut self, from: NodeId, is: InstallSnapshot) {
        if is.term < self.current_term {
            self.reply_snapshot(from);
            return;
        }
        if is.term > self.current_term {
            self.become_follower(is.term);
        } else if self.role != Role::Follower {
            self.set_role(Role::Follower);
        }
        self.leader_id = Some(is.leader_id);
        self.reset_election_timer();

        let last = is.last_included;
        // Ignore snapshots we already cover; nothing to install.
        if last.index.0 <= self.log.snapshot_index().0 || last.index <= self.last_applied {
            self.reply_snapshot(from);
            return;
        }

        self.log.install_snapshot(last.index, last.term);
        // Installing a snapshot may discard conflicting entries beyond the
        // boundary; mark the log dirty from just past it so `take_persist`
        // reconciles the stored suffix (truncate + re-append the retained tail)
        // before the driver purges the compacted prefix (backlog A6).
        self.mark_log_dirty(LogIndex(last.index.0 + 1));
        self.snapshot = Some(StoredSnapshot {
            last_index: last.index,
            last_term: last.term,
            membership: is.last_config.clone(),
            data: is.data.clone(),
        });
        if self.commit_index < last.index {
            self.commit_index = last.index;
        }
        self.last_applied = last.index;
        self.outbox.push(Output::LoadSnapshot {
            index: last.index,
            data: is.data,
        });
        self.reply_snapshot(from);
    }

    fn reply_snapshot(&mut self, to: NodeId) {
        let reply = InstallSnapshotReply {
            term: self.current_term,
        };
        self.outbox
            .push(Output::Reply(to, RaftRpcReply::InstallSnapshot(reply)));
    }

    fn handle_snapshot_reply(&mut self, from: NodeId, reply: &InstallSnapshotReply) {
        if self.role != Role::Leader || reply.term != self.current_term {
            return;
        }
        // The follower is now caught up to the snapshot boundary we sent.
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
        self.maybe_advance_commit();
    }
}
