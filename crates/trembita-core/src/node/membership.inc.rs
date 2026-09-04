impl RaftNode {
    // ---- Membership finalization (membership-early) ------------------------------

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
            self.log_append(self.current_term, EntryPayload::Membership(final_config));
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
}
