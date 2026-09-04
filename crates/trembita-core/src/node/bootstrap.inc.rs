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
        let phi_threshold = config.reachability.phi_threshold;
        Self {
            id,
            initial: membership,
            config,
            current_term: Term::ZERO,
            voted_for: None,
            log: Log::default(),
            persisted_term: Term::ZERO,
            persisted_vote: None,
            log_dirty_from: None,
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
            snapshot: None,
            last_ack_clock: BTreeMap::new(),
            ack_liveness: AckWindowLiveness::default(),
            phi_liveness: PhiAccrualLiveness::new(phi_threshold),
            lease_round: Round::ZERO,
            lease_round_clock: 0,
            lease_acks: BTreeSet::new(),
            lease_expiry: 0,
            elapsed: 0,
            heartbeat_elapsed: 0,
            election_timeout,
            logical_clock: 0,
            rng,
            outbox: Vec::new(),
        }
    }

    /// Rebuild a node from durably persisted state after a restart (backlog
    /// B4). `term`/`voted_for` come from the stored `HardState` and `entries`
    /// are the stored log (ascending, contiguous from index 1 — no snapshot
    /// support yet). The node comes back as a [`Follower`](Role::Follower) with
    /// `commit_index`/`last_applied` at 0: it re-learns its commit index from
    /// the current leader (or re-derives it after winning an election) and the
    /// application state machine is rebuilt by replaying the recovered log.
    ///
    /// `members` is the bootstrap voter set, used only when the recovered log
    /// carries no membership entry (a cluster that never reconfigured).
    #[must_use]
    pub fn restore(
        id: NodeId,
        members: impl IntoIterator<Item = NodeId>,
        config: Config,
        term: Term,
        voted_for: Option<NodeId>,
        entries: impl IntoIterator<Item = LogEntry>,
    ) -> Self {
        let mut node = Self::new(id, members, config);
        node.current_term = term;
        node.voted_for = voted_for;
        for entry in entries {
            node.log.push_entry(entry);
        }
        // Everything just loaded is already durable; start with a clean slate
        // so the first `take_persist` reports only post-restart changes.
        node.persisted_term = term;
        node.persisted_vote = voted_for;
        node.log_dirty_from = None;
        node
    }

    /// Rebuild a node from a durable snapshot plus the live log suffix after a
    /// restart (backlog A6). Used when the stored log was compacted: `snapshot`
    /// summarizes everything through `snapshot.last_included`, and `entries` are
    /// the remaining log entries (indices strictly greater than the boundary,
    /// ascending and contiguous).
    ///
    /// The application state machine must be restored from `snapshot.data`
    /// *before* the node is driven; the node comes back as a
    /// [`Follower`](Role::Follower) with `commit_index`/`last_applied` at the
    /// snapshot boundary (which is durably committed), then re-learns any higher
    /// commit index from the current leader and replays the suffix.
    #[must_use]
    pub fn restore_with_snapshot(
        id: NodeId,
        members: impl IntoIterator<Item = NodeId>,
        config: Config,
        term: Term,
        voted_for: Option<NodeId>,
        snapshot: SnapshotState,
        entries: impl IntoIterator<Item = LogEntry>,
    ) -> Self {
        let mut node = Self::new(id, members, config);
        node.current_term = term;
        node.voted_for = voted_for;
        let last = snapshot.last_included;
        node.log.install_snapshot(last.index, last.term);
        node.snapshot = Some(StoredSnapshot {
            last_index: last.index,
            last_term: last.term,
            membership: snapshot.membership,
            data: snapshot.data,
        });
        for entry in entries {
            node.log.push_entry(entry);
        }
        // The snapshot boundary is durably committed and already reflected in
        // the restored state machine.
        node.commit_index = last.index;
        node.last_applied = last.index;
        node.persisted_term = term;
        node.persisted_vote = voted_for;
        node.log_dirty_from = None;
        node
    }
}
