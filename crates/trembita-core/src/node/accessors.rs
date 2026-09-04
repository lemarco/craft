use super::RaftNode;
use super::prelude::*;

impl RaftNode {
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
    /// The active configuration. Prefers the last config entry still in the
    /// log, then the snapshot's configuration (its config entry may have been
    /// compacted), then the bootstrap configuration.
    #[must_use]
    pub(crate) fn configuration(&self) -> Configuration {
        let membership = self
            .log
            .last_membership()
            .map(|(_, m)| m)
            .or_else(|| self.snapshot.as_ref().map(|s| &s.membership))
            .unwrap_or(&self.initial);
        Configuration::from_membership(membership)
    }

    /// The committed membership as a wire [`Membership`] value.
    #[must_use]
    pub fn committed_membership(&self) -> trembita_proto::Membership {
        self.configuration().to_membership()
    }

    /// Highest index covered by this node's snapshot (0 if none).
    #[must_use]
    pub fn snapshot_index(&self) -> LogIndex {
        self.log.snapshot_index()
    }

    /// Applied entries not yet compacted into a snapshot.
    #[must_use]
    pub fn compactable_entries(&self) -> u64 {
        self.last_applied
            .0
            .saturating_sub(self.log.snapshot_index().0)
    }

    /// Estimated byte size of applied log entries not yet compacted.
    #[must_use]
    pub fn compactable_log_bytes(&self) -> u64 {
        self.log.bytes_up_to(self.last_applied)
    }

    /// The most recent snapshot this node holds (its boundary, configuration,
    /// and application bytes), or `None` if nothing has been compacted or
    /// installed. A runtime persists this via a `SnapshotStore` after a
    /// [`compact`](RaftNode::compact) or a leader-shipped install so it survives
    /// a restart (backlog A6).
    #[must_use]
    pub fn stored_snapshot(&self) -> Option<SnapshotState> {
        self.snapshot.as_ref().map(|s| SnapshotState {
            last_included: LogId::new(s.last_term, s.last_index),
            membership: s.membership.clone(),
            data: s.data.clone(),
        })
    }
    /// Live log entries from `from` through the last index (inclusive).
    #[must_use]
    pub fn log_entries_from(&self, from: LogIndex) -> Vec<LogEntry> {
        self.log.entries_from(from).to_vec()
    }

    /// The active voting set (sorted).
    #[must_use]
    pub fn voters(&self) -> Vec<NodeId> {
        self.configuration().voters()
    }

    /// Non-voting learners in the committed configuration (sorted).
    #[must_use]
    pub fn learners(&self) -> Vec<NodeId> {
        self.configuration().to_membership().learners
    }

    /// Highest replicated index reported by `peer` on this leader (zero if unknown).
    #[must_use]
    pub fn peer_match_index(&self, peer: NodeId) -> LogIndex {
        self.match_index
            .get(&peer)
            .copied()
            .unwrap_or(LogIndex::ZERO)
    }

    /// Collect `(peer, match_index)` for every configured peer except self.
    #[must_use]
    pub fn peer_match_indices(&self) -> BTreeMap<NodeId, LogIndex> {
        self.configuration()
            .members()
            .into_iter()
            .filter(|id| *id != self.id)
            .map(|id| (id, self.peer_match_index(id)))
            .collect()
    }
    /// Whether the active configuration is a joint (transitional) config.
    #[must_use]
    pub fn is_joint(&self) -> bool {
        self.configuration().is_joint()
    }

    /// Resolved reachability silence window in logical ticks.
    #[must_use]
    pub fn reachability_window_ticks(&self) -> u64 {
        self.config
            .reachability
            .window(self.config.election_timeout_max)
    }

    /// Drain accumulated effects. The runtime calls this after every event.
    #[must_use]
    pub fn take_outputs(&mut self) -> Vec<Output> {
        std::mem::take(&mut self.outbox)
    }

    /// Take the durable state delta accumulated since the previous call, or
    /// `None` if neither the hard state nor the log changed (backlog B4). The
    /// runtime persists the returned [`Persist`] *before* dispatching any
    /// [`Output`] from [`take_outputs`](RaftNode::take_outputs) for the same
    /// step, so a follower never ack's an entry it has not fsync'd and a node
    /// never reveals a vote it has not recorded (Raft §5.1–§5.3).
    #[must_use]
    pub fn take_persist(&mut self) -> Option<Persist> {
        let hard_state_dirty =
            self.current_term != self.persisted_term || self.voted_for != self.persisted_vote;
        let log_from = self.log_dirty_from.take();
        if !hard_state_dirty && log_from.is_none() {
            return None;
        }
        self.persisted_term = self.current_term;
        self.persisted_vote = self.voted_for;
        let (truncate_from, entries) = match log_from {
            // Never touch indices already sealed into a snapshot; clamp to the
            // first live index.
            Some(from) => {
                let from = LogIndex(from.0.max(self.log.snapshot_index().0 + 1));
                (Some(from), self.log.entries_from(from).to_vec())
            }
            None => (None, Vec::new()),
        };
        Some(Persist {
            term: self.current_term,
            voted_for: self.voted_for,
            hard_state_dirty,
            truncate_from,
            entries,
        })
    }
}
