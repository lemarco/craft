use super::RaftNode;
use super::prelude::*;

impl RaftNode {
    // ---- Log mutation (durability-tracked) -------------------------------

    /// Lowest index whose entry changed; `take_persist` emits from here.
    pub(in crate::node) fn mark_log_dirty(&mut self, from: LogIndex) {
        self.log_dirty_from = Some(match self.log_dirty_from {
            Some(cur) if cur.0 <= from.0 => cur,
            _ => from,
        });
    }

    /// Append a fresh entry and record it as dirty for persistence.
    pub(in crate::node) fn log_append(&mut self, term: Term, payload: EntryPayload) -> LogIndex {
        let idx = self.log.append(term, payload);
        self.mark_log_dirty(idx);
        idx
    }

    /// Push a pre-built entry and record it as dirty for persistence.
    pub(in crate::node) fn log_push(&mut self, entry: LogEntry) {
        let idx = entry.index;
        self.log.push_entry(entry);
        self.mark_log_dirty(idx);
    }

    /// Truncate the log suffix and record the cut point as dirty.
    pub(in crate::node) fn log_truncate_from(&mut self, idx: LogIndex) {
        self.log.truncate_from(idx);
        self.mark_log_dirty(idx);
    }

    // ---- Configuration helpers ------------------------------------------

    pub(in crate::node) fn config_index(&self) -> LogIndex {
        self.log
            .last_membership()
            .map_or(LogIndex::ZERO, |(idx, _)| idx)
    }

    pub(in crate::node) fn is_voter(&self, id: NodeId) -> bool {
        self.configuration().is_voter(id)
    }

    pub(in crate::node) fn peers(&self) -> Vec<NodeId> {
        self.configuration().peers(self.id)
    }

    /// Whether `acked` satisfies quorum in the current (possibly joint) config.
    pub(in crate::node) fn quorum_ok(&self, acked: &BTreeSet<NodeId>) -> bool {
        self.configuration().has_quorum(acked)
    }

    pub(in crate::node) fn quorum_of_votes(&self) -> bool {
        let votes = self.votes.clone();
        self.quorum_ok(&votes)
    }
}
