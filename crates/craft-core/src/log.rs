//! In-memory Raft log used by the core FSM.
//!
//! Indices are 1-based; index 0 is the empty-log sentinel with term 0. After
//! compaction (Raft §7) the prefix `1..=snapshot_index` is discarded and
//! summarized by `(snapshot_index, snapshot_term)`; live entries then start at
//! `snapshot_index + 1`. A production node persists these entries through a
//! `craft-storage` adapter, but the core keeps its own view for
//! decision-making (ADR 030).

use craft_proto::{EntryPayload, LogEntry, LogId, LogIndex, Membership, Term};

/// The replicated log. Live `entries[k]` holds absolute index
/// `snapshot_index + 1 + k`; everything at or below `snapshot_index` has been
/// compacted into a snapshot.
#[derive(Debug, Clone, Default)]
pub(crate) struct Log {
    entries: Vec<LogEntry>,
    snapshot_index: LogIndex,
    snapshot_term: Term,
}

impl Log {
    /// Highest index covered by the snapshot (0 when nothing is compacted).
    pub(crate) fn snapshot_index(&self) -> LogIndex {
        self.snapshot_index
    }

    /// Index of the last entry (the snapshot boundary when no live entries).
    pub(crate) fn last_index(&self) -> LogIndex {
        LogIndex(self.snapshot_index.0 + self.entries.len() as u64)
    }

    /// Term of the last entry (snapshot term when there are no live entries).
    pub(crate) fn last_term(&self) -> Term {
        self.entries
            .last()
            .map(|e| e.term)
            .unwrap_or(self.snapshot_term)
    }

    /// Position `(term, index)` of the last entry.
    pub(crate) fn last_id(&self) -> LogId {
        LogId::new(self.last_term(), self.last_index())
    }

    /// Offset of absolute `idx` into `entries`, if it is a live entry.
    fn offset(&self, idx: LogIndex) -> Option<usize> {
        if idx.0 <= self.snapshot_index.0 {
            return None;
        }
        usize::try_from(idx.0 - self.snapshot_index.0 - 1).ok()
    }

    /// Term at `idx`. Returns the snapshot term at the snapshot boundary and
    /// `None` for indices that are compacted away or beyond the log.
    pub(crate) fn term_at(&self, idx: LogIndex) -> Option<Term> {
        if idx == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        if idx.0 < self.snapshot_index.0 {
            return None; // compacted; term no longer known
        }
        self.entries.get(self.offset(idx)?).map(|e| e.term)
    }

    /// Borrow the live entry at `idx`, if present.
    pub(crate) fn get(&self, idx: LogIndex) -> Option<&LogEntry> {
        self.entries.get(self.offset(idx)?)
    }

    /// Append a new entry for `term` carrying `payload`; returns its index.
    pub(crate) fn append(&mut self, term: Term, payload: EntryPayload) -> LogIndex {
        let index = self.last_index().next();
        self.entries.push(LogEntry {
            term,
            index,
            payload,
        });
        index
    }

    /// Push a pre-built entry (index is normalized by the caller).
    pub(crate) fn push_entry(&mut self, entry: LogEntry) {
        self.entries.push(entry);
    }

    /// Delete the entry at `idx` and everything after it. Indices within the
    /// snapshot are immutable and never truncated.
    pub(crate) fn truncate_from(&mut self, idx: LogIndex) {
        if idx.0 <= self.snapshot_index.0 {
            self.entries.clear();
        } else {
            let keep = (idx.0 - self.snapshot_index.0 - 1) as usize;
            self.entries.truncate(keep);
        }
    }

    /// Live entries starting at `idx` (inclusive), as a slice. Indices at or
    /// below the snapshot boundary yield all live entries.
    pub(crate) fn entries_from(&self, idx: LogIndex) -> &[LogEntry] {
        let start = self.offset(idx).unwrap_or(0);
        self.entries.get(start..).unwrap_or(&[])
    }

    /// First index whose entry has `term`, used for conflict backtracking.
    pub(crate) fn first_index_of_term(&self, term: Term) -> Option<LogIndex> {
        self.entries
            .iter()
            .find(|e| e.term == term)
            .map(|e| e.index)
    }

    /// The most recent membership entry among the live entries.
    pub(crate) fn last_membership(&self) -> Option<(LogIndex, &Membership)> {
        self.entries.iter().rev().find_map(|e| match &e.payload {
            EntryPayload::Membership(m) => Some((e.index, m)),
            _ => None,
        })
    }

    /// Compact the log up to and including `up_to`, discarding that prefix.
    /// `up_to` must be a live index (`snapshot_index < up_to <= last_index`).
    pub(crate) fn compact(&mut self, up_to: LogIndex, up_to_term: Term) {
        debug_assert!(up_to.0 > self.snapshot_index.0 && up_to.0 <= self.last_index().0);
        let drop = (up_to.0 - self.snapshot_index.0) as usize;
        self.entries.drain(0..drop.min(self.entries.len()));
        self.snapshot_index = up_to;
        self.snapshot_term = up_to_term;
    }

    /// Reset the log to a snapshot boundary at `(last_index, last_term)` when
    /// installing a leader's snapshot. Retains a matching live suffix if the
    /// entry at `last_index` already has `last_term`; otherwise discards all
    /// entries (Raft §7).
    pub(crate) fn install_snapshot(&mut self, last_index: LogIndex, last_term: Term) {
        if self.term_at(last_index) == Some(last_term) && last_index.0 <= self.last_index().0 {
            if last_index.0 > self.snapshot_index.0 {
                self.compact(last_index, last_term);
            }
        } else {
            self.entries.clear();
            self.snapshot_index = last_index;
            self.snapshot_term = last_term;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(term: u64) -> EntryPayload {
        EntryPayload::Command(vec![term as u8])
    }

    #[test]
    fn empty_log_sentinels() {
        let log = Log::default();
        assert_eq!(log.last_index(), LogIndex(0));
        assert_eq!(log.last_term(), Term::ZERO);
        assert_eq!(log.term_at(LogIndex(0)), Some(Term::ZERO));
        assert_eq!(log.term_at(LogIndex(1)), None);
        assert!(log.get(LogIndex(0)).is_none());
    }

    #[test]
    fn append_assigns_sequential_indices() {
        let mut log = Log::default();
        assert_eq!(log.append(Term(1), cmd(1)), LogIndex(1));
        assert_eq!(log.append(Term(1), cmd(2)), LogIndex(2));
        assert_eq!(log.append(Term(2), cmd(3)), LogIndex(3));
        assert_eq!(log.last_index(), LogIndex(3));
        assert_eq!(log.last_term(), Term(2));
        assert_eq!(log.term_at(LogIndex(2)), Some(Term(1)));
        assert_eq!(log.term_at(LogIndex(3)), Some(Term(2)));
    }

    #[test]
    fn truncate_removes_suffix() {
        let mut log = Log::default();
        for t in 1..=5 {
            log.append(Term(t), cmd(t));
        }
        log.truncate_from(LogIndex(3));
        assert_eq!(log.last_index(), LogIndex(2));
        assert_eq!(log.term_at(LogIndex(3)), None);
    }

    #[test]
    fn truncate_from_zero_clears() {
        let mut log = Log::default();
        log.append(Term(1), cmd(1));
        log.truncate_from(LogIndex(0));
        assert_eq!(log.last_index(), LogIndex(0));
    }

    #[test]
    fn entries_from_slices_correctly() {
        let mut log = Log::default();
        for t in 1..=4 {
            log.append(Term(1), cmd(t));
        }
        assert_eq!(log.entries_from(LogIndex(3)).len(), 2);
        assert_eq!(log.entries_from(LogIndex(1)).len(), 4);
        assert_eq!(log.entries_from(LogIndex(0)).len(), 4);
        assert_eq!(log.entries_from(LogIndex(99)).len(), 0);
    }

    #[test]
    fn first_index_of_term_finds_earliest() {
        let mut log = Log::default();
        log.append(Term(1), cmd(1));
        log.append(Term(1), cmd(2));
        log.append(Term(3), cmd(3));
        assert_eq!(log.first_index_of_term(Term(1)), Some(LogIndex(1)));
        assert_eq!(log.first_index_of_term(Term(3)), Some(LogIndex(3)));
        assert_eq!(log.first_index_of_term(Term(2)), None);
    }

    #[test]
    fn compaction_preserves_index_math() {
        let mut log = Log::default();
        for t in 1..=5 {
            log.append(Term(t), cmd(t));
        }
        log.compact(LogIndex(3), Term(3));
        assert_eq!(log.snapshot_index(), LogIndex(3));
        // Boundary term is retained; the compacted prefix is gone.
        assert_eq!(log.term_at(LogIndex(3)), Some(Term(3)));
        assert_eq!(log.term_at(LogIndex(2)), None, "compacted away");
        // Live entries keep their absolute indices.
        assert_eq!(log.term_at(LogIndex(4)), Some(Term(4)));
        assert_eq!(log.term_at(LogIndex(5)), Some(Term(5)));
        assert_eq!(log.last_index(), LogIndex(5));
        assert_eq!(log.entries_from(LogIndex(4)).len(), 2);
    }

    #[test]
    fn append_after_compaction_continues_indices() {
        let mut log = Log::default();
        for t in 1..=3 {
            log.append(Term(t), cmd(t));
        }
        log.compact(LogIndex(3), Term(3));
        assert_eq!(log.append(Term(4), cmd(4)), LogIndex(4));
        assert_eq!(log.last_index(), LogIndex(4));
    }

    #[test]
    fn install_snapshot_retains_matching_suffix() {
        let mut log = Log::default();
        for t in 1..=5 {
            log.append(Term(1), cmd(t));
        }
        // Snapshot boundary matches our entry at index 3 -> keep 4 and 5.
        log.install_snapshot(LogIndex(3), Term(1));
        assert_eq!(log.snapshot_index(), LogIndex(3));
        assert_eq!(log.last_index(), LogIndex(5));
        assert_eq!(log.term_at(LogIndex(4)), Some(Term(1)));
    }

    #[test]
    fn install_snapshot_discards_on_mismatch() {
        let mut log = Log::default();
        for t in 1..=5 {
            log.append(Term(1), cmd(t));
        }
        // Boundary term disagrees with our log -> discard everything.
        log.install_snapshot(LogIndex(3), Term(2));
        assert_eq!(log.snapshot_index(), LogIndex(3));
        assert_eq!(log.term_at(LogIndex(3)), Some(Term(2)));
        assert_eq!(log.last_index(), LogIndex(3), "all live entries dropped");
        assert!(log.get(LogIndex(4)).is_none());
    }

    #[test]
    fn install_snapshot_beyond_log_resets() {
        let mut log = Log::default();
        log.append(Term(1), cmd(1));
        log.install_snapshot(LogIndex(10), Term(4));
        assert_eq!(log.snapshot_index(), LogIndex(10));
        assert_eq!(log.last_index(), LogIndex(10));
        assert_eq!(log.term_at(LogIndex(10)), Some(Term(4)));
        assert_eq!(log.append(Term(4), cmd(1)), LogIndex(11));
    }
}
