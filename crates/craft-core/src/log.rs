//! In-memory Raft log used by the core FSM.
//!
//! Indices are 1-based; index 0 is the empty-log sentinel with term 0. A
//! production node persists these entries through a `craft-storage` adapter,
//! but the core keeps its own view for decision-making (ADR 030).

use craft_proto::{EntryPayload, LogEntry, LogIndex, Membership, Term};

/// The replicated log, entry `i` (1-based) stored at `entries[i - 1]`.
#[derive(Debug, Clone, Default)]
pub(crate) struct Log {
    entries: Vec<LogEntry>,
}

impl Log {
    /// Index of the last entry (0 when empty).
    pub(crate) fn last_index(&self) -> LogIndex {
        LogIndex(self.entries.len() as u64)
    }

    /// Term of the last entry (0 when empty).
    pub(crate) fn last_term(&self) -> Term {
        self.entries.last().map(|e| e.term).unwrap_or(Term::ZERO)
    }

    /// Term at `idx`, or `None` if the index is beyond the log. Index 0 is the
    /// sentinel and returns `Some(Term::ZERO)`.
    pub(crate) fn term_at(&self, idx: LogIndex) -> Option<Term> {
        if idx.0 == 0 {
            return Some(Term::ZERO);
        }
        self.entries.get((idx.0 - 1) as usize).map(|e| e.term)
    }

    /// Borrow the entry at `idx` (1-based), if present.
    pub(crate) fn get(&self, idx: LogIndex) -> Option<&LogEntry> {
        if idx.0 == 0 {
            return None;
        }
        self.entries.get((idx.0 - 1) as usize)
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

    /// Delete the entry at `idx` and everything after it.
    pub(crate) fn truncate_from(&mut self, idx: LogIndex) {
        if idx.0 == 0 {
            self.entries.clear();
        } else {
            self.entries.truncate((idx.0 - 1) as usize);
        }
    }

    /// Entries starting at `idx` (inclusive), as a slice.
    pub(crate) fn entries_from(&self, idx: LogIndex) -> &[LogEntry] {
        let start = if idx.0 == 0 { 0 } else { (idx.0 - 1) as usize };
        self.entries.get(start..).unwrap_or(&[])
    }

    /// First index whose entry has `term`, used for conflict backtracking.
    pub(crate) fn first_index_of_term(&self, term: Term) -> Option<LogIndex> {
        self.entries
            .iter()
            .find(|e| e.term == term)
            .map(|e| e.index)
    }

    /// The most recent membership entry in the log (Raft uses the last config
    /// in the log, committed or not) with its index.
    pub(crate) fn last_membership(&self) -> Option<(LogIndex, &Membership)> {
        self.entries.iter().rev().find_map(|e| match &e.payload {
            EntryPayload::Membership(m) => Some((e.index, m)),
            _ => None,
        })
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
}
