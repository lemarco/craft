//! Automatic log compaction policy (pure; runtime executes [`RaftNode::compact`]).
//!
//! The runtime snapshots applied state and purges the log prefix once either
//! threshold is reached — whichever comes first when both are configured.

use crafty_proto::{EntryPayload, LogEntry, LogIndex};

use crate::RaftNode;

/// Default retained applied entries before auto-compaction (Tier 1 ops).
pub const DEFAULT_COMPACT_ENTRIES: u64 = 1024;

/// Default retained applied log bytes before auto-compaction (~4 MiB).
pub const DEFAULT_COMPACT_BYTES: u64 = 4 * 1024 * 1024;

/// When the runtime should automatically compact the Raft log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPolicy {
    /// Compact when applied entries since the last snapshot reach this count.
    /// `None` disables the entry threshold.
    pub max_entries: Option<u64>,
    /// Compact when applied log bytes since the last snapshot reach this size.
    /// `None` disables the byte threshold.
    pub max_bytes: Option<u64>,
}

impl CompactionPolicy {
    /// Disable automatic compaction (`compact()` remains available manually).
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            max_entries: None,
            max_bytes: None,
        }
    }

    /// Entry and byte thresholds with crafty defaults.
    #[must_use]
    pub fn default_auto() -> Self {
        Self {
            max_entries: Some(DEFAULT_COMPACT_ENTRIES),
            max_bytes: Some(DEFAULT_COMPACT_BYTES),
        }
    }

    /// Compact after `entries` applied log entries beyond the snapshot boundary.
    #[must_use]
    pub fn entries(entries: u64) -> Self {
        Self {
            max_entries: Some(entries),
            max_bytes: None,
        }
    }

    /// Whether every threshold is unset.
    #[must_use]
    pub fn is_disabled(&self) -> bool {
        self.max_entries.is_none() && self.max_bytes.is_none()
    }
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self::default_auto()
    }
}

/// Observed log retention relative to the last snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionStats {
    /// Highest index covered by the current snapshot (0 if none).
    pub snapshot_index: LogIndex,
    /// Highest index applied to the state machine.
    pub last_applied: LogIndex,
    /// Applied entries not yet compacted (`last_applied - snapshot_index`).
    pub compactable_entries: u64,
    /// Estimated byte size of the compactable prefix.
    pub compactable_bytes: u64,
}

/// Collect compaction stats from a live node view.
#[must_use]
pub fn compaction_stats(node: &RaftNode) -> CompactionStats {
    CompactionStats {
        snapshot_index: node.snapshot_index(),
        last_applied: node.last_applied(),
        compactable_entries: node.compactable_entries(),
        compactable_bytes: node.compactable_log_bytes(),
    }
}

/// Whether `policy` says the runtime should run [`RaftNode::compact`] now.
#[must_use]
pub fn should_compact(policy: &CompactionPolicy, stats: &CompactionStats) -> bool {
    if policy.is_disabled() || stats.compactable_entries == 0 {
        return false;
    }
    if policy
        .max_entries
        .is_some_and(|limit| stats.compactable_entries >= limit)
    {
        return true;
    }
    policy
        .max_bytes
        .is_some_and(|limit| stats.compactable_bytes >= limit)
}

/// Rough on-disk size of one log entry for byte-threshold policy.
#[must_use]
pub fn entry_estimated_bytes(entry: &LogEntry) -> u64 {
    const FIXED: u64 = 16; // term + index
    FIXED
        + match &entry.payload {
            EntryPayload::Noop => 1,
            EntryPayload::Command(bytes) => bytes.len() as u64,
            EntryPayload::Membership(m) => {
                ((m.voters.len() + m.voters_outgoing.len() + m.learners.len()) * 8) as u64 + 8
            }
            EntryPayload::Catalog(c) => match c {
                crafty_proto::CatalogCommand::AddGroups { new_groups, .. } => {
                    new_groups.len() as u64 * 4 + 8
                }
            },
            EntryPayload::SagaJournal(c) => c.record.len() as u64 + 32,
            EntryPayload::TwoPhasePrepare(c) => {
                c.tx_id.len() as u64 + c.route_key.len() as u64 + c.command.len() as u64 + 32
            }
            EntryPayload::TwoPhaseAbort(c) => c.tx_id.len() as u64 + c.route_key.len() as u64 + 32,
            EntryPayload::TwoPhaseJournal(c) => c.record.len() as u64 + 32,
            EntryPayload::QueueAutoscalePolicy(c) => {
                c.stream.len() as u64
                    + c.worker
                        .as_ref()
                        .map_or(0, |w| w.worker_group.len() as u64 + 32)
                    + c.membership.as_ref().map_or(0, |_| 32)
                    + 16
            }
        }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;
    use crafty_proto::NodeId;

    fn node(id: u64, members: &[u64]) -> RaftNode {
        RaftNode::new(
            NodeId(id),
            members.iter().copied().map(NodeId),
            Config::default(),
        )
    }

    #[test]
    fn disabled_policy_never_triggers() {
        let n = node(1, &[1]);
        let stats = compaction_stats(&n);
        assert!(!should_compact(&CompactionPolicy::disabled(), &stats));
    }

    #[test]
    fn entry_threshold_triggers() {
        let policy = CompactionPolicy::entries(3);
        assert!(should_compact(
            &policy,
            &CompactionStats {
                snapshot_index: LogIndex(0),
                last_applied: LogIndex(3),
                compactable_entries: 3,
                compactable_bytes: 0,
            }
        ));
        assert!(!should_compact(
            &policy,
            &CompactionStats {
                snapshot_index: LogIndex(0),
                last_applied: LogIndex(2),
                compactable_entries: 2,
                compactable_bytes: 0,
            }
        ));
    }

    #[test]
    fn byte_threshold_triggers() {
        let policy = CompactionPolicy {
            max_entries: None,
            max_bytes: Some(100),
        };
        assert!(should_compact(
            &policy,
            &CompactionStats {
                snapshot_index: LogIndex(1),
                last_applied: LogIndex(2),
                compactable_entries: 1,
                compactable_bytes: 100,
            }
        ));
    }

    #[test]
    fn zero_compactable_never_triggers() {
        let mut n = node(1, &[1]);
        n.campaign();
        let _ = n.take_outputs();
        let stats = compaction_stats(&n);
        assert_eq!(stats.compactable_entries, 1); // no-op applied
        assert!(!should_compact(&CompactionPolicy::entries(1024), &stats));
    }
}
