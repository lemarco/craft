//! Export/import helpers for cross-node Raft group migration (write-sharding-multi-raft).

use craft_proto::{
    GroupMigrationBundle, GroupMigrationHardState, GroupMigrationSnapshot,
    GroupMigrationSnapshotMeta, LogIndex,
};

use crate::{HardState, MemoryStorage, RaftStorage, Snapshot, StorageError};

fn hard_state_to_wire(state: &HardState) -> GroupMigrationHardState {
    GroupMigrationHardState {
        current_term: state.current_term,
        voted_for: state.voted_for,
    }
}

fn hard_state_from_wire(state: &GroupMigrationHardState) -> HardState {
    HardState {
        current_term: state.current_term,
        voted_for: state.voted_for,
    }
}

fn snapshot_to_wire(snapshot: &Snapshot) -> GroupMigrationSnapshot {
    GroupMigrationSnapshot {
        meta: GroupMigrationSnapshotMeta {
            last_included: snapshot.meta.last_included,
            membership: snapshot.meta.membership.clone(),
        },
        data: snapshot.data.clone(),
    }
}

fn snapshot_from_wire(snapshot: &GroupMigrationSnapshot) -> Snapshot {
    Snapshot {
        meta: crate::SnapshotMeta {
            last_included: snapshot.meta.last_included,
            membership: snapshot.meta.membership.clone(),
        },
        data: snapshot.data.clone(),
    }
}

/// Export durable Raft state from any storage backend into a wire bundle.
///
/// # Errors
/// Returns [`StorageError`] if any backend read fails.
pub fn export_migration(storage: &dyn RaftStorage) -> Result<GroupMigrationBundle, StorageError> {
    let hard_state = hard_state_to_wire(&storage.load_hard_state()?);
    let snapshot = storage.load_snapshot()?.as_ref().map(snapshot_to_wire);
    let first = storage.first_index()?;
    let log = storage.read_from(first)?;
    let purged_through = LogIndex(first.0.saturating_sub(1));
    Ok(GroupMigrationBundle {
        hard_state,
        purged_through,
        snapshot,
        log,
    })
}

/// Import a migration bundle into a storage backend, replacing prior contents.
///
/// # Errors
/// Returns [`StorageError`] if any backend write fails.
pub fn import_migration(
    storage: &mut dyn RaftStorage,
    bundle: &GroupMigrationBundle,
) -> Result<(), StorageError> {
    storage.save_hard_state(&hard_state_from_wire(&bundle.hard_state))?;
    if let Some(snapshot) = &bundle.snapshot {
        storage.save_snapshot(&snapshot_from_wire(snapshot))?;
    }
    storage.truncate_suffix(LogIndex(1))?;
    if bundle.purged_through.0 > 0 {
        storage.purge_prefix(bundle.purged_through)?;
    }
    if !bundle.log.is_empty() {
        storage.append(&bundle.log)?;
    }
    Ok(())
}

/// Import a migration bundle into an empty [`MemoryStorage`].
///
/// # Errors
/// Returns [`StorageError`] if import fails.
pub fn import_migration_memory(
    storage: &mut MemoryStorage,
    bundle: &GroupMigrationBundle,
) -> Result<(), StorageError> {
    import_migration(storage, bundle)
}

/// Populate a fresh boxed storage backend from a migration bundle.
///
/// # Errors
/// Returns [`StorageError`] if import fails.
pub fn import_migration_boxed(
    bundle: &GroupMigrationBundle,
) -> Result<Box<dyn RaftStorage>, StorageError> {
    let mut storage = MemoryStorage::new();
    import_migration_memory(&mut storage, bundle)?;
    Ok(Box::new(storage))
}

#[cfg(test)]
mod tests {
    use craft_proto::{LogEntry, LogId, LogIndex, Membership, NodeId, Term};

    use super::*;
    use crate::{HardStateStore, LogStore, MemoryStorage, SnapshotStore};

    #[test]
    fn export_import_round_trip_preserves_log_tail() {
        let mut storage = MemoryStorage::new();
        storage
            .save_hard_state(&HardState {
                current_term: Term(3),
                voted_for: Some(NodeId(2)),
            })
            .unwrap();
        storage
            .append(&[LogEntry {
                term: Term(3),
                index: LogIndex(1),
                payload: craft_proto::EntryPayload::Noop,
            }])
            .unwrap();

        let bundle = export_migration(&storage).unwrap();
        let mut restored = MemoryStorage::new();
        import_migration_memory(&mut restored, &bundle).unwrap();

        assert_eq!(
            restored.load_hard_state().unwrap(),
            storage.load_hard_state().unwrap()
        );
        assert_eq!(restored.last_index().unwrap(), LogIndex(1));
        assert_eq!(
            restored.read(LogIndex(1)).unwrap(),
            storage.read(LogIndex(1)).unwrap()
        );
    }

    #[test]
    fn export_import_with_snapshot_boundary() {
        let mut storage = MemoryStorage::new();
        storage
            .save_hard_state(&HardState {
                current_term: Term(5),
                voted_for: None,
            })
            .unwrap();
        let snapshot = Snapshot {
            meta: crate::SnapshotMeta {
                last_included: LogId {
                    term: Term(4),
                    index: LogIndex(10),
                },
                membership: Membership {
                    voters: vec![NodeId(1)],
                    voters_outgoing: vec![],
                    learners: vec![],
                },
            },
            data: b"snap".to_vec(),
        };
        storage.save_snapshot(&snapshot).unwrap();
        storage.purge_prefix(LogIndex(10)).unwrap();
        storage
            .append(&[LogEntry {
                term: Term(5),
                index: LogIndex(11),
                payload: craft_proto::EntryPayload::Noop,
            }])
            .unwrap();

        let bundle = export_migration(&storage).unwrap();
        let mut restored = MemoryStorage::new();
        import_migration_memory(&mut restored, &bundle).unwrap();

        assert_eq!(restored.load_snapshot().unwrap(), Some(snapshot));
        assert_eq!(restored.first_index().unwrap(), LogIndex(11));
        assert_eq!(restored.last_index().unwrap(), LogIndex(11));
    }
}
