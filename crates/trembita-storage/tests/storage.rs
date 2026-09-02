//! Store-contract tests.
//!
//! The same contract runs against both [`MemoryStorage`] and [`RedbStorage`] so
//! the in-memory test double provably matches the durable backend. A separate
//! test reopens a `redb` file to prove crash recovery (backlog B3).

use trembita_storage::proto::{EntryPayload, LogEntry, LogId, LogIndex, Membership, NodeId, Term};
use trembita_storage::{
    HardState, HardStateStore, LogStore, MemoryStorage, RedbStorage, Snapshot, SnapshotMeta,
    SnapshotStore, StorageError,
};

fn entry(term: u64, index: u64) -> LogEntry {
    LogEntry {
        term: Term(term),
        index: LogIndex(index),
        payload: EntryPayload::Command(format!("cmd-{index}").into_bytes()),
    }
}

fn sample_snapshot() -> Snapshot {
    Snapshot {
        meta: SnapshotMeta {
            last_included: LogId::new(Term(3), LogIndex(9)),
            membership: Membership {
                voters: vec![NodeId(1), NodeId(2), NodeId(3)],
                voters_outgoing: vec![],
                learners: vec![NodeId(4)],
            },
        },
        data: b"state-machine-image".to_vec(),
    }
}

/// The full behavioural contract every store must satisfy.
fn run_contract<S: LogStore + HardStateStore + SnapshotStore>(store: &mut S) {
    // Fresh store: empty log, default hard state, no snapshot.
    assert_eq!(store.first_index().unwrap(), LogIndex(1));
    assert_eq!(store.last_index().unwrap(), LogIndex(0));
    assert_eq!(store.read(LogIndex(1)).unwrap(), None);
    assert_eq!(store.load_hard_state().unwrap(), HardState::default());
    assert_eq!(store.load_snapshot().unwrap(), None);

    // Hard state round-trips.
    let hs = HardState {
        current_term: Term(5),
        voted_for: Some(NodeId(2)),
    };
    store.save_hard_state(&hs).unwrap();
    assert_eq!(store.load_hard_state().unwrap(), hs);

    // Appending a contiguous batch.
    let empty: [LogEntry; 0] = [];
    store.append(&empty).unwrap(); // empty append is a no-op
    store
        .append(&[
            entry(1, 1),
            entry(1, 2),
            entry(2, 3),
            entry(2, 4),
            entry(3, 5),
        ])
        .unwrap();
    assert_eq!(store.first_index().unwrap(), LogIndex(1));
    assert_eq!(store.last_index().unwrap(), LogIndex(5));
    assert_eq!(store.read(LogIndex(3)).unwrap(), Some(entry(2, 3)));
    assert_eq!(store.read(LogIndex(6)).unwrap(), None);
    assert_eq!(
        store.read_from(LogIndex(3)).unwrap(),
        vec![entry(2, 3), entry(2, 4), entry(3, 5)]
    );

    // A gap is rejected.
    let gap = store.append(&[entry(3, 7)]);
    assert!(matches!(
        gap,
        Err(StorageError::NonContiguous {
            expected: 6,
            got: 7
        })
    ));
    // An internally non-consecutive batch is rejected.
    let jumpy = store.append(&[entry(3, 6), entry(3, 8)]);
    assert!(matches!(
        jumpy,
        Err(StorageError::NonContiguous {
            expected: 7,
            got: 8
        })
    ));

    // Suffix truncation (conflict resolution) drops 4 and 5.
    store.truncate_suffix(LogIndex(4)).unwrap();
    assert_eq!(store.last_index().unwrap(), LogIndex(3));
    assert_eq!(store.read(LogIndex(4)).unwrap(), None);
    // ... and we can re-append the truncated indices with new terms.
    store.append(&[entry(9, 4), entry(9, 5)]).unwrap();
    assert_eq!(store.read(LogIndex(4)).unwrap(), Some(entry(9, 4)));
    assert_eq!(store.last_index().unwrap(), LogIndex(5));

    // Prefix purge (compaction) drops 1 and 2 and advances first_index.
    store.purge_prefix(LogIndex(2)).unwrap();
    assert_eq!(store.first_index().unwrap(), LogIndex(3));
    assert_eq!(store.read(LogIndex(1)).unwrap(), None);
    assert_eq!(store.read(LogIndex(2)).unwrap(), None);
    assert_eq!(store.last_index().unwrap(), LogIndex(5));
    assert_eq!(
        store.read_from(LogIndex(1)).unwrap().first().unwrap().index,
        LogIndex(3)
    );

    // Snapshot round-trips and replaces any previous one.
    store.save_snapshot(&sample_snapshot()).unwrap();
    assert_eq!(store.load_snapshot().unwrap(), Some(sample_snapshot()));

    // Purging the whole log leaves an empty-but-offset log.
    store.purge_prefix(LogIndex(5)).unwrap();
    assert_eq!(store.first_index().unwrap(), LogIndex(6));
    assert_eq!(store.last_index().unwrap(), LogIndex(5));
    assert_eq!(store.read_from(LogIndex(1)).unwrap(), vec![]);
    // Appends must continue past the purge boundary, not restart at 1.
    assert!(matches!(
        store.append(&[entry(9, 1)]),
        Err(StorageError::NonContiguous {
            expected: 6,
            got: 1
        })
    ));
    store.append(&[entry(9, 6)]).unwrap();
    assert_eq!(store.read(LogIndex(6)).unwrap(), Some(entry(9, 6)));
}

#[test]
fn memory_satisfies_contract() {
    let mut store = MemoryStorage::new();
    run_contract(&mut store);
}

#[test]
fn redb_satisfies_contract() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = RedbStorage::open(dir.path().join("contract.redb")).unwrap();
    run_contract(&mut store);
}

#[test]
fn redb_recovers_state_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("recover.redb");

    // First "process lifetime": write, compact, snapshot, then drop (close).
    {
        let mut store = RedbStorage::open(&path).unwrap();
        store
            .save_hard_state(&HardState {
                current_term: Term(7),
                voted_for: Some(NodeId(3)),
            })
            .unwrap();
        store
            .append(&[entry(1, 1), entry(1, 2), entry(2, 3), entry(2, 4)])
            .unwrap();
        store.purge_prefix(LogIndex(2)).unwrap();
        store.save_snapshot(&sample_snapshot()).unwrap();
    }

    // Second "process lifetime": reopen the same file and verify durability.
    let store = RedbStorage::open(&path).unwrap();
    assert_eq!(
        store.load_hard_state().unwrap(),
        HardState {
            current_term: Term(7),
            voted_for: Some(NodeId(3)),
        }
    );
    assert_eq!(store.first_index().unwrap(), LogIndex(3));
    assert_eq!(store.last_index().unwrap(), LogIndex(4));
    assert_eq!(store.read(LogIndex(1)).unwrap(), None);
    assert_eq!(store.read(LogIndex(3)).unwrap(), Some(entry(2, 3)));
    assert_eq!(store.load_snapshot().unwrap(), Some(sample_snapshot()));
}
