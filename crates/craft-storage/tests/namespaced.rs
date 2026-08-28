//! Per-group storage isolation (multi-Raft, write-sharding-multi-raft).

use craft_storage::proto::{EntryPayload, LogEntry, LogIndex, Term};
use craft_storage::{GroupRedbLayout, HardState, HardStateStore, LogStore, group_redb_path};

#[test]
fn group_redb_files_are_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path();

    assert_eq!(group_redb_path(base, 0), base.join("group-0.redb"));
    assert_eq!(group_redb_path(base, 3), base.join("group-3.redb"));
    assert_eq!(
        group_redb_path(base, u32::MAX),
        base.join("group-meta.redb")
    );
}

#[test]
fn group_redb_layout_isolates_hard_state() {
    let dir = tempfile::tempdir().unwrap();
    let layout = GroupRedbLayout::new(dir.path());

    {
        let mut g0 = layout.open_group(0).unwrap();
        g0.save_hard_state(&HardState {
            current_term: Term(7),
            voted_for: None,
        })
        .unwrap();
    }
    {
        let mut g1 = layout.open_group(1).unwrap();
        g1.save_hard_state(&HardState {
            current_term: Term(9),
            voted_for: None,
        })
        .unwrap();
    }

    let reopened_g0 = layout.open_group(0).unwrap();
    let reopened_g1 = layout.open_group(1).unwrap();
    assert_eq!(reopened_g0.load_hard_state().unwrap().current_term, Term(7));
    assert_eq!(reopened_g1.load_hard_state().unwrap().current_term, Term(9));
}

#[test]
fn group_redb_layout_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let layout = GroupRedbLayout::new(dir.path());
    {
        let mut store = layout.open_group(0).unwrap();
        store
            .append(&[LogEntry {
                term: Term(1),
                index: LogIndex(1),
                payload: EntryPayload::Noop,
            }])
            .unwrap();
    }
    let store = layout.open_group(0).unwrap();
    assert_eq!(store.last_index().unwrap(), LogIndex(1));
}
