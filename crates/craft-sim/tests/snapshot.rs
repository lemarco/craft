//! Cluster-level snapshot / log-compaction scenario (Raft §7): a follower that
//! falls behind the leader's compaction point must catch up via `InstallSnapshot`.

use craft_sim::Cluster;

#[test]
fn lagging_follower_catches_up_via_snapshot() {
    let mut c = Cluster::new(3, 5);
    assert!(c.run_until_leader(400));
    let leader = c.leader().unwrap();

    // Cut one follower off and commit a batch of commands without it.
    let laggard = c.ids().into_iter().find(|n| *n != leader).unwrap();
    c.isolate(laggard);
    for i in 0..8u8 {
        c.propose(vec![i]);
        c.run(15);
    }

    // Compact the leader's log beyond the laggard's position.
    assert!(c.compact_leader(), "leader compacts applied prefix");
    assert!(c.snapshot_index(leader).0 > 0);

    // Healing forces the leader to bring the laggard up to date; because the
    // needed entries were compacted, that must happen via a snapshot.
    c.heal();
    c.run(500);

    assert!(
        c.snapshot_loaded(laggard).is_some(),
        "laggard installed a leader snapshot"
    );
    let lead = c.leader().expect("a leader still exists");
    assert_eq!(
        c.commit_index(laggard),
        c.commit_index(lead),
        "laggard converged to the leader's commit index"
    );
}
