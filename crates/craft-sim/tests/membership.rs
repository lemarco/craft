//! Cluster-level joint-consensus membership scenarios (membership-early) driven through
//! the deterministic simulator, with safety invariants asserted every step.

use craft_sim::Cluster;

fn all_applied_equal(c: &Cluster) {
    let ids = c.ids();
    let first = c.applied(ids[0]);
    for id in &ids[1..] {
        assert_eq!(c.applied(*id), first, "node {id} diverged");
    }
}

#[test]
fn grows_from_three_to_five_nodes() {
    // Two extra node processes exist but start outside the voting set.
    let mut c = Cluster::with_membership(5, &[1, 2, 3], 7);
    assert!(c.run_until_leader(400));

    for i in 0..5u8 {
        c.propose(vec![i]);
        c.run(10);
    }

    assert!(
        c.change_membership(&[1, 2, 3, 4, 5], &[]),
        "leader accepts growth"
    );
    c.run(500);

    for id in c.ids() {
        assert_eq!(
            c.voters(id),
            vec![1, 2, 3, 4, 5],
            "node {id} sees new config"
        );
    }
    all_applied_equal(&c);

    // The enlarged cluster keeps making progress.
    let before = c.committed_count();
    c.propose(vec![99]);
    c.run(60);
    assert!(c.committed_count() > before);
}

#[test]
fn shrinks_and_removes_the_leader() {
    let mut c = Cluster::new(5, 4);
    assert!(c.run_until_leader(400));
    let old_leader = c.leader().unwrap();
    c.propose(vec![1]);
    c.run(30);

    // New configuration excludes the current leader, forcing a step-down.
    let survivors: Vec<u64> = c
        .ids()
        .into_iter()
        .filter(|n| *n != old_leader)
        .take(3)
        .collect();
    assert!(c.change_membership(&survivors, &[]));
    c.run(600);

    let new_leader = c.leader().expect("survivors elect a leader");
    assert!(
        survivors.contains(&new_leader),
        "leader is one of the survivors"
    );
    assert_ne!(new_leader, old_leader, "old leader stepped down");
    for id in &survivors {
        assert_eq!(c.voters(*id), {
            let mut s = survivors.clone();
            s.sort_unstable();
            s
        });
    }

    // Progress continues on the smaller cluster.
    let before = c.committed_count();
    c.propose(vec![2]);
    c.run(60);
    assert!(c.committed_count() > before);
}
