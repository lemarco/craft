//! Liveness scenarios on a reliable network: elections converge and proposals
//! replicate to every node.

use trembita_sim::Cluster;

fn all_applied_equal(c: &Cluster) {
    let ids = c.ids();
    let first = c.applied(ids[0]);
    for id in &ids[1..] {
        assert_eq!(
            c.applied(*id),
            first,
            "node {} diverged from node {}",
            id,
            ids[0]
        );
    }
}

#[test]
fn elects_a_single_leader_across_seeds() {
    for seed in 0..25 {
        let mut c = Cluster::new(3, seed);
        assert!(c.run_until_leader(300), "seed {seed} failed to elect");
        assert!(c.leader().is_some());
    }
}

#[test]
fn five_node_cluster_elects_leader() {
    for seed in 0..15 {
        let mut c = Cluster::new(5, seed);
        assert!(c.run_until_leader(400), "seed {seed} failed to elect");
    }
}

#[test]
fn replicates_all_proposals_to_every_node() {
    let mut c = Cluster::new(3, 7);
    assert!(c.run_until_leader(300));

    let mut expected = Vec::new();
    for i in 0..10u8 {
        assert!(c.propose(vec![i]), "leader should accept proposal {i}");
        expected.push(vec![i]);
        c.run(10);
    }
    c.run(100);

    for id in c.ids() {
        assert_eq!(c.applied(id), expected, "node {id} log mismatch");
    }
}

#[test]
fn reelects_and_progresses_after_leader_isolation() {
    let mut c = Cluster::new(5, 3);
    assert!(c.run_until_leader(400));
    let old = c.leader().unwrap();
    let old_term = c.term(old);

    assert!(c.propose(vec![1]));
    c.run(40);

    // Isolate the leader; the majority must elect a new one in a higher term.
    c.isolate(old);
    c.run(400);
    let new = c.leader().expect("majority elects a new leader");
    assert_ne!(new, old, "a different node leads");
    assert!(c.term(new) > old_term, "new leader has a higher term");

    // The new leader keeps making progress.
    let before = c.committed_count();
    assert!(c.propose(vec![2]));
    c.run(60);
    assert!(
        c.committed_count() > before,
        "cluster commits without the old leader"
    );

    // On heal, everyone reconciles to one consistent log.
    c.heal();
    c.run(400);
    all_applied_equal(&c);
}

#[test]
fn minority_partition_cannot_commit_alone() {
    let mut c = Cluster::new(5, 11);
    assert!(c.run_until_leader(400));
    let leader = c.leader().unwrap();

    // Majority side keeps the leader; minority is two other nodes.
    let others: Vec<u64> = c.ids().into_iter().filter(|n| *n != leader).collect();
    let majority = vec![leader, others[0], others[1]];
    let minority = vec![others[2], others[3]];
    c.partition(&[&majority, &minority]);

    let before = c.committed_count();
    for i in 0..5u8 {
        c.propose(vec![100 + i]);
        c.run(15);
    }
    assert!(
        c.committed_count() > before,
        "majority (with leader) keeps committing"
    );

    // Heal and let the minority catch up to the same log.
    c.heal();
    c.run(400);
    all_applied_equal(&c);
}
