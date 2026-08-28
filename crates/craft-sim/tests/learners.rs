//! Learner replicas under partition: quorum without learners, no learner leadership.

use craft_core::Role;
use craft_sim::Cluster;

fn cluster_with_learner(seed: u64) -> Cluster {
    let mut c = Cluster::with_membership(4, &[1, 2, 3], seed);
    assert!(c.run_until_leader(400), "seed {seed}: elect leader");
    assert!(c.propose(vec![1]), "seed {seed}: baseline write");
    c.run(40);
    assert!(
        c.change_membership(&[1, 2, 3], &[4]),
        "seed {seed}: add learner 4"
    );
    c.run(400);
    for id in c.ids() {
        assert_eq!(c.voters(id), vec![1, 2, 3], "node {id} voters");
        assert_eq!(c.learners(id), vec![4], "node {id} learners");
    }
    c
}

#[test]
fn learner_partition_does_not_block_quorum() {
    let mut c = cluster_with_learner(42);
    c.isolate(4);

    let before = c.committed_count();
    for i in 0..6u8 {
        assert!(c.propose(vec![10 + i]), "write {i} with learner isolated");
        c.run(40);
    }
    assert!(
        c.committed_count() > before,
        "voters commit without partitioned learner"
    );
    assert_ne!(c.role(4), Role::Leader, "learner must not become leader");
}

#[test]
fn learner_never_leads_while_isolated() {
    let mut c = cluster_with_learner(7);
    c.isolate(4);
    for _ in 0..600 {
        c.run(1);
        assert_ne!(
            c.role(4),
            Role::Leader,
            "isolated learner must not win an election"
        );
    }
}

#[test]
fn learner_catches_up_after_partition_heals() {
    let mut c = cluster_with_learner(11);
    c.isolate(4);

    for i in 0..4u8 {
        assert!(c.propose(vec![20 + i]));
        c.run(30);
    }
    let leader = c.leader().expect("leader on voter side");
    let target = c.last_applied(leader);

    c.heal();
    c.run(500);

    assert!(
        c.last_applied(4) >= target,
        "learner should catch up after heal (learner={}, leader={})",
        c.last_applied(4).0,
        target.0
    );
    assert_ne!(c.role(4), Role::Leader);
}

#[test]
fn learner_on_minority_side_does_not_expand_quorum() {
    // Voters {1,2,3}, learner {4}. Split 4 alone vs {1,2,3}.
    let mut c = cluster_with_learner(3);
    c.partition(&[&[4], &[1, 2, 3]]);

    let before = c.committed_count();
    for i in 0..5u8 {
        c.propose(vec![30 + i]);
        c.run(30);
    }
    assert!(
        c.committed_count() > before,
        "majority voter partition commits"
    );
    assert_ne!(c.role(4), Role::Leader);

    c.heal();
    c.run(400);
    let reference = c.applied(c.leader().unwrap());
    assert_eq!(
        c.applied(4),
        reference,
        "learner log matches voters after heal"
    );
}
