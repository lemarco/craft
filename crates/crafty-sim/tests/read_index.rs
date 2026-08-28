//! Cluster-level `ReadIndex` (linearizable read) scenarios (read-consistency), driven
//! through the deterministic simulator.

use crafty_sim::Cluster;

#[test]
fn read_completes_on_a_healthy_cluster() {
    let mut c = Cluster::new(3, 11);
    assert!(c.run_until_leader(400));

    for i in 0..4u8 {
        c.propose(vec![i]);
        c.run(10);
    }
    let commit = c.commit_index(c.leader().unwrap());

    assert!(c.read_index(1), "leader accepts the read");
    c.run(30);

    let index = c.read_ready(1).expect("read confirmed by a quorum");
    assert!(index >= commit, "read reflects everything committed so far");
    assert!(!c.read_failed(1));
}

#[test]
fn read_does_not_complete_while_leader_is_isolated() {
    let mut c = Cluster::new(5, 21);
    assert!(c.run_until_leader(400));
    c.propose(vec![1]);
    c.run(20);

    // Cut the leader off from the cluster before it can confirm the read.
    let leader = c.leader().unwrap();
    c.isolate(leader);
    assert!(
        c.read_index(2),
        "the isolated leader still accepts the request"
    );
    c.run(60);

    // With no quorum reachable, the read cannot be confirmed by that leader.
    assert!(
        c.read_ready(2).is_none(),
        "an isolated leader must not serve a linearizable read"
    );

    // Once healed, the (possibly new) leader can serve fresh reads.
    c.heal();
    assert!(c.run_until_leader(400));
    assert!(c.read_index(3));
    c.run(80);
    assert!(c.read_ready(3).is_some(), "reads resume after healing");
}
