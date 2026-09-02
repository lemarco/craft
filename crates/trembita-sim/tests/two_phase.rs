//! Seeded multi-Raft sim: durable 2PC prepare survives partition mid-transaction.

use trembita_core::RaftGroupId;
use trembita_sim::MultiRaftCluster;

fn key_for_group(groups: &[RaftGroupId], target: RaftGroupId) -> Vec<u8> {
    use trembita_core::{ShardRouter, place_shard};
    let router = ShardRouter::new(64);
    for i in 0u64..10_000 {
        let key = format!("2pc-{i}");
        let shard = router.shard_for(key.as_bytes());
        if place_shard(shard, groups) == Some(target) {
            return key.into_bytes();
        }
    }
    panic!("no route key for group {}", target.0);
}

#[test]
fn durable_prepare_survives_partition_during_cross_shard_2pc() {
    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let key_a = key_for_group(&groups, RaftGroupId(0));
    let key_b = key_for_group(&groups, RaftGroupId(1));
    let tx_id = b"sim-partition-2pc".to_vec();

    let mut sim = MultiRaftCluster::new(5, 2, 42);
    assert!(sim.run_until_leaders(400));

    assert!(sim.propose_two_phase_prepare(0, tx_id.clone(), key_a.clone(), b"cmd-a".to_vec(),));
    sim.run(40);
    assert!(sim.group_has_two_phase_prepare(0, &tx_id, &key_a));

    sim.isolate(3);
    sim.run(60);
    let _ = sim.propose_two_phase_prepare(1, tx_id.clone(), key_b.clone(), b"cmd-b".to_vec());
    sim.run(80);

    sim.heal();
    sim.run(120);
    assert!(sim.run_until_leaders(300));

    assert!(sim.propose_two_phase_prepare(1, tx_id.clone(), key_b.clone(), b"cmd-b".to_vec(),));
    sim.run(40);

    assert!(sim.group_has_two_phase_prepare(0, &tx_id, &key_a));
    assert!(sim.group_has_two_phase_prepare(1, &tx_id, &key_b));
}
