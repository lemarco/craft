//! Multi-Raft simulation: shard routing and independent group safety.

use trembita_core::{RaftGroupId, ShardRouter, place_shard};
use trembita_sim::{Fault, MultiRaftCluster};

fn key_for_group(groups: &[RaftGroupId], target: RaftGroupId) -> Vec<u8> {
    let router = ShardRouter::new(64);
    for i in 0u64..10_000 {
        let key = format!("route-{i}");
        let shard = router.shard_for(key.as_bytes());
        if place_shard(shard, groups) == Some(target) {
            return key.into_bytes();
        }
    }
    panic!("no route key for group {}", target.0);
}

#[test]
fn keyed_proposals_route_to_independent_groups() {
    let groups = [RaftGroupId(0), RaftGroupId(1)];
    let key_a = key_for_group(&groups, RaftGroupId(0));
    let key_b = key_for_group(&groups, RaftGroupId(1));

    let mut sim = MultiRaftCluster::new(3, 2, 77);
    assert!(sim.run_until_leaders(400));

    assert!(sim.propose_keyed(&key_a, b"group-a-cmd".to_vec()));
    assert!(sim.propose_keyed(&key_b, b"group-b-cmd".to_vec()));
    sim.run(40);

    assert!(
        sim.group_applied_any(0, b"group-a-cmd"),
        "group 0 should apply keyed write for key_a"
    );
    assert!(
        sim.group_applied_any(1, b"group-b-cmd"),
        "group 1 should apply keyed write for key_b"
    );
    assert!(
        !sim.group_applied_any(0, b"group-b-cmd"),
        "group 0 must not observe group 1 writes"
    );
}

#[test]
fn partitioned_groups_stay_safe_independently() {
    let mut sim = MultiRaftCluster::new(5, 2, 5);
    sim.set_fault(Fault {
        drop_percent: 10,
        max_latency: 3,
    });
    assert!(sim.run_until_leaders(400));

    sim.isolate(1);
    for step in 0..200u64 {
        sim.run(1);
        if step % 11 == 0 {
            let step_byte = u8::try_from(step).expect("test step fits u8");
            let _ = sim.propose_keyed(b"alpha", vec![step_byte]);
            let _ = sim.propose_keyed(b"omega", vec![0x80 | step_byte]);
        }
        if step == 100 {
            sim.heal();
        }
    }
}
