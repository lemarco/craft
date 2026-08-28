//! Group hosting invariants under live-node churn (supervisor rebalance planner).

use std::collections::BTreeSet;

use craft_sim::RebalanceSim;
use proptest::prelude::*;

/// `(step, join?, node_id)` membership event.
type ChurnEvent = (u64, bool, u64);

fn run_churn_schedule(max_nodes: u64, group_count: u32, rf: u32, lf: u32, events: &[ChurnEvent]) {
    let mut sim = RebalanceSim::new(max_nodes, group_count, rf, lf);
    sim.assert_hosting_invariants();

    let mut schedule: Vec<_> = events.to_vec();
    schedule.sort_by_key(|e| e.0);

    let mut live: BTreeSet<u64> = sim.live_nodes().into_iter().collect();
    let min_live = rf.max(1) as usize;

    for (step, join, node) in schedule {
        let _ = step;
        let node = node.max(1).min(max_nodes);
        if join {
            live.insert(node);
        } else if live.len() > min_live {
            live.remove(&node);
        }
        let live_vec: Vec<_> = live.iter().copied().collect();
        sim.set_live_ids(&live_vec);
        sim.assert_hosting_invariants();
    }
}

fn proptest_config() -> ProptestConfig {
    let cases = std::env::var("CRAFT_PROptest_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(32);
    ProptestConfig::with_cases(cases)
}

proptest! {
    #![proptest_config(proptest_config())]

    #[test]
    fn hosting_invariants_hold_under_live_churn(
        events in prop::collection::vec((0u64..40, any::<bool>(), 1u64..=6), 0..16),
    ) {
        run_churn_schedule(6, 8, 3, 1, &events);
    }
}

#[test]
fn rebalance_converges_after_node_join() {
    let mut sim = RebalanceSim::new(5, 6, 3, 1);
    sim.assert_hosting_invariants();

    // Shrink live set then re-add node 5 to force retire/adopt.
    sim.set_live_ids(&[1, 2, 3, 4]);
    sim.assert_hosting_invariants();

    sim.set_live_ids(&[1, 2, 3, 4, 5]);
    sim.assert_hosting_invariants();
    assert!(
        !sim.hosted_groups(5).is_empty(),
        "joining node should adopt some groups"
    );
}

#[test]
fn rebalance_retires_groups_when_node_leaves() {
    let mut sim = RebalanceSim::new(5, 4, 3, 0);
    let before = sim.hosted_groups(5);
    assert!(!before.is_empty(), "node 5 should host groups initially");

    sim.set_live_ids(&[1, 2, 3, 4]);
    sim.assert_hosting_invariants();
    assert!(
        sim.hosted_groups(5).is_empty(),
        "departed node must not retain hosted groups"
    );
}

#[test]
fn learner_hosting_survives_churn() {
    let mut sim = RebalanceSim::new(6, 10, 3, 1);
    sim.assert_hosting_invariants();

    for live in [
        vec![1, 2, 3, 4, 5, 6],
        vec![1, 2, 3, 4, 5],
        vec![1, 2, 3, 4, 5, 6],
        vec![1, 2, 3, 4],
        vec![1, 2, 3, 4, 5, 6],
    ] {
        sim.set_live_ids(&live);
        sim.assert_hosting_invariants();
    }
}
