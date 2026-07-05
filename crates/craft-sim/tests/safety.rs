//! Safety-under-faults: random seeds, drops, latency, partitions, and crashes.
//!
//! The [`Cluster`] asserts election safety, commit agreement, and monotonic
//! application on every step, so these tests fail (with a reproducible seed) if
//! any schedule ever violates a Raft safety invariant. Liveness is deliberately
//! *not* asserted here — under adversarial faults the cluster may stall, but it
//! must never become inconsistent.

use craft_sim::{Cluster, Fault};
use proptest::prelude::*;

/// A scheduled network event: `(step, heal?, node)`.
type Event = (u64, bool, u64);

fn run_schedule(nodes: u64, seed: u64, drop_percent: u64, max_latency: u64, events: Vec<Event>) {
    let mut c = Cluster::new(nodes, seed);
    c.set_fault(Fault {
        drop_percent,
        max_latency,
    });

    let mut schedule = events;
    schedule.sort_by_key(|e| e.0);
    let mut next = 0;

    for step in 0..300u64 {
        while next < schedule.len() && schedule[next].0 == step {
            let (_, heal, node) = schedule[next];
            if heal {
                c.heal();
            } else {
                c.isolate(node % nodes + 1);
            }
            next += 1;
        }
        c.run(1);
        if step % 7 == 0 {
            let _ = c.propose(vec![(step & 0xff) as u8]);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(250))]

    /// No schedule of faults may ever break Raft safety invariants.
    #[test]
    fn safety_holds_under_arbitrary_faults(
        seed in any::<u64>(),
        nodes in 3u64..=5,
        drop_percent in 0u64..40,
        max_latency in 1u64..5,
        events in prop::collection::vec((0u64..300, any::<bool>(), 0u64..5), 0..12),
    ) {
        run_schedule(nodes, seed, drop_percent, max_latency, events);
    }
}

#[test]
fn reliable_network_reaches_agreement() {
    // A focused, deterministic sanity case alongside the property test.
    let mut c = Cluster::new(5, 99);
    assert!(c.run_until_leader(400));
    for i in 0..8u8 {
        c.propose(vec![i]);
        c.run(12);
    }
    c.run(120);
    let reference = c.applied(c.leader().unwrap());
    assert_eq!(reference.len(), 8);
    for id in c.ids() {
        assert_eq!(c.applied(id), reference, "node {id} disagrees");
    }
}

#[test]
fn total_partition_stalls_but_stays_safe() {
    // Split a 5-node cluster into two minorities + a singleton: nobody can
    // reach quorum, so no progress — but also no divergence.
    let mut c = Cluster::new(5, 5);
    assert!(c.run_until_leader(300));
    c.propose(vec![1]);
    c.run(30);
    let committed_before = c.committed_count();

    c.partition(&[&[1, 2], &[3, 4], &[5]]);
    for i in 0..10u8 {
        c.propose(vec![50 + i]);
        c.run(20);
    }
    // No group has a majority, so nothing new commits (safety via harness).
    assert_eq!(
        c.committed_count(),
        committed_before,
        "no quorum, no progress"
    );

    c.heal();
    c.run(400);
    assert!(c.committed_count() >= committed_before);
}
