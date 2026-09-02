//! `soak` — long-running stability harness over the deterministic simulator
//! (backlog T10, testing-strategy).
//!
//! Repeatedly builds fresh clusters with new seeds and drives them hard —
//! proposing commands while injecting isolations/partitions and healing — for a
//! wall-clock budget. The [`Cluster`] checks Raft safety invariants on *every*
//! step, so any violation panics with the exact seed to replay. On a clean run
//! it prints coverage stats (rounds, proposals, commits, throughput).
//!
//! Configure via env:
//!   SOAK_SECS  wall-clock budget in seconds (default 15)
//!   SOAK_NODES cluster size            (default 5)
//!   SOAK_SEED  starting seed           (default 0xC0FFEE)

use std::time::{Duration, Instant};

use trembita_benchmarks::TinyRng;
use trembita_sim::Cluster;

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let budget = Duration::from_secs(env_u64("SOAK_SECS", 15));
    let nodes = env_u64("SOAK_NODES", 5).max(1);
    let mut seed = env_u64("SOAK_SEED", 0x00C0_FFEE);

    println!(
        "soak: {nodes} nodes for {}s (seed base {seed:#x})",
        budget.as_secs()
    );

    let start = Instant::now();
    let mut rounds = 0u64;
    let mut proposals = 0u64;
    let mut commits = 0u64;

    while start.elapsed() < budget {
        let round_seed = seed;
        let mut cl = Cluster::new(nodes, round_seed);
        let mut rng = TinyRng::new(round_seed ^ 0x5DEE_CE66_DEAD_BEEF);

        assert!(
            cl.run_until_leader(2_000),
            "soak: no leader on fresh cluster (seed {round_seed:#x})"
        );

        for step in 0..500u64 {
            // Occasionally perturb the network; invariants must still hold.
            match rng.next_u64() % 24 {
                0 => cl.isolate(1 + rng.next_u64() % nodes),
                1 if nodes >= 3 => {
                    let k = 1 + rng.next_u64() % (nodes - 1);
                    let a: Vec<u64> = (1..=k).collect();
                    let b: Vec<u64> = (k + 1..=nodes).collect();
                    cl.partition(&[&a, &b]);
                }
                2 => cl.heal(),
                _ => {}
            }

            let cmd = (round_seed ^ step).to_le_bytes().to_vec();
            if cl.propose(cmd) {
                proposals += 1;
            }
            cl.run(1 + rng.next_u64() % 4);
        }

        // Heal and let the cluster settle so progress is possible again.
        cl.heal();
        assert!(
            cl.run_until_leader(4_000),
            "soak: cluster did not recover a leader after heal (seed {round_seed:#x})"
        );
        commits += cl.committed_count() as u64;
        rounds += 1;
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
    }

    let secs = start.elapsed().as_secs_f64();
    println!(
        "soak OK: {rounds} rounds, {proposals} proposals, {commits} commits in {secs:.1}s \
         ({:.0} proposals/s) — no invariant violations",
        proposals as f64 / secs
    );
}
