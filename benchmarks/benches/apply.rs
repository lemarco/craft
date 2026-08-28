//! `apply` — consensus propose→commit→apply pipeline throughput (backlog T10).
//!
//! Drives a single-node [`RaftNode`] leader (quorum of one, so a proposal
//! commits and applies immediately) and measures the cost of proposing a batch
//! of commands and draining the resulting [`Output::Apply`] effects — the pure,
//! I/O-free hot path the runtime's applier loop feeds from.

use crafty_core::proto::NodeId;
use crafty_core::{Config, Output, RaftNode};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

/// A fresh single-node leader with its election/no-op outputs drained.
fn leader() -> RaftNode {
    let mut node = RaftNode::new(NodeId(1), [NodeId(1)], Config::default());
    node.campaign();
    let _ = node.take_outputs();
    assert_eq!(
        node.role(),
        crafty_core::Role::Leader,
        "single node must lead"
    );
    node
}

fn bench_apply(c: &mut Criterion) {
    let cmd = vec![0x11u8; 64];
    let mut group = c.benchmark_group("apply");
    for batch in [1u64, 100, 1000] {
        group.throughput(Throughput::Elements(batch));
        group.bench_function(format!("propose_commit_apply/{batch}"), |b| {
            b.iter_batched(
                leader,
                |mut node| {
                    let mut applied = 0u64;
                    for _ in 0..batch {
                        node.propose(cmd.clone()).unwrap();
                        for out in node.take_outputs() {
                            if let Output::Apply(_) = out {
                                applied += 1;
                            }
                        }
                    }
                    black_box(applied)
                },
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_apply);
criterion_main!(benches);
