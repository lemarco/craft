//! `append` — Raft log append throughput (backlog T10).
//!
//! Compares the volatile [`MemoryStorage`] (encode-free, map insert) against the
//! durable [`RedbStorage`] (postcard encode + fsync'd write transaction). Each
//! iteration appends one contiguous batch to a *fresh* store so the measurement
//! is a clean append, not amortized over a growing log.

use craft_benchmarks::command_entries;
use craft_storage::{LogStore, MemoryStorage, RedbStorage};
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

const PAYLOAD: usize = 256;

fn bench_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("append");
    for batch in [1u64, 16, 256] {
        group.throughput(Throughput::Elements(batch));

        group.bench_function(format!("memory/batch_{batch}"), |b| {
            b.iter_batched(
                || (MemoryStorage::new(), command_entries(batch, PAYLOAD)),
                |(mut store, entries)| store.append(black_box(&entries)).unwrap(),
                BatchSize::SmallInput,
            );
        });

        group.bench_function(format!("redb/batch_{batch}"), |b| {
            b.iter_batched(
                || {
                    let dir = tempfile::tempdir().unwrap();
                    let store = RedbStorage::open(dir.path().join("log.redb")).unwrap();
                    // Keep `dir` alive alongside the store for the timed closure.
                    (dir, store, command_entries(batch, PAYLOAD))
                },
                |(_dir, mut store, entries)| store.append(black_box(&entries)).unwrap(),
                BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_append);
criterion_main!(benches);
