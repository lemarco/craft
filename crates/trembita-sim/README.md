# trembita-sim

Deterministic simulation harness for testing
[trembita](https://crates.io/crates/trembita) clusters without real network I/O or
wall-clock sleeps.

Drives the pure Raft core and actor runtime with a virtual clock, seeded RNG,
and partition/injection hooks. Used for correctness tests (election safety,
commit guarantees, rebalance) in CI.

## Documentation

- [docs.rs/trembita-sim](https://docs.rs/trembita-sim)
- [Repository](https://gitlab.com/lemarco/trembita)

## License

Dual-licensed under `MIT OR Apache-2.0`.
