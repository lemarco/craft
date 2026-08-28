# crafty-sim

Deterministic simulation harness for testing
[crafty](https://crates.io/crates/crafty) clusters without real network I/O or
wall-clock sleeps.

Drives the pure Raft core and actor runtime with a virtual clock, seeded RNG,
and partition/injection hooks. Used for correctness tests (election safety,
commit guarantees, rebalance) in CI.

## Documentation

- [docs.rs/crafty-sim](https://docs.rs/crafty-sim)
- [Repository](https://gitlab.com/lemarco/craft)

## License

Dual-licensed under `MIT OR Apache-2.0`.
