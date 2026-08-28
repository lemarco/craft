# crafty-core

Pure Raft consensus state machine for the
[crafty](https://crates.io/crates/crafty) framework — **no I/O**, no async runtime,
no wall clock.

Consensus is modeled as `RaftInput → (state, RaftOutput)`; side effects are
returned as data and executed by the outer runtime (`crafty-actor`).

Most applications should depend on the [`crafty`](https://crates.io/crates/crafty)
facade rather than this crate directly.

## Documentation

- [docs.rs/crafty-core](https://docs.rs/crafty-core)
- [Repository](https://gitlab.com/lemarco/craft)

## License

Dual-licensed under `MIT OR Apache-2.0`.
