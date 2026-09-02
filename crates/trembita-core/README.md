# trembita-core

Pure Raft consensus state machine for the
[trembita](https://crates.io/crates/trembita) framework — **no I/O**, no async runtime,
no wall clock.

Includes [`kv`](src/kv.rs) — a reference key/value [`StateMachine`](src/state_machine.rs) for tutorials and tests (re-exported by the `trembita` facade as `trembita::kv`).

Consensus is modeled as `RaftInput → (state, RaftOutput)`; side effects are
returned as data and executed by the outer runtime (`trembita-actor`).

Most applications should depend on the [`trembita`](https://crates.io/crates/trembita)
facade rather than this crate directly.

## Documentation

- [docs.rs/trembita-core](https://docs.rs/trembita-core)
- [Repository](https://gitlab.com/lemarco/trembita)

## License

Dual-licensed under `MIT OR Apache-2.0`.
