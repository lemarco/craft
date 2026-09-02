# trembita-storage

Durable Raft log, hard state, and snapshot storage for
[trembita](https://crates.io/crates/trembita), backed by
[`redb`](https://crates.io/crates/redb).

Implements the `RaftStorage` port used by the actor runtime. An in-memory
implementation lives in the same crate for tests and simulation.

Most applications should depend on the [`trembita`](https://crates.io/crates/trembita)
facade rather than this crate directly.

## Documentation

- [docs.rs/trembita-storage](https://docs.rs/trembita-storage)
- [Repository](https://gitlab.com/lemarco/trembita)

## License

Dual-licensed under `MIT OR Apache-2.0`.
