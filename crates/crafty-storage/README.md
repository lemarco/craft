# crafty-storage

Durable Raft log, hard state, and snapshot storage for
[crafty](https://crates.io/crates/crafty), backed by
[`redb`](https://crates.io/crates/redb).

Implements the `RaftStorage` port used by the actor runtime. An in-memory
implementation lives in the same crate for tests and simulation.

Most applications should depend on the [`crafty`](https://crates.io/crates/crafty)
facade rather than this crate directly.

## Documentation

- [docs.rs/crafty-storage](https://docs.rs/crafty-storage)
- [Repository](https://gitlab.com/lemarco/craft)

## License

Dual-licensed under `MIT OR Apache-2.0`.
