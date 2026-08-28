# crafty-actor

Actor runtime, registry, directory sync, and cluster supervision for
[crafty](https://crates.io/crates/crafty).

This crate ties together consensus (`crafty-core`), storage (`crafty-storage`),
transport (`crafty-net`), and the actor model: `RaftDriver`, local and
cross-node messaging, leader-only supervision, and graceful drain.

Most applications should depend on the [`crafty`](https://crates.io/crates/crafty)
facade rather than this crate directly.

## Documentation

- [docs.rs/crafty-actor](https://docs.rs/crafty-actor)
- [Repository](https://gitlab.com/lemarco/craft)

## License

Dual-licensed under `MIT OR Apache-2.0`.
