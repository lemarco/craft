# trembita-actor

Actor runtime, registry, directory sync, and cluster supervision for
[trembita](https://crates.io/crates/trembita).

This crate ties together consensus (`trembita-core`), storage (`trembita-storage`),
transport (`trembita-net`), and the actor model: `RaftDriver`, local and
cross-node messaging, leader-only supervision, and graceful drain.

Most applications should depend on the [`trembita`](https://crates.io/crates/trembita)
facade rather than this crate directly.

## Documentation

- [docs.rs/trembita-actor](https://docs.rs/trembita-actor)
- [Repository](https://gitlab.com/lemarco/trembita)

## License

Dual-licensed under `MIT OR Apache-2.0`.
