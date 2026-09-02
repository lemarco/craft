# trembita-runtime

Raft node runtime, actor registry, cross-node messaging, and cluster supervision for
[trembita](https://crates.io/crates/trembita).

Hosts [`spawn_node`](https://docs.rs/trembita-runtime/latest/trembita_runtime/fn.spawn.html),
[`ActorRegistry`](https://docs.rs/trembita-runtime/latest/trembita_runtime/struct.ActorRegistry.html),
and the leader-only [`ClusterSupervisor`](https://docs.rs/trembita-runtime/latest/trembita_runtime/struct.ClusterSupervisor.html).

Most applications should depend on the [`trembita`](https://crates.io/crates/trembita)
facade rather than this crate directly.

## Documentation

- [docs.rs/trembita-runtime](https://docs.rs/trembita-runtime)
- [Repository](https://gitlab.com/lemarco/trembita)

## License

Dual-licensed under `MIT OR Apache-2.0`.
