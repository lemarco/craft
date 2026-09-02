# trembita-actor-store

Workflow and idempotency state for stateful actors via [`ActorStateStore`](https://docs.rs/trembita-actor-store/latest/trembita_actor_store/trait.ActorStateStore.html) for
[trembita](https://crates.io/crates/trembita).

The default [`RedbActorStateStore`](https://docs.rs/trembita-actor-store/latest/trembita_actor_store/struct.RedbActorStateStore.html) persists per-actor keys outside the Raft
log. Optional Redis backing lives in [`trembita-store-redis`](https://crates.io/crates/trembita-store-redis).

Most applications should depend on the [`trembita`](https://crates.io/crates/trembita)
facade rather than this crate directly.

## Documentation

- [docs.rs/trembita-actor-store](https://docs.rs/trembita-actor-store)
- [Repository](https://gitlab.com/lemarco/trembita)

## License

Dual-licensed under `MIT OR Apache-2.0`.
