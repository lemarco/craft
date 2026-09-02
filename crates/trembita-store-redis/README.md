# trembita-store-redis

Redis-backed `ActorStateStore` for stateful
[trembita](https://crates.io/crates/trembita) actors.

Use when actor instances need durable key/value state outside the Raft log
(idempotent workers, session caches, etc.). Supports TLS via the `redis` crate's
Rustls feature set.

Optional integration tests require Docker (`docker-tests` feature; heavy CI lane).

## Documentation

- [docs.rs/trembita-store-redis](https://docs.rs/trembita-store-redis)
- [Repository](https://gitlab.com/lemarco/trembita)

## License

Dual-licensed under `MIT OR Apache-2.0`.
