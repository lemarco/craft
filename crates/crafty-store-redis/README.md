# crafty-store-redis

Redis-backed `ActorStateStore` for stateful
[crafty](https://crates.io/crates/crafty) actors.

Use when actor instances need durable key/value state outside the Raft log
(idempotent workers, session caches, etc.). Supports TLS via the `redis` crate's
Rustls feature set.

Optional integration tests require Docker (`docker-tests` feature; heavy CI lane).

## Documentation

- [docs.rs/crafty-store-redis](https://docs.rs/crafty-store-redis)
- [Repository](https://gitlab.com/lemarco/craft)

## License

Dual-licensed under `MIT OR Apache-2.0`.
