# trembita-events

Durable pub/sub topics, redb adapter, and leader [`TopicService`](https://docs.rs/trembita-events/latest/trembita_events/struct.TopicService.html) for
[trembita](https://crates.io/crates/trembita).

Event topics replicate through Raft and fan out to subscribers with leases and retention.

Most applications should depend on the [`trembita`](https://crates.io/crates/trembita)
facade rather than this crate directly.

## Documentation

- [docs.rs/trembita-events](https://docs.rs/trembita-events)
- [Repository](https://gitlab.com/lemarco/trembita)

## License

Dual-licensed under `MIT OR Apache-2.0`.
