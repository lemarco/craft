# trembita-jobs

Durable job queue port, redb adapter, autoscale, and leader [`QueueService`](https://docs.rs/trembita-jobs/latest/trembita_jobs/struct.QueueService.html) for
[trembita](https://crates.io/crates/trembita).

Covers at-least-once job delivery, leases, scheduling, external backlog feeds, and
workload governors.

Most applications should depend on the [`trembita`](https://crates.io/crates/trembita)
facade rather than this crate directly.

## Documentation

- [docs.rs/trembita-jobs](https://docs.rs/trembita-jobs)
- [Repository](https://gitlab.com/lemarco/trembita)

## License

Dual-licensed under `MIT OR Apache-2.0`.
