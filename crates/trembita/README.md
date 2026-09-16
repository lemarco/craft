# trembita

**A distributed Raft + actor framework for Rust: one codebase, N nodes, elastic and self-healing.**

Write your state machine and actors once, then run the *same* binary on as many
nodes as you like. Nodes form a [Raft](https://raft.github.io/) cluster over
HTTP/3 (QUIC + mTLS), replicate a linearizable state machine, and host
supervised actors that can message, spawn, and migrate across the cluster.

This crate is the **facade**: it re-exports the stable public API, so most users
depend only on `trembita`.

```toml
[dependencies]
trembita = { version = "0.5", features = ["http-jobs", "dev-certs"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal"] }
```

## Product quickstart (`TrembitaApp`)

Every process is a QUIC cluster member. Topology comes from `TREMBITA_*` env; domain logic from Rust.

```rust,no_run
use std::time::Duration;
use trembita::{JobOpts, RunOpts, TrembitaApp};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
TrembitaApp::from_env()?
    .jobs([JobOpts::new("jobs", Duration::from_secs(300)).http_enqueue(true)])
    .run(RunOpts::from_env()?)
    .await?;
# Ok(())
# }
```

See [getting-started.md](../../docs/getting-started.md) and runnable [examples/](../../examples/README.md).

## Runtime embedding (`trembita::cluster`)

Handles, enqueue options, saga journals, and TLS helpers: [`trembita::cluster`](src/cluster.rs). Product apps use [`TrembitaApp`](#product-quickstart-trembitaapp) only.

## Features

- `http-jobs` (default) — product HTTP gateway helpers (`GatewayOpts`, `/jobs/*`, `/actors/*`, `/workflows/*`)
- `dev-certs` — ephemeral mTLS for solo local seeds without PEM files
- `redis-store` — Redis [`ActorStateStore`](https://docs.rs/trembita-store-redis) via `trembita::store_redis`
- `external-backlog` — Postgres [`ExternalBacklog`](https://docs.rs/trembita-backlog-postgres) via `trembita::backlog_postgres`
- `domain-outbox` — Postgres [`EventOutboxSource`](https://docs.rs/trembita-events-postgres) via `trembita::events_postgres`

Optional integrations are enabled on the single `trembita` dependency — no separate adapter crates in your `Cargo.toml`:

```toml
trembita = { version = "0.5", features = ["http-jobs", "external-backlog", "redis-store"] }
```

Direct `trembita-*` crate dependencies remain available for advanced use.

Full reference: [facade ADR](../../docs/decisions/facade.md).

## Learn more

- Product showcases: `./scripts/run-example.sh background-jobs` — full index in [examples/README.md](../../examples/README.md).
- The reference runner binary: [`trembita-node`](../trembita-tools) (repo only, not on crates.io).
- Architecture, ADRs, and the wire protocol: [repository docs](https://gitlab.com/lemarco/trembita/-/tree/master/docs)

## License

Dual-licensed under `MIT OR Apache-2.0`.
