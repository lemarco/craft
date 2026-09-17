# trembita

**A distributed Raft runtime for Rust: one codebase, N nodes, elastic and self-healing.**

Product services register **capabilities** (`CapManifest`, `#[cap_handler]`), jobs, and topics on [`TrembitaApp`](../../crates/trembita/src/app/mod.rs). The cluster runs over HTTP/3 (QUIC + mTLS) with embedded redb — no mandatory Redis. Custom **`UserActor`** groups and `/actors/*` HTTP are **advanced** and off by default ([product terminology](../../docs/decisions/product-terminology.md)).

This crate is the **facade**: it re-exports the stable public API, so most users
depend only on `trembita`.

```toml
[dependencies]
trembita = { version = "0.6", features = ["http-jobs", "dev-certs"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal"] }
```

## Product quickstart (`TrembitaApp`)

Every process is a QUIC cluster member. Topology comes from `TREMBITA_*` env; domain logic from Rust.

```rust,no_run
use std::time::Duration;
use trembita::{AppManifest, JobOpts, TrembitaApp};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
TrembitaApp::from_env()?
    .manifest(
        AppManifest::new().jobs([
            JobOpts::new("jobs", Duration::from_secs(300)).http_enqueue(true),
        ]),
    )
    .run()
    .await?;
# Ok(())
# }
```

See [getting-started.md](../../docs/getting-started.md), [env.md](../../docs/env.md), and runnable [examples/](../../examples/README.md).

## Runtime embedding (`trembita::cluster`)

Handles, enqueue options, saga journals, and TLS helpers: [`trembita::cluster`](src/cluster.rs). Prefer **`trembita`** over direct `trembita-assembly`; **`trembita-showcase`** is workspace-only ([facade-layering ADR](../../docs/decisions/facade-layering.md), [public-api ADR](../../docs/decisions/public-api-1.0.md)).

## Features

- `http-jobs` (default) — product HTTP gateway (`GatewayOpts`, `/jobs/*`, `cap_*`, `/workflows/*`; `/actors/*` advanced, default off)
- `dev-certs` — ephemeral mTLS for solo local seeds without PEM files
- `redis-store` — Redis [`ActorStateStore`](https://docs.rs/trembita-store-redis) via `trembita::store_redis`
- `external-backlog` — Postgres [`ExternalBacklog`](https://docs.rs/trembita-backlog-postgres) via `trembita::backlog_postgres`
- `domain-outbox` — Postgres [`EventOutboxSource`](https://docs.rs/trembita-events-postgres) via `trembita::events_postgres`

Optional integrations are enabled on the single `trembita` dependency — no separate adapter crates in your `Cargo.toml`:

```toml
trembita = { version = "0.6", features = ["http-jobs", "external-backlog", "redis-store"] }
```

Direct `trembita-*` crate dependencies remain available for advanced use.

Full reference: [facade ADR](../../docs/decisions/facade.md).

## Learn more

- Product showcases: `./scripts/run-example.sh background-jobs` — full index in [examples/README.md](../../examples/README.md).
- The reference runner binary: [`trembita-node`](../trembita-tools) (repo only, not on crates.io).
- Architecture, ADRs, and the wire protocol: [repository docs](https://gitlab.com/lemarco/trembita/-/tree/master/docs)

## License

Dual-licensed under `MIT OR Apache-2.0`.
