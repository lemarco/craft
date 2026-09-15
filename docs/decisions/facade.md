# Facade crate and Cargo features

**Status:** Accepted  
**Date:** 2026-09-05  

## Context

[trembita](naming.md) is a **library-first framework**: product teams embed one binary, run it on N VPS nodes, and wire domain logic through [`TrembitaApp`](../../crates/trembita/src/app/mod.rs). The workspace splits implementation across many `trembita-*` crates ([architecture-style](architecture-style.md)).

Embedders should not need a mental map of fifteen crate names. They need:

1. **One dependency** in `Cargo.toml` for the product path
2. **Feature flags** for optional integrations (HTTP gateway helpers, Redis store, Postgres adapters)
3. **Stable re-exports** — semver applies to types reachable through `trembita`, not to every internal module

## Decision

### Primary dependency

```toml
[dependencies]
trembita = "0.4"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal"] }
```

Everything below is enabled on that single line via `features = […]`.

### Facade features (`trembita` crate)

| Feature | Default | Pulls in | Rust path |
|---------|---------|----------|-----------|
| `http-jobs` | **yes** | `trembita-http` | Root re-exports (`Gateway`, `RouteTable`, …) and [`trembita::gateway::http`](../../crates/trembita/src/gateway/http/mod.rs) |
| `dev-certs` | no | `trembita-net/dev-certs` | Ephemeral mTLS for local solo seeds |
| `json-wire` | no | `trembita-proto/json-wire` | Dev-only human-readable wire (never production) |
| `redis-store` | no | `trembita-store-redis` | [`trembita::store_redis`](../../crates/trembita/src/lib.rs), `RedisStore`, `RedisTlsConfig` |
| `external-backlog` | no | `trembita-backlog-postgres` | [`trembita::backlog_postgres`](../../crates/trembita/src/lib.rs), `PgBacklog`, … |
| `domain-outbox` | no | `trembita-events-postgres` | [`trembita::events_postgres`](../../crates/trembita/src/lib.rs), `PgEventOutboxSource`, … |
| `full` | no | all optional integrations | docs.rs only (`all-features = true`) |

Core crates (`core`, `jobs`, `events`, `runtime`, `net`, `macros`, …) are **always** linked and re-exported — no feature flag required.

### Typical `Cargo.toml` combinations

**Minimal product app (embedded redb, no Postgres/Redis):**

```toml
trembita = { version = "0.4", features = ["dev-certs"] }
```

**HTTP jobs + gateway product APIs (default):**

```toml
trembita = { version = "0.4", features = ["http-jobs", "dev-certs"] }
```

**Postgres-backed external job backlog:**

```toml
trembita = { version = "0.4", features = ["http-jobs", "external-backlog"] }
```

**Redis actor state + transactional domain outbox:**

```toml
trembita = { version = "0.4", features = ["redis-store", "domain-outbox"] }
```

### Rust imports

```rust
use trembita::prelude::*; // TrembitaApp, opts, consumer!, …

// Always available (core re-exports):
use trembita::{jobs, events, core, net, actor_store};

// With `http-jobs`:
use trembita::{Gateway, RouteTable, GatewayOpts};
use trembita::gateway::http::RequestCtx;

// With `external-backlog`:
use trembita::{PgBacklog, BacklogFeedOpts};

// With `domain-outbox`:
use trembita::{PgEventOutboxSource, EventOutboxDrainOpts};

// With `redis-store`:
use trembita::{RedisStore, store_redis::RedisTlsConfig};
```

Types from optional integrations are **`#[cfg(feature = "...")]`** — enable the matching Cargo feature or the compiler will not see them.

### Scaffolded apps (`trembita new`)

**`trembita new`** generates **app-level** Cargo features that forward to the facade ([framework-conventions](framework-conventions.md)).
Product capabilities (jobs, topics, workers) are registered in **`src/manifest.rs`** via [`AppManifest`](../../crates/trembita/src/app/manifest.rs); `app.rs` calls [`.manifest(manifest::build())`](../../crates/trembita/src/app/builder.rs) and wires HTTP. Scaffolded apps edit marker regions in `manifest.rs` manually; [`trembita doctor`](../../crates/trembita-cli/) validates wiring.

App-level **Cargo** features:

```toml
[dependencies]
trembita = { version = "0.4", features = ["dev-certs", "http-jobs", "external-backlog"] }

[features]
default = ["jobs", "gateway", "telemetry", "external-backlog"]
jobs = []
gateway = []
telemetry = []
external-backlog = ["trembita/external-backlog"]
domain-outbox = ["trembita/domain-outbox"]
```

App features (`jobs`, `gateway`, …) control **generated modules** (`consumers/`, `http/`, …). Facade features control **which optional crates compile in**.

```bash
trembita new my-service --features jobs,gateway,telemetry,external-backlog,domain-outbox
```

### Direct `trembita-*` dependencies

Advanced users may depend on sub-crates directly (custom feature sets, faster compile when not using the full facade, workspace patches). This is supported but **not** the documented product path.

| Use direct dep when… |
|----------------------|
| You need `trembita-http/static-s3` without pulling the full facade |
| You publish a third-party adapter crate that implements a trembita port |
| You are developing inside the trembita workspace |

For product apps: **prefer `trembita` + features**.

### Workspace layout (internal)

```
crates/
├── trembita/                 # facade — what embedders depend on
├── trembita-proto/ …         # always pulled by facade
├── trembita-http/            # optional via http-jobs
├── trembita-store-redis/     # optional via redis-store
├── trembita-backlog-postgres/# optional via external-backlog
├── trembita-events-postgres/ # optional via domain-outbox
├── trembita-cli/             # scaffolding (not a runtime dep)
└── trembita-tools/           # reference binaries (publish = false)
```

## Consequences

**Positive**

- One obvious `Cargo.toml` line; features document the integration surface
- Synchronized versions — no drift between `trembita` and `trembita-http`
- docs.rs builds `full` via `all-features = true`
- CLI scaffold forwards adapter features through the facade, not separate deps

**Negative**

- Compile time grows when many features are enabled (`redis-store` + Postgres adapters + `http-jobs`)
- Disabling `http-jobs` (`default-features = false`) breaks gateway modules that assume `trembita-http` — intentional; most product apps keep the default

## Related

- [naming.md](naming.md) — product name and crate map
- [library-and-publishing.md](library-and-publishing.md) — publish order and semver
- [public-api-1.0.md](public-api-1.0.md) — semver surface on the facade
- [framework-conventions.md](framework-conventions.md) — app-level feature forwarding
- [getting-started.md](../getting-started.md) — tutorial entry point
