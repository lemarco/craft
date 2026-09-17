# Project and crate naming

**Status:** Accepted  
**Date:** 2026-07-05  

## Context

Repository: `distributive_raft_actor_system`. Crates need a unique, memorable namespace on crates.io — avoiding collision with tikv/raft-rs (`raft`, `raft-proto`, …) — and **one obvious dependency** for embedders.

## Decision

**Product name: `trembita`.** Prefixed workspace crates **`trembita-*`** + facade crate **`trembita`**.

| Name | Role |
|------|------|
| **trembita** | Product / project name |
| **`trembita`** | Primary dependency — `TrembitaApp` for product apps; `trembita::cluster` for custom SM / low-level control |
| **`trembita-*`** | Internal workspace crates |
| **`distributive_raft_actor_system`** | Git repo folder (unchanged) |

### Crate map

```
crates/
├── trembita/              # facade — what most users depend on (TrembitaApp, product gateway)
├── trembita-assembly/     # crates.io (advanced) — cluster boot, TrembitaClusterBuilder, env merge
├── trembita-showcase/     # workspace-only — maintainer harnesses + showcase bins
├── trembita-proto/
├── trembita-core/
├── trembita-storage/
├── trembita-net/
├── trembita-runtime/      # RaftDriver, actors, supervisor, multi-Raft
├── trembita-jobs/         # JobQueue port + redb adapter
├── trembita-events/       # EventTopic port + redb adapter
├── trembita-actor-store/  # CapStore port (rename → trembita-capstore); facade `trembita::capstore`
├── trembita-client/
├── trembita-macros/
├── trembita-sim/
├── trembita-http/         # product gateway (facade `http-jobs`)
└── trembita-tools/        # reference binaries (`trembita-node`, ops, e2e clients)
```

See [facade-layering](facade-layering.md) for product vs assembly vs showcase boundaries.

### User `Cargo.toml`

One dependency; optional integrations via [facade features](facade.md):

```toml
[dependencies]
trembita = { version = "0.6", features = ["http-jobs", "dev-certs"] }
# optional:
# trembita = { features = ["external-backlog", "redis-store", "domain-outbox"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal"] }
```

```rust
use trembita::{TrembitaApp, RunOpts};
use trembita::cluster::{TrembitaCluster, ResourceProfile};
use trembita_macros::{consumer, consumer_json};
```

Macros are re-exported from `trembita` (`consumer!`, `consumer_json!`); a direct `trembita-macros` dependency is optional.

### Main product entry

Product apps boot via **`TrembitaApp::from_env()`** / **`from_config(AppConfig)`** + **[`AppManifest`](../../crates/trembita/src/app/manifest.rs)** ([env.md](../env.md)).

Use **`trembita::cluster::TrembitaCluster`** for **runtime handles** (queues, leave, client) after boot — not for public cluster assembly. **`TrembitaCluster::builder`** and exported **`TrembitaClusterBuilder`** were removed; custom SM wiring is **`trembita-showcase`** / in-crate **`integration`** only ([public-api-1.0](public-api-1.0.md)).

### Why `trembita`

- Short, memorable, distinct from generic `raft-*`
- Reads as a **framework you trembita applications with** — fits library-first embed model
- Publish-ready prefix `trembita-*` without ecosystem collision

### Rejected

| Option | Why not |
|--------|---------|
| **`raft-*`** | crates.io / tikv collision |
| **`drafs-*`** | Accurate acronym but opaque |

## Consequences

- **Positive:** Clear brand; single `trembita` import
- **Negative:** Verify `trembita` availability on crates.io before publish (reserve or use org scope if taken)

## Related

- [architecture.md](../architecture.md)
- [facade.md](facade.md)
