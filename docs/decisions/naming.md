# Project and crate naming

**Status:** Accepted  
**Date:** 2026-07-05  
**Amended:** 2026-07-05 — product name **trembita** (replaces `drafs`)

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
├── trembita/              # facade — what most users depend on
├── trembita-proto/
├── trembita-core/
├── trembita-storage/
├── trembita-net/
├── trembita-actor/
├── trembita-client/
├── trembita-macros/
├── trembita-sim/
└── trembita-node/         # optional reference binary
```

### User `Cargo.toml`

One dependency; optional integrations via [facade features](facade.md):

```toml
[dependencies]
trembita = { version = "0.3", features = ["http-jobs", "dev-certs"] }
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

### Main cluster type

Public builder type renamed to match the product:

```rust
TrembitaCluster::builder()
    .node_id(1)
    .listen("0.0.0.0:7443")
    .spawn()
    .await?;
```

(`RaftCluster` alias may exist temporarily for docs migration; **`TrembitaCluster` is canonical**.)

### Why `trembita`

- Short, memorable, distinct from generic `raft-*`
- Reads as a **framework you trembita applications with** — fits library-first embed model
- Publish-ready prefix `trembita-*` without ecosystem collision

### Rejected

| Option | Why not |
|--------|---------|
| **`raft-*`** | crates.io / tikv collision |
| **`drafs-*`** | Accurate acronym but opaque; superseded by user choice |

## Consequences

- **Positive:** Clear brand; single `trembita` import
- **Negative:** Verify `trembita` availability on crates.io before publish (reserve or use org scope if taken)

## Related

- [architecture.md](../architecture.md)
- [facade.md](facade.md)
