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

```toml
[dependencies]
trembita = { path = "../trembita" }   # or crates.io when published
trembita-macros = { path = "../trembita-macros" }
```

```rust
use trembita::{TrembitaApp, RunOpts};
use trembita::cluster::{TrembitaCluster, ResourceProfile};
use trembita_macros::{UserActor, StateMachine};
```

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
