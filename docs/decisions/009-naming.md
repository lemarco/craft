# ADR 009: Project and crate naming

**Status:** Accepted  
**Date:** 2026-07-05  
**Amended:** 2026-07-05 — product name **craft** (replaces `drafs`)

## Context

Repository: `distributive_raft_actor_system`. Crates need a unique, memorable namespace on crates.io — avoiding collision with tikv/raft-rs (`raft`, `raft-proto`, …) — and **one obvious dependency** for embedders.

## Decision

**Product name: `craft`.** Prefixed workspace crates **`craft-*`** + facade crate **`craft`**.

| Name | Role |
|------|------|
| **craft** | Product / project name |
| **`craft`** | Primary dependency — re-exports `CraftCluster`, `ClientHandle`, `ActorRegistry`, macros |
| **`craft-*`** | Internal workspace crates |
| **`distributive_raft_actor_system`** | Git repo folder (unchanged) |

### Crate map

```
crates/
├── craft/              # facade — what most users depend on
├── craft-proto/
├── craft-core/
├── craft-storage/
├── craft-net/
├── craft-actor/
├── craft-client/
├── craft-macros/
├── craft-sim/
└── craft-node/         # optional reference binary
```

### User `Cargo.toml`

```toml
[dependencies]
craft = { path = "../craft" }   # or crates.io when published
craft-macros = { path = "../craft-macros" }
```

```rust
use craft::{CraftCluster, ResourceProfile, ClientHandle};
use craft_macros::{UserActor, StateMachine};
```

### Main cluster type

Public builder type renamed to match the product:

```rust
CraftCluster::builder()
    .node_id(1)
    .listen("0.0.0.0:7443")
    .spawn()
    .await?;
```

(`RaftCluster` alias may exist temporarily for docs migration; **`CraftCluster` is canonical**.)

### Why `craft`

- Short, memorable, distinct from generic `raft-*`
- Reads as a **framework you craft applications with** — fits library-first embed model
- Publish-ready prefix `craft-*` without ecosystem collision

### Rejected

| Option | Why not |
|--------|---------|
| **`raft-*`** | crates.io / tikv collision |
| **`drafs-*`** | Accurate acronym but opaque; superseded by user choice |

## Consequences

- **Positive:** Clear brand; single `craft` import
- **Negative:** Verify `craft` availability on crates.io before publish (reserve or use org scope if taken)

## Related

- [architecture.md](../architecture.md)
