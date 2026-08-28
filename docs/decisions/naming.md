# Project and crate naming

**Status:** Accepted  
**Date:** 2026-07-05  
**Amended:** 2026-07-05 — product name **crafty** (replaces `drafs`)

## Context

Repository: `distributive_raft_actor_system`. Crates need a unique, memorable namespace on crates.io — avoiding collision with tikv/raft-rs (`raft`, `raft-proto`, …) — and **one obvious dependency** for embedders.

## Decision

**Product name: `crafty`.** Prefixed workspace crates **`crafty-*`** + facade crate **`crafty`**.

| Name | Role |
|------|------|
| **crafty** | Product / project name |
| **`crafty`** | Primary dependency — re-exports `CraftyCluster`, `ClientHandle`, `ActorRegistry`, macros |
| **`crafty-*`** | Internal workspace crates |
| **`distributive_raft_actor_system`** | Git repo folder (unchanged) |

### Crate map

```
crates/
├── crafty/              # facade — what most users depend on
├── crafty-proto/
├── crafty-core/
├── crafty-storage/
├── crafty-net/
├── crafty-actor/
├── crafty-client/
├── crafty-macros/
├── crafty-sim/
└── crafty-node/         # optional reference binary
```

### User `Cargo.toml`

```toml
[dependencies]
crafty = { path = "../crafty" }   # or crates.io when published
crafty-macros = { path = "../crafty-macros" }
```

```rust
use crafty::{CraftyCluster, ResourceProfile, ClientHandle};
use crafty_macros::{UserActor, StateMachine};
```

### Main cluster type

Public builder type renamed to match the product:

```rust
CraftyCluster::builder()
    .node_id(1)
    .listen("0.0.0.0:7443")
    .spawn()
    .await?;
```

(`RaftCluster` alias may exist temporarily for docs migration; **`CraftyCluster` is canonical**.)

### Why `crafty`

- Short, memorable, distinct from generic `raft-*`
- Reads as a **framework you crafty applications with** — fits library-first embed model
- Publish-ready prefix `crafty-*` without ecosystem collision

### Rejected

| Option | Why not |
|--------|---------|
| **`raft-*`** | crates.io / tikv collision |
| **`drafs-*`** | Accurate acronym but opaque; superseded by user choice |

## Consequences

- **Positive:** Clear brand; single `crafty` import
- **Negative:** Verify `crafty` availability on crates.io before publish (reserve or use org scope if taken)

## Related

- [architecture.md](../architecture.md)
