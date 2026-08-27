# One worker per VPS (production)

**Status:** Accepted  
**Date:** 2026-07-05

## Context

[cross-node-actors](cross-node-actors.md) allows `spawn_pool`, `scale_local`, and `scale_cluster` to place multiple worker instances on one VPS. The user wants a different **production** model:

- **One worker actor per VPS** — scale by adding VPSes, not by stacking workers on one machine.
- That single worker should **use all resources** on the VPS (CPU, parallelism inside the process).
- **Multiple workers on one machine** is **development only** (local laptop / single-node testing).

## Decision

### Placement modes

| Mode | How enabled | Max workers per VPS (per logical name) |
|------|-------------|----------------------------------------|
| **Production** (default) | default | **1** |
| **Development** | `--dev-multi-workers` or `RAFT_DEV_MULTI_WORKERS=1` | unlimited (user responsibility) |

Production mode is the default for release builds; dev mode is explicit opt-in.

### Enforced rules (production)

| API call | Production behavior |
|----------|---------------------|
| `spawn` / `spawn_remote` | OK if no instance of `name` on that node yet |
| `spawn_pool(count > 1)` | **Rejected** — `SpawnError::MultiWorkerDisabled` |
| `scale_local(name, n)` where `n > 1` | **Rejected** |
| `scale_local(name, 1)` | OK (idempotent) |
| `scale_cluster(name, total)` | `total` = desired **worker count cluster-wide**; **at most 1 per live node**; `total ≤ live_node_count` |
| Second `spawn` same `name` on same node | **Rejected** — `SpawnError::WorkerAlreadyOnNode` |

**Scaling out in production:** deploy another VPS (`JOIN_ADDR`) + `scale_cluster` increases count, or supervisor auto-spawns on new node when it joins.

### `scale_cluster` placement (production)

```
total = 5, live nodes = 3  →  ERROR ScaleError::InsufficientNodes { need: 5, have: 3 }
total = 3, live nodes = 5  →  1 worker on 3 nodes (pick 3 nodes; default lowest node_id)
total = 5, live nodes = 5  →  exactly 1 per node
```

Reconciliation never places a second worker on a node that already has one for that `name`.

### Resource utilization (one worker, full VPS)

The framework does **not** spawn multiple actors to consume cores. Instead, the **single worker** is configured to use the machine:

```rust
CraftCluster::builder()
    .resource_profile(ResourceProfile::UseAllAvailable)  // default in production
    // ...
```

```rust
pub enum ResourceProfile {
    /// Default production: expose VPS capacity to the one worker
    UseAllAvailable,
    /// Cap for dev / tests
    Limited { worker_threads: usize },
}

// Passed into UserActor::Config via framework helper
pub struct VpsResources {
    pub available_parallelism: usize,  // std::thread::available_parallelism
    pub tokio_worker_threads: usize,   // matches available_parallelism
    pub suggested_internal_pool: usize,
}
```

**User responsibility:** `Worker` actor uses `VpsResources` to size internal thread pools, batch workers, or async concurrency — **parallelism lives inside the actor**, not as multiple actor instances on one VPS.

Document in user guide: *production scale = more VPSes; dev scale = `--dev-multi-workers` on one machine.*

### Development mode

```bash
# Laptop: simulate 4 workers on one process
RAFT_DEV_MULTI_WORKERS=1 cargo run -- --dev-multi-workers
```

```rust
registry.spawn_pool::<Worker>("workers", 4, cfg).await?;  // OK in dev only
registry.scale_local("workers", 8).await?;                 // OK in dev only
```

Framework logs warning at startup when dev mode enabled.

### Migration ([cross-node-actors](cross-node-actors.md))

On node leave, migration targets a node **without** an existing worker for that `name`. Never migrates two workers onto one VPS in production.

## API changes

```rust
impl ActorRegistry {
    /// Returns current placement mode
    pub fn placement_mode(&self) -> PlacementMode;
}

pub enum PlacementMode {
    Production,           // 1 worker / VPS
    DevelopmentMulti,   // --dev-multi-workers
}
```

## Consequences

**Positive**

- Clear ops model: 1 VPS = 1 worker unit; horizontal scale only
- Avoids resource contention from multiple worker actors on one OS process
- Matches chain-deploy VPS workflow

**Negative**

- `scale_cluster` cannot exceed node count; must deploy VPS first
- Dev/prod behavioral split requires testing in both modes

## Related

- [cross-node-actors.md](cross-node-actors.md)
- [elastic-cluster.md](elastic-cluster.md)
- [scale-targets.md](scale-targets.md)
