# Cluster elasticity & worker placement

**Status:** Accepted  
**Date:** 2026-07-05  
**Updated:** 2026-08-28 — merged elastic-cluster, one-worker-per-vps, auto-spawn-on-join, scale-targets, supervisor-leader

## Context

Users deploy the **same app** to multiple VPSes. Actors scale on demand across the cluster ([cross-node-actors](cross-node-actors.md)). Production model: **one worker per VPS**, horizontal scale by adding nodes, framework auto-spawns on join.

## Elastic cluster — VPS join & scaling

| Step | Behavior |
|------|----------|
| Seed node | `JOIN_ADDR` empty; **`--allow-join`** to accept joins |
| Joining node | `JOIN_ADDR=<host:port>` → after membership → **auto workers spawned** |
| Leave | `cluster.leave().await?` → migrate actors, then Raft remove |

```bash
NODE_ID=1 LISTEN_ADDR=0.0.0.0:7443 cargo run -- --allow-join
NODE_ID=2 JOIN_ADDR=vps1:7443 cargo run   # framework spawns workers when joined
```

### On-demand scale scenarios

| User intent | Production |
|-------------|------------|
| Add VPS | Deploy with `JOIN_ADDR` → auto worker on new node |
| Match N VPSes | `scale_cluster(N)` or rely on auto workers = 1 per node |
| Message workers | `registry.cluster("workers")?.send(...)` |

Dev mode (`--dev-multi-workers`): `spawn_pool`, `scale_local` on one machine.

## One worker per VPS (production)

| Mode | How enabled | Max workers per VPS |
|------|-------------|---------------------|
| **Production** (default) | default | **1** |
| **Development** | `--dev-multi-workers` | unlimited |

| API call | Production behavior |
|----------|---------------------|
| `spawn` / `spawn_remote` | OK if no instance of `name` on that node yet |
| `spawn_pool(count > 1)` | **Rejected** — `SpawnError::MultiWorkerDisabled` |
| `scale_local(n > 1)` | **Rejected** |
| `scale_cluster(total)` | At most 1 per live node; `total ≤ live_node_count` |
| Second `spawn` same name on same node | **Rejected** — `SpawnError::WorkerAlreadyOnNode` |

Parallelism on a VPS lives **inside** the single worker via `ResourceProfile::UseAllAvailable` and `VpsResources` — not multiple worker actors.

```rust
CraftyCluster::builder()
    .resource_profile(ResourceProfile::UseAllAvailable)
    .auto_workers([AutoWorkerSpec::new("workers", WorkerConfig::default)])
```

Migration on node leave targets a node **without** an existing worker for that name.

## Auto-spawn on join

**Framework auto-spawns configured workers when a node becomes a cluster member.**

```rust
CraftyCluster::builder()
    .auto_workers([
        AutoWorkerSpec {
            name: "workers",
            factory: |resources| WorkerConfig::with_resources(resources),
        },
    ])
    .spawn()
    .await?;
```

| Event | Framework action |
|-------|------------------|
| Seed node starts | After cluster ready → spawn auto workers on self |
| Node joins | After membership confirmed → spawn auto workers on that node |
| Node leaves | Migrate/stop per cross-node-actors; supervisor may respawn via `scale_cluster` |

**Who triggers:** (1) joining node's local supervisor after Raft reports member; (2) leader supervisor reconciles all nodes on membership change. Both paths idempotent by `(name, node_id, generation)`.

Disable: `.auto_workers([])` and manage manually.

## Scale targets

| Parameter | Default |
|-----------|---------|
| Cluster size | 3–5 VPSes typical |
| Workers cluster-wide | equals VPS count (1:1 in production) |
| Command size | small (<1 KiB) |
| Write throughput | moderate per group; scale via multi-Raft ([multi-raft](multi-raft.md)) |
| HTTP/3 | one QUIC conn per peer; batch append 256 |

`scale_cluster(10)` requires **10 live nodes** in production. More VPSes = more worker compute and fault tolerance, not linear write multiplication on one log.

## Supervisor — leader-only reconciliation

**Only the Raft leader** runs cluster-wide supervisor decisions.

| Action | Who |
|--------|-----|
| Reconcile auto workers on all nodes | **Leader** `ClusterSupervisor` |
| `scale_cluster` placement plan | **Leader** |
| Migration target selection on node leave | **Leader** |
| Local `spawn` / `scale_local` (dev) | **Local node** |
| Execute `POST /actor/spawn` on target | Target node (instructed by leader) |

Non-leaders forward `scale_cluster` and post-join callbacks to leader ([client-and-routing](client-and-routing.md) forward pattern). During election: `503` / retry.

Leader reconciliation is **declarative**: desired state = N auto workers on N nodes; diff vs directory; idempotent spawns. Uses `reachable_nodes()` for liveness-aware planning ([cluster-membership](cluster-membership.md#liveness-vs-membership)).

**Rejected:** every node supervises cluster-wide (split-brain placement risk).

## Consequences

**Positive:** Deploy story: `JOIN_ADDR` + same binary → worker appears; clear 1 VPS = 1 worker ops model; single planner consistent with Raft leadership.

**Negative:** `scale_cluster` cannot exceed node count; dev/prod behavioral split; brief placement unavailability during election; supervisor reconciliation adds complexity.

## Related

- [cluster-membership.md](cluster-membership.md)
- [cross-node-actors.md](cross-node-actors.md)
- [deployment-model.md](deployment-model.md)
- [drain-timeout.md](drain-timeout.md)
