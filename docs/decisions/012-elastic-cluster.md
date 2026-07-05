# ADR 012: Elastic cluster — incremental VPS join & actor scaling

**Status:** Accepted  
**Date:** 2026-07-05

## Context

[ADR 004](004-deployment-model.md): users deploy the **same app** to multiple VPSes. **Actors scale on demand** across the cluster ([ADR 013](013-cross-node-actors.md)). **Production: one worker per VPS** ([ADR 014](014-one-worker-per-vps.md)). **Auto-spawn workers on join** ([ADR 015](015-auto-spawn-on-join.md)).

## Decision

### Cluster elasticity (VPS join)

| Step | Behavior |
|------|----------|
| Seed node | `JOIN_ADDR` empty; **`--allow-join`** to accept joins |
| Joining node | `JOIN_ADDR=<host:port>` → after membership → **auto workers spawned** |
| Leave | `cluster.leave().await?` → migrate actors, then Raft remove |

```bash
NODE_ID=1 LISTEN_ADDR=0.0.0.0:7443 cargo run -- --allow-join
NODE_ID=2 JOIN_ADDR=vps1:7443 cargo run   # framework spawns "workers" when joined
```

### Application actor scaling

**Production (default):**

```rust
CraftCluster::builder()
    .auto_workers([AutoWorkerSpec::new("workers", WorkerConfig::default)])
    .resource_profile(ResourceProfile::UseAllAvailable)
    .spawn()
    .await?;

// Optional: cluster-wide count = one per live node (reconcile)
cluster.actor_registry().scale_cluster::<Worker>("workers", 3, cfg).await?;

registry.cluster("workers")?.send(WorkerMsg::Process(id)).await?;
```

User **does not** need `spawn` in `main` for declared auto workers ([ADR 015](015-auto-spawn-on-join.md)).

**Development (`--dev-multi-workers`):**

```rust
registry.spawn_pool::<Worker>("workers", 4, cfg).await?;
registry.scale_local("workers", 8).await?;
```

### On-demand scale scenarios

| User intent | Production |
|-------------|------------|
| Add VPS | Deploy with `JOIN_ADDR` → **auto worker** on new node |
| Match N VPSes | `scale_cluster(N)` or rely on auto workers = 1 per node |
| Message workers | `registry.cluster("workers")?.send(...)` |

## Related

- [015-auto-spawn-on-join.md](015-auto-spawn-on-join.md)
- [014-one-worker-per-vps.md](014-one-worker-per-vps.md)
- [013-cross-node-actors.md](013-cross-node-actors.md)
- [004-deployment-model.md](004-deployment-model.md)
