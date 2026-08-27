# Scale and performance targets

**Status:** Accepted  
**Date:** 2026-07-05  
**Updated:** 2026-07-05 — one worker per VPS ([one-worker-per-vps](one-worker-per-vps.md))

## Context

Scale design depends on cluster size, throughput, and **how workers map to VPSes**.

## Decision

### Worker ↔ VPS mapping

| Environment | Workers per VPS | Scale mechanism |
|-------------|-----------------|-----------------|
| **Production** | **1** per logical worker name | Add VPSes; `scale_cluster(N)` where `N ≤ node_count` |
| **Development** | Multiple (`--dev-multi-workers`) | `spawn_pool`, `scale_local` on one machine |

Parallelism on a VPS comes from **inside** the single worker (`VpsResources`, internal pools) — not from multiple worker actors on one node.

### Default targets

| Parameter | Default |
|-----------|---------|
| Cluster size | 3–5 VPSes typical; grows with demand |
| Workers cluster-wide | equals VPS count (1:1 in production) |
| Command size | small (<1 KiB) |
| Write throughput | moderate; scale writes by sharding state machines later if needed |
| HTTP/3 | one QUIC conn per peer; batch append 256 |

### Implications

- `scale_cluster(10)` requires **10 live nodes** in production — deploy VPSes first.
- Raft write throughput still ~single-leader; more VPSes = more **worker compute** and fault tolerance, not linear write multiplication on one log.
- Dev mode for laptop testing without N VPSes.

## Related

- [one-worker-per-vps.md](one-worker-per-vps.md)
- [cross-node-actors.md](cross-node-actors.md)
- [architecture.md](../architecture.md)
