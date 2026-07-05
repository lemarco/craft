# ADR 007: Cluster discovery

**Status:** Accepted  
**Date:** 2026-07-05  
**Updated:** 2026-07-05 — full joint-consensus membership early ([ADR 016](016-membership-early.md))

## Context

Nodes must find peers to form and grow a cluster. Deployment model: **incremental VPS join** — seed first, then `JOIN_ADDR` for each new VPS.

## Decision

**Join-address bootstrap + Raft-persisted membership (joint consensus in v1).**

| Mechanism | Purpose |
|-----------|---------|
| **`JOIN_ADDR` (optional env/CLI)** | First contact for a **new** VPS joining the cluster |
| **Seed mode** | No `JOIN_ADDR` → single-node cluster; **`--allow-join` required** to accept joins |
| **Raft cluster config** | Authoritative peer list — **joint-consensus membership changes** ([ADR 016](016-membership-early.md)) |
| **Static peer files** | Optional for air-gapped templates only; not the primary path |

### Join flow

0. Target member must have **`--allow-join`** / `RAFT_ALLOW_JOIN=1`.
1. Joining node resolves `JOIN_ADDR` (HTTP/3 + mTLS).
2. Joining node presents `NODE_ID`, listen address, cert via `POST /raft/v1/cluster/join`.
3. Leader proposes **joint-consensus add**; change commits through Raft log.
4. All nodes update peer map; QUIC pools connect; [ADR 015](015-auto-spawn-on-join.md) spawns auto workers.

### No gossip in v1

Gossip / cloud metadata deferred. VPS users pass `JOIN_ADDR` explicitly.

## Consequences

- **Positive:** Simple ops (`JOIN_ADDR`) with **correct** dynamic membership
- **Negative:** Seed address stability; join retries during election; membership complexity in core

## Related

- [016-membership-early.md](016-membership-early.md)
- [012-elastic-cluster.md](012-elastic-cluster.md)
- [004-deployment-model.md](004-deployment-model.md)
- [006-security.md](006-security.md)
