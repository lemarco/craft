# Tier 2 — multi-Raft architecture (write scaling)

**Status:** Accepted (Phases 1–4 landed)  
**Date:** 2026-08-27

## Context

[Tier 1](tier1-multi-raft-advances.md) landed learners, modulus shard expansion, non-atomic keyed batch, and group introspection. Tier 2 adds **elastic group catalog**, **stable shard routing**, and **cross-shard write coordination** without a separate meta-Raft group.

## Decision

### Phasing

| Phase | Item | Status |
|-------|------|--------|
| **1** | Pure planners (`validate_catalog`, `plan_catalog_expansion`, `StableShardRouter`) | **landed** |
| **2** | Dynamic catalog (`add_raft_groups`, `CatalogCommand::AddGroups` on group 0) | **landed** |
| **3** | Stable shard activation (`activate_shards`, `switch_to_stable_shards`; default router) | **landed** |
| **4** | Cross-shard transactions (saga + optional 2PC) | **landed** — [cross-shard-transactions](cross-shard-transactions.md) |
| **—** | Meta-Raft group | **landed** — [meta-raft](meta-raft.md) |

### Dynamic catalog expansion

- Catalog rules: non-empty, starts at group 0, contiguous ids `0..=max`.
- Leader proposes `CatalogCommand::AddGroups`; all nodes replay → rebalance adopt + membership sync.
- Facade: `CraftCluster::add_raft_groups(count)`; introspect exposes `catalog_version`.

### Stable shard activation

- Fixed virtual space `[0, MAX_VIRTUAL_SHARDS)` (4096); `active_count` grows without remapping keys in the active prefix.
- New clusters default to `StableShardRouter`; existing modulus clusters use `switch_to_stable_shards` (drain keyed clients first).

### Cross-shard atomic transactions

Saga coordinator (`run_saga`, `resume_saga`, `CompositeSagaJournal`) and optional 2PC behind `cross_shard_2pc(true)`. See [cross-shard-transactions](cross-shard-transactions.md).

### Meta-Raft (landed)

Dedicated coordinator Raft group (`META_RAFT_GROUP_ID`) for cluster registry, catalog, and saga journal when `raft_groups > 1`. See [meta-raft](meta-raft.md).

## Consequences

**Positive:** Operators add throughput by adding groups without restart; stable routing avoids remapping on shard growth; phased planners keep runtime changes testable.

**Negative:** Modulus → stable migration requires client drain; catalog expansion moves shard ownership via rebalance migrate RPC.

## Related

- [write-sharding-multi-raft](write-sharding-multi-raft.md)
- [tier1-multi-raft-advances](tier1-multi-raft-advances.md)
- [cross-shard-transactions](cross-shard-transactions.md)
- [per-group-raft-membership](per-group-raft-membership.md)
- [status.md](../status.md)
