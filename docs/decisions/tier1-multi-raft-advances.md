# Tier 1 multi-Raft advances

**Status:** Accepted (landed)  
**Date:** 2026-08-27

## Context

[write-sharding-multi-raft](write-sharding-multi-raft.md) and [per-group-raft-membership](per-group-raft-membership.md) landed multi-group runtime, rebalance, migration RPC, and per-group membership sync. Tier 1 adds operator and client APIs for the next scaling/ops gaps.

## Decision

### 1. Per-group learners

- `group_learners` + `GroupReplicationTarget` in `craft-core::shard`
- `CraftClusterBuilder::group_learner_factor` (default `0`)
- `NodeHandle::propose_membership(voters, learners)`

Learners are non-voting catch-up replicas ranked after voters by rendezvous weight.

### 2. Operator shard expansion (modulus)

- `ShardRouter::expand_shard_count` + `CraftCluster::expand_shard_count`
- **Keys remap** when modulus increases — drain clients first
- Prefer **add Raft groups** for primary write scaling; use `.modulus_shards()` for legacy clusters

### 3. Cross-shard batch propose (non-atomic)

- `craft_client::propose_keyed_batch` — sequential keyed proposes
- `BatchError::Partial` on failure — callers compensate or use saga

### 4. Multi-Raft observability

- `GET /introspect/raft-groups` — shard count, RF/LF, hosted groups, per-group status, `catalog_version`

## Related

- [tier2-multi-raft-architecture](tier2-multi-raft-architecture.md) — dynamic catalog, stable shards, saga
- [cross-shard-transactions](cross-shard-transactions.md)
- [write-sharding-multi-raft](write-sharding-multi-raft.md)
- [status.md](../status.md)
