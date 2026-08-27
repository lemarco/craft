# Tier 1 multi-Raft advances

**Status:** Accepted  
**Date:** 2026-08-27

## Context

[write-sharding-multi-raft](write-sharding-multi-raft.md) and
[per-group-raft-membership](per-group-raft-membership.md) landed multi-group runtime,
rebalance, migration RPC, and per-group ConfChange fan-out. Tier 1 closes the
next scaling/ops gaps without promising cross-shard atomicity.

## Decision

### 1. Per-group learners (landed)

- `group_learners` + `GroupReplicationTarget` in `craft-core::shard`
- `CraftClusterBuilder::group_learner_factor` (default `0`)
- `NodeHandle::propose_membership(voters, learners)` wired through membership sync

Learners are non-voting catch-up replicas ranked after voters by rendezvous weight.

### 2. Operator shard expansion (landed)

- `MAX_VIRTUAL_SHARDS = 4096` cap on active shard count
- `ShardRouter::expand_shard_count` + `CraftCluster::expand_shard_count` (multi-Raft only)
- **Keys remap** when modulus increases — operators must drain clients first

Primary write scaling remains **add Raft groups** (rendezvous placement); shard
expansion is an explicit, rare operator action.

### 3. Cross-shard batch propose (landed, non-atomic)

- `craft_client::propose_keyed_batch` — sequential keyed proposes
- On failure returns `BatchError::Partial { step, completed, source }`
- Callers implement compensation (saga); no 2PC in this increment

### 4. Multi-Raft observability (landed)

- `GET /introspect/raft-groups` — shard count, RF/LF, hosted groups, per-group status

### Deferred (Tier 2+)

- **Meta-Raft group** for membership — group 0 coordinator remains sufficient
- **Cross-shard atomic transactions** — separate ADR required
- **Stable shard expansion without remapping** — needs fixed virtual shard space migration

## Related

- [write-sharding-multi-raft](write-sharding-multi-raft.md)
- [per-group-raft-membership](per-group-raft-membership.md)
- [future-work-and-risks](future-work-and-risks.md) — R1
