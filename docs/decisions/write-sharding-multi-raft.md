# Write sharding / multi-Raft

**Status:** Accepted (landed)  
**Date:** 2026-07-06

## Context

A single Raft group funnels all writes through one leader and one log. Adding nodes improves fault tolerance and actor compute but **not** linear write throughput per group (risk **R1** in [future-work-and-risks](future-work-and-risks.md)).

The scaling answer is to partition the keyspace across **multiple independent Raft groups**. Each group replicates its own shard of state and elects its own leader.

## Decision

**Fixed-shard, rendezvous-placed, multi-group** design:

| Step | Mechanism | Property |
|------|-----------|----------|
| key → shard | `ShardRouter` / `StableShardRouter` | Stable across restarts |
| shard → group | `place_shard` (rendezvous hashing) | ~`1/N` shard movement when groups change |
| group → host | `place_group` / `RaftGroupReconciler` | Leader-owned rebalance on membership change |

### Runtime model

```
                       ┌── RaftGroup 0  → driver + log + SM slice
 client write (key) ─▶ ShardRouter ─▶ group ─▶ RaftGroup 1
                       └── RaftGroup 2  …
```

- Each node may host several groups (`spawn_multi_raft_node`, `ShardedNodeService`).
- `GroupTransport` multiplexes peer RPCs by group on one UDP socket.
- Per-group storage: `GroupRedbLayout` / `group-<id>.redb` under `data_dir`.
- Keyed wire: `ProposeKeyed` / `QueryKeyed`; ungrouped traffic hits group 0.
- Rebalance: leader plans via `plan_node_group_rebalance`; retire exports state and calls `POST /cluster/group/migrate`; adopt via `spawn_raft_group_from_bundle`.

### Cross-shard writes

Single-shard writes are the default per `propose`. Multi-shard coordination: [cross-shard-transactions](cross-shard-transactions.md) (saga + optional 2PC).

### Membership

Per-group joint consensus ([per-group-raft-membership](per-group-raft-membership.md)). Group 0 is the cluster coordinator for join/leave and catalog commands ([tier2-multi-raft-architecture](tier2-multi-raft-architecture.md)).

## Implementation

| Component | Location |
|-----------|----------|
| Shard planners | `craft-core::shard` |
| Sharded runtime | `craft-actor::sharded`, `group_rebalance` |
| Keyed client | `craft-client` |
| Facade builder | `CraftClusterBuilder::raft_groups`, `stable_shards`, `data_dir` |
| Migration bundle | `craft-storage::migration`, `RaftDriver::export_migration` |

## Consequences

**Positive:** Write capacity scales with group count; rendezvous minimizes churn; pure planners are testable in isolation.

**Negative:** Fixed virtual shard space trades repartitioning flexibility for routing simplicity; catalog expansion and rebalance still move ~`1/N` shard ownership — clients must tolerate brief dual-host windows during migrate.

## Related

- [per-group-raft-membership](per-group-raft-membership.md)
- [tier1-multi-raft-advances](tier1-multi-raft-advances.md)
- [tier2-multi-raft-architecture](tier2-multi-raft-architecture.md)
- [cross-shard-transactions](cross-shard-transactions.md)
- [supervisor-leader](supervisor-leader.md)
- [status.md](../status.md)
