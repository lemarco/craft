# Multi-Raft write scaling

**Status:** Accepted (landed)  
**Date:** 2026-07-06  
**Updated:** 2026-08-28 — merged write-sharding, modulus/stable routing, meta-Raft, cross-shard transactions, production reliability

## Context

A single Raft group funnels all writes through one leader (risk **R1** in [future-work-and-risks](future-work-and-risks.md)). The scaling answer: partition the keyspace across **multiple independent Raft groups**, each with its own log and leader.

## Core model

**Fixed-shard, rendezvous-placed, multi-group** design:

| Step | Mechanism | Property |
|------|-----------|----------|
| key → shard | `ShardRouter` / `StableShardRouter` | Stable across restarts |
| shard → group | `place_shard` (rendezvous hashing) | ~`1/N` shard movement when groups change |
| group → host | `place_group` / `RaftGroupReconciler` | Leader-owned rebalance on membership change |

```
                       ┌── RaftGroup 0  → driver + log + SM slice
 client write (key) ─▶ ShardRouter ─▶ group ─▶ RaftGroup 1
                       └── RaftGroup 2  …
```

- Each node may host several groups (`ShardedNodeService`).
- Per-group storage: `group-<id>.redb` under `data_dir`.
- Keyed wire: `ProposeKeyed` / `QueryKeyed`; ungrouped traffic hits group 0.
- Rebalance: leader plans via `plan_node_group_rebalance`; retire exports state and calls `POST /cluster/group/migrate`.

| Component | Location |
|-----------|----------|
| Shard planners | `crafty-core::shard` |
| Sharded runtime | `crafty-actor::sharded`, `group_rebalance` |
| Keyed client | `crafty-client` |
| Facade builder | `CraftyClusterBuilder::raft_groups`, `stable_shards`, `data_dir` |

Per-group membership: [cluster-membership](cluster-membership.md#per-group-membership-multi-raft).

## Meta-Raft coordinator

When `raft_groups > 1`, a dedicated **Meta-Raft** group isolates coordinator traffic from user writes:

| Concern | Single-group | Multi-Raft |
|---------|--------------|------------|
| Cluster registry (join/leave) | Group 0 | Meta-Raft |
| Dynamic catalog | Group 0 log | Meta-Raft log |
| Saga journal | Group 0 log | Meta-Raft log |
| User state machine | Group 0 | Group 0 (unchanged) |

- `META_RAFT_GROUP_ID = u32::MAX`; storage: `group-meta.redb`.
- Not in user catalog or keyed shard routing; hosted on every live node.
- Single-group clusters (`raft_groups == 1`): unchanged — group 0 remains coordinator + user SM.

## Modulus routing & keyed batch

| Feature | API |
|---------|-----|
| Per-group learners | `group_learner_factor`, `NodeHandle::propose_membership(voters, learners)` |
| Modulus shard expansion | `ShardRouter::expand_shard_count`, `CraftyCluster::expand_shard_count` — keys remap; prefer add groups |
| Non-atomic keyed batch | `propose_keyed_batch` — sequential; `BatchError::Partial` on failure |
| Observability | `GET /introspect/raft-groups` |

## Stable shards & dynamic catalog

| Phase | Item | Status |
|-------|------|--------|
| **1** | Pure planners (`validate_catalog`, `plan_catalog_expansion`, `StableShardRouter`) | landed |
| **2** | Dynamic catalog (`add_raft_groups`, `CatalogCommand::AddGroups`) | landed |
| **3** | Stable shard activation (default router; `switch_to_stable_shards`) | landed |
| **4** | Cross-shard transactions (saga + optional 2PC) | landed |

### Dynamic catalog

- Catalog rules: non-empty, starts at group 0, contiguous ids `0..=max`.
- Leader proposes `CatalogCommand::AddGroups`; all nodes replay → rebalance adopt + membership sync.
- Facade: `CraftyCluster::add_raft_groups(count)`.

### Stable shards

- Fixed virtual space `[0, MAX_VIRTUAL_SHARDS)` (4096); `active_count` grows without remapping keys in the active prefix.
- New clusters default to `StableShardRouter`; modulus → stable migration requires client drain.

## Cross-shard transactions

Multi-Raft routes each keyed write to one group. For atomicity across shards:

| API | Guarantee |
|-----|-----------|
| `propose_keyed_batch` | Sequential; partial failure surfaced |
| `run_saga` / `resume_saga` | All steps committed OR compensators run; durable journal on Meta-Raft (+ optional Redis mirror) |
| `propose_cross_shard_2pc` | Atomic commit if all groups ack prepare (opt-in via `cross_shard_2pc(true)`) |
| + `durable_cross_shard_2pc(true)` | Prepare/abort in each group's Raft log; replay rebuilds `PrepareStore` |

**Default path:** framework saga coordinator with `SagaStep { key, command, compensate }`, `SagaJournal`, metrics (`crafty_saga_*`). Compensation runs on the **same shard as the forward step**.

Neither saga nor 2PC provides **global serializable isolation** — explicit non-goal.

Rejected for v1: Percolator/Spanner-style global timestamps.

## Production reliability

Six production-oriented capabilities (landed):

| Feature | Implementation |
|---------|----------------|
| Reachability tuning + hysteresis | `ReachabilityConfig`, `CraftyClusterBuilder::reachability()` |
| Phi-accrual detector | `FailureDetectorKind::PhiAccrual` |
| Snapshot backup / restore | `crafty-ops` CLI — local gzip-tar + `s3://` / `gs://` / `file://` via opendal |
| Rolling wire upgrade (N/N−1) | `MIN_COMPATIBLE_PROTOCOL_VERSION` + `protocol_version_compatible()` |
| Admin TLS | `AdminServer::serve_tls`, `CRAFTY_ADMIN_TLS_*` |
| Jepsen-lite gate | `e2e/linearizability.sh` — crafty-sim checker + docker phase |

`app_version` join skew remains **exact match**; only protocol/wire accepts a compatibility band ([cluster-membership](cluster-membership.md#version-skew--hard-reject)).

## Consequences

**Positive:** Write capacity scales with group count; coordinator isolated from user traffic; phased planners testable in isolation; saga/2PC options without blocking catalog work.

**Negative:** Fixed virtual shard space; catalog expansion moves ~`1/N` shard ownership; modulus → stable migration requires drain; multi-Raft nodes host extra Meta-Raft group; backup must include `group-meta.redb`.

## Related

- [cluster-membership.md](cluster-membership.md)
- [cluster-elasticity.md](cluster-elasticity.md) — supervisor leader reconcile
- [client-and-routing.md](client-and-routing.md)
- [status.md](../status.md)
