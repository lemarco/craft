# Tier 2 — multi-Raft architecture (write scaling)

**Status:** Accepted (Phase 1 planners + Phase 2 dynamic catalog runtime landed)  
**Date:** 2026-08-27

## Context

[Tier 1 multi-Raft advances](tier1-multi-raft-advances.md) landed learners, operator
shard expansion (with key remapping), non-atomic keyed batch, and group
introspection. Primary write scaling still requires **more Raft groups**, but
today the group **catalog is fixed at process start** (`CraftClusterBuilder::raft_groups`).

Four gaps block elastic write throughput without downtime:

| Gap | Impact |
|-----|--------|
| Static catalog | Cannot add groups without rolling restart |
| Modulo shard expansion | `expand_shard_count` remaps existing keys |
| No cross-shard atomicity | Callers must implement sagas manually |
| Meta-Raft | Group 0 coordinator is sufficient for v1 multi-Raft |

This ADR names the **Tier 2 architecture**, lands testable pure planners (Phase 1),
and sequences runtime work.

## Decision

### Phasing overview

| Phase | Item | Scope | Status |
|-------|------|-------|--------|
| **1** | Pure planners (`craft-core::shard`) | Catalog validation/expansion; stable virtual shard space | **landed** |
| **2** | **Dynamic catalog expansion** | Replicated catalog + leader command + rebalance | **landed** |
| **3** | **Stable shard activation** | `StableShardRouter` in runtime; migration from Tier 1 modulus | **landed** |
| **4** | Cross-shard atomic transactions | Separate ADR — [cross-shard-transactions](cross-shard-transactions.md) | **done** (saga coordinator) |
| **—** | Meta-Raft group | Deferred — group 0 coordinator remains | deferred |

---

### 1. Dynamic catalog expansion (Phase 2 — primary write-scaling path)

**Goal:** Add Raft groups `N, N+1, …` to a live cluster without restart.

**Catalog rules** (pure: [`validate_catalog`](../../crates/craft-core/src/shard.rs)):

- Non-empty, starts at **group 0** (cluster coordinator).
- **Contiguous** ids: `0..=max` with no gaps or duplicates.
- Group 0 always present; groups 1+ are write-shard hosts.

**Expansion** ([`plan_catalog_expansion`](../../crates/craft-core/src/shard.rs)):

- Append the next contiguous ids (`last + 1 …`).
- Rendezvous placement moves ~`1/new_count` shards to each new group (same
  property as Tier 1 `adding_a_group_moves_a_minimal_fraction_of_shards`).
- Existing [`MultiRaftState::rebalance`](../../crates/craft/src/multi_raft.rs) +
  group migrate RPC adopt/retire replicas on physical nodes.

**Runtime design (Phase 2 — landed):**

```
Leader (group 0)
  → POST /raft/v1/cluster/catalog/add (CatalogAddRequest)
  → propose CatalogCommand::AddGroups on group 0 log (EntryPayload::Catalog)
  → all nodes replay → on_catalog_applied → MultiRaftState::apply_catalog_command
  → facts refresher → rebalance adopt + extend_routing_catalog + membership sync
```

- Catalog is **cluster metadata**, not application state — handled by craft
  runtime on group 0 (same boundary as `/cluster/join` membership).
- Facade API: `CraftCluster::add_raft_groups(count)` (leader-only, redirects
  followers via wire RPC).
- `GET /introspect/raft-groups` exposes live `catalog_size` and monotonic
  `catalog_version` (starts at `1`, bumps on catalog add) from the in-memory catalog.

**Safety:**

- Idempotent: re-proposing the same target catalog is a no-op.
- New groups start empty; shard data arrives via rebalance migrate or fresh writes.
- Clients must refresh routing (catalog version in introspect or client poll).

---

### 2. Stable shard expansion without remapping (Phase 3)

**Problem:** Tier 1 [`ShardRouter::shard_for`](../../crates/craft-core/src/shard.rs)
uses `hash(key) % active_count`. Increasing `active_count` **remaps** keys in
`[0, old_count)`.

**Decision:** Fixed virtual space `[0, MAX_VIRTUAL_SHARDS)` (4096):

| Concept | Definition |
|---------|------------|
| Virtual shard | `virtual_shard_for(key) = hash(key) % MAX_VIRTUAL_SHARDS` — **immutable** |
| Active prefix | First `active_count` virtual shards accept traffic |
| Expansion | Increase `active_count` only — activates slots `[from, to)` |

Pure API (Phase 1):

- [`StableShardRouter`](../../crates/craft-core/src/shard.rs) — `shard_for` returns
  `None` for keys landing in inactive virtual slots.
- [`plan_stable_shard_activation`](../../crates/craft-core/src/shard.rs) — rejects shrink.
- Keys whose virtual shard ∈ `[0, from)` keep the same [`place_shard`](../../crates/craft-core/src/shard.rs)
  owner when `active_count` grows (unit-tested).

**Migration from Tier 1 modulus router:**

- New clusters: builder defaults to `StableShardRouter`.
- Existing clusters: [`CraftCluster::switch_to_stable_shards`](../../crates/craft/src/cluster.rs)
  performs a one-time modulus → stable switch at the current active count
  (operator must drain keyed clients first — hash formulas differ).

**Relation to catalog expansion:** orthogonal — catalog adds groups (rendezvous
churn); stable activation adds virtual slots without remapping within a fixed
group set.

---

### 3. Cross-shard atomic transactions (Phase 4 — separate ADR)

**Landed:** saga coordinator in `craft-client` ([`run_saga`](../../crates/craft-client/src/saga.rs)),
facade [`StoreSagaJournal`](../../crates/craft/src/saga.rs). See
[cross-shard-transactions](cross-shard-transactions.md) for guarantees and 2PC deferral.

Tier 1 [`propose_keyed_batch`](../../crates/craft-client/src/batch.rs) remains the
**non-atomic** default for callers that manage compensation themselves.

---

### 4. Meta-Raft group (deferred)

A dedicated meta-group replicating catalog + membership in isolation from group 0
user state is **not required** while:

- Group 0 handles cluster join/leave ([per-group-raft-membership](per-group-raft-membership.md)).
- Catalog commands (Phase 2) ride the group 0 coordinator path.

Revisit if group 0 user SM becomes a bottleneck or needs strict isolation from
cluster metadata.

---

## Consequences

**Positive**

- Names the post-Tier-1 write-scaling path with phased, testable increments.
- Phase 1 planners are pure and CI-friendly before runtime risk.
- Dynamic catalog is the main operator action for throughput (add groups, not
  only expand modulus).

**Negative**

- Two shard routing modes during migration (modulus vs stable virtual).
- Catalog expansion + rebalance still moves ~`1/N` shard **ownership** — clients
  and caches must tolerate brief dual-host windows during migrate RPC.
- Cross-shard atomicity remains unsolved until Phase 4 ADR is implemented.

## Related

- [write-sharding-multi-raft](write-sharding-multi-raft.md)
- [tier1-multi-raft-advances](tier1-multi-raft-advances.md)
- [cross-shard-transactions](cross-shard-transactions.md)
- [per-group-raft-membership](per-group-raft-membership.md)
- [future-work-and-risks](future-work-and-risks.md) — R1
