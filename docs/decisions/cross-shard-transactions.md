# Cross-shard atomic transactions (multi-Raft)

**Status:** Proposed  
**Date:** 2026-08-27

## Context

Multi-Raft routes each keyed write to one Raft group ([write-sharding-multi-raft](write-sharding-multi-raft.md)).
[Tier 1](tier1-multi-raft-advances.md) added **non-atomic**
[`propose_keyed_batch`](../../crates/craft-client/src/batch.rs) — sequential keyed
proposes with `BatchError::Partial` for caller-side compensation.

Applications that need **atomicity across shards** (transfer, inventory reserve +
deduct, idempotent multi-key invariants) currently implement sagas manually.
This ADR scopes a **framework-level** option without blocking Tier 2 catalog /
stable-shard work ([tier2-multi-raft-architecture](tier2-multi-raft-architecture.md)).

## Problem statement

Given keys `k1..kn` potentially mapped to **different Raft groups**, provide an
optional client/facade API such that either:

- all writes commit visible together (strongest), or
- the framework exposes a structured failure with enough context to compensate
  (saga coordinator), without silent partial success.

Linearizable **reads** across shards remain independent `query_keyed` calls —
this ADR covers **writes** only.

## Options considered

### A. Application saga (status quo)

| Pros | Cons |
|------|------|
| No consensus protocol change | Every app reinvents retry/compensation |
| Matches Tier 1 batch semantics | Hard to get right under partitions |

**v1 default** — keep [`propose_keyed_batch`](../../crates/craft-client/src/batch.rs).

### B. Framework saga coordinator (recommended Phase 4 entry)

Craft provides:

- `TransactionPlan` — ordered steps with compensating commands (user-supplied).
- `run_saga(plan)` — executes steps, runs compensators in reverse on failure,
  persists saga state in **group 0 coordinator metadata** or Redis
  ([actor-state-redis](actor-state-redis.md)) for resume.
- Idempotency keys per step (reuse actor deliver dedup where applicable).

| Pros | Cons |
|------|------|
| No cross-group 2PC / no blocking locks | Not serializable atomicity — eventual |
| Fits craft's async actor model | Compensation must be user-defined |

### C. Two-phase commit over Raft groups

Coordinator (leader) **prepare** on each group → **commit** on all or **abort**.

| Pros | Cons |
|------|------|
| True atomic commit boundary | Latency, blocking, coordinator failure modes |
| Familiar 2PC mental model | Requires new per-group prepare log entries + timeout GC |

### D. Percolator / Spanner-style timestamp oracle

Global timestamps + intent locks across groups.

**Rejected for craft v1** — operational complexity, new storage coupling, out of
scope for library-first design.

## Decision (proposed)

1. **Do not** implement 2PC in Tier 2.
2. **Phase 4 default path:** framework **saga coordinator** (option B) with:
   - explicit `SagaStep { key, command, compensate }` types in `craft-client`;
   - durable saga journal (Redis or group-0 side channel);
   - metrics: `saga_completed`, `saga_compensated`, `saga_stuck`.
3. **Optional later increment:** limited **2PC** (option C) for ≤3 groups and
   small payloads, behind `CraftClusterBuilder::cross_shard_2pc(true)` — only if
   saga adoption shows demand.

## Consistency guarantees (target)

| API | Guarantee |
|-----|-----------|
| `propose_keyed_batch` | Sequential; partial failure surfaced |
| `run_saga` (proposed) | All steps committed OR compensators run; at-least-once step delivery with idempotency |
| `propose_cross_shard_2pc` (future) | Atomic commit if all groups ack prepare |

Neither saga nor 2PC provides **global serializable isolation** across shards
without a global transaction manager — document as explicit non-goal.

## Open questions

- Saga journal in group 0 metadata vs Redis-only — affects restart without Redis.
- Whether compensation runs on **same shard** as forward step (key affinity).
- Interaction with dynamic catalog expansion mid-saga (catalog version pin).

## Related

- [tier2-multi-raft-architecture](tier2-multi-raft-architecture.md)
- [tier1-multi-raft-advances](tier1-multi-raft-advances.md)
- [client-routing](client-routing.md)
