# Cross-shard atomic transactions (multi-Raft)

**Status:** Accepted (Phase 4 saga + optional 2PC landed)  
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

## Decision

1. **Do not** require 2PC for Tier 2 — saga remains the default path.
2. **Phase 4 default path (landed):** framework **saga coordinator** (option B) with:
   - explicit `SagaStep { key, command, compensate }` types in `craft-client`;
   - [`run_saga`](../../crates/craft-client/src/saga.rs) + [`SagaJournal`](../../crates/craft-client/src/saga.rs) trait;
   - durable saga journal via [`Group0SagaJournal`](../../crates/craft/src/saga.rs) (group 0 Raft metadata, `EntryPayload::SagaJournal`) with optional Redis mirror via [`CompositeSagaJournal`](../../crates/craft/src/saga.rs) when [`CraftClusterBuilder::actor_state_store`](../../crates/craft/src/builder.rs) is configured; standalone [`StoreSagaJournal`](../../crates/craft/src/saga.rs) remains for external-only stores;
   - [`CraftCluster::saga_journal`](../../crates/craft/src/cluster.rs) — default journal (group 0, composite when Redis configured);
   - metrics helper [`record_saga_metrics`](../../crates/craft/src/saga.rs): `craft_saga_completed_total`, `craft_saga_compensated_total`, `craft_saga_stuck_total`.
3. **Optional increment (landed):** limited **2PC** (option C) for ≤3 groups and
   small payloads, behind [`CraftClusterBuilder::cross_shard_2pc`](../../crates/craft/src/builder.rs)(true):
   - [`TwoPhasePlan`](../../crates/craft-core/src/two_phase.rs) validation in `craft-core`;
   - wire types [`ClientRequest::TwoPhasePrepare/Commit/Abort`](../../crates/craft-proto/src/client.rs);
   - leader in-memory [`PrepareStore`](../../crates/craft-actor/src/two_phase.rs) per Raft group;
   - client API [`propose_cross_shard_2pc`](../../crates/craft-client/src/two_phase.rs) over existing transport.

## Consistency guarantees (target)

| API | Guarantee |
|-----|-----------|
| `propose_keyed_batch` | Sequential; partial failure surfaced |
| `run_saga` | All steps committed OR compensators run; at-least-once step delivery with idempotency |
| `propose_cross_shard_2pc` | Atomic commit if all groups ack prepare (leader memory; cleared on leadership loss) |

Neither saga nor 2PC provides **global serializable isolation** across shards
without a global transaction manager — document as explicit non-goal.

## Remaining design note

- Compensation runs on the **same shard as the forward step** (key affinity on `SagaStep.key`) — callers must supply compensators that target the same key.

## Related

- [tier2-multi-raft-architecture](tier2-multi-raft-architecture.md)
- [tier1-multi-raft-advances](tier1-multi-raft-advances.md)
- [client-routing](client-routing.md)
