# Idempotency contract — queues, actor store, external backlog

**Status:** Accepted  
**Date:** 2026-09-15

## Context

Product scenarios combine **at-least-once** delivery (job queue, topic cursors, cross-node cast) with **deduplication** (`dedup_key`, actor workflow keys, external backlog claims). Adapters differ (redb, Postgres, Redis), but operators and application code need one mental model.

## Decision

### Delivery semantics

| Surface | Default | Idempotency hook |
|---------|---------|------------------|
| Job queue | At-least-once lease/ack | `EnqueueOptions::dedup_key` — held while a job exists; released after ack ([background-jobs](../scenarios/background-jobs.md#dedup_key-lifecycle)) |
| Event topics | At-least-once per subscription cursor | Named subscriptions; consumers must tolerate redelivery |
| Actor mailbox | At-least-once with optional durable spool | Handler idempotency via `ActorStateStore` keys / workflow helpers |
| External backlog | Claim + settle | `Settlement::Done { attempts }` must match claim generation ([external-backlog](external-backlog.md)) |
| Raft `propose` / SM | Exactly-once per committed index | Application commands define their own idempotent apply |

### Port rules (adapters)

1. **Dedup keys are advisory at enqueue** — duplicate enqueue with the same key while a job is live returns the existing job id; after ack the key may be reused.
2. **Replicate-before-ack on leader services** — queue, topic, and actor-store mutations replicate to reachable voters before success is returned to clients ([job-queue](job-queue.md), [event-topics](event-topics.md), [actor-state-store](actor-state-store.md)).
3. **External settle is generation-aware** — stale `Done` from an older attempt must not ack the current lease (CF-017).
4. **No global cross-shard serializable isolation** — sagas and 2PC document their guarantees ([multi-raft](multi-raft.md#cross-shard-transactions)).

### Application layout

Business idempotency logic belongs in **`domain/`** (no `trembita` imports). Consumers and actors call into `domain` and use trembita only for wiring ([framework-conventions](framework-conventions.md)). `trembita doctor` enforces the hexagon boundary.

## Consequences

**Positive**

- Scenario docs and adapter behavior stay aligned.
- New ports must document how they participate in dedup / settle.

**Negative**

- Redelivery remains possible everywhere except committed Raft SM entries — handlers must stay idempotent by default.

## Related

- [product-scenarios.md](product-scenarios.md)
- [architecture-style.md](architecture-style.md)
- [framework-conventions.md](framework-conventions.md)
