# Stateful actors — workflow store (`ActorStateStore`, redb-first)

**Status:** Accepted  
**Date:** 2026-08-28  
**Supersedes (default path):** [actor-state-redis](actor-state-redis.md) — Redis remains an optional adapter, not the product default.

## Context

Medium open question **#2** ([cross-node-actors](cross-node-actors.md)): what happens to **stateful actor** workflow data on VPS crash?

The original ADR recommended **Redis** as the primary example. Product direction (2026-08-28) is **zero mandatory external infra**: the same embedded `redb` model used for [job-queue](job-queue.md) and Raft storage should cover actor workflow keys for teams that run entirely on trembita.

## Decision

### Two layers of state (unchanged)

| Layer | Store | Purpose |
|-------|--------|---------|
| **Authoritative / consensus** | Raft → `StateMachine` | Orders, balances, config — linearizable and replicated |
| **Actor workflow** | [`ActorStateStore`](../../crates/trembita-actor/src/store.rs) | Session progress, idempotency keys, locks, handler caches — survives crash when backed by durable store |

**Do not** put routine actor workflow bytes in the Raft log — avoids R1 write ceiling and wrong abstraction ([future-work-and-risks](future-work-and-risks.md)).

### Default backend: embedded redb (target)

| Backend | Role | Status |
|---------|------|--------|
| **`InMemoryStore`** | Tests, single-node dev | **shipped** |
| **`RedbActorStateStore`** | Production, `{data_dir}/actor-store.redb`, voter replication like `RedbJobQueue` | **shipped** (0.2.x) |
| **`RedisStore`** (`trembita-store-redis`) | Optional — shared cache with non-trembita services | **shipped** |

Product getting started and [scenario guides](../scenarios/README.md) use **redb or SM only**. Redis is documented under optional integration.

### When to use which layer

| Data | Put it in |
|------|-----------|
| Business entity the product audits | `StateMachine` via `propose` |
| Job payload / retry | `JobQueue` via `enqueue` |
| Hot state for one session (loss on crash OK) | Actor struct fields + [`ActorSession`](../../crates/trembita-actor/src/session.rs) |
| Idempotency / step counter across crash | `ActorStateStore` (`RedbActorStateStore` with `.data_dir()`) |
| Saga coordinator progress (multi-step workflow) | `MetaRaftSagaJournal` / `CompositeSagaJournal` ([workflows](../scenarios/workflows.md)) |

### Framework surface (shipped port)

```rust
#[async_trait]
pub trait ActorStateStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError>;
    async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), StoreError>;
    async fn delete(&self, key: &str) -> Result<(), StoreError>;
}

TrembitaCluster::builder()
    .data_dir("/var/lib/trembita")
    .actor_state_store(store)  // auto: RedbActorStateStore when `.data_dir()` is set
    .auto_workers([...])
```

Inject into actors via builder / `WorkerCtx` (see [stateful-workers](../scenarios/stateful-workers.md)).

### Crash and migration

| Event | Behavior |
|-------|----------|
| **Graceful leave** | Drain in-flight; durable store retains keys — new worker continues |
| **Crash** | Leader respawns worker on another VPS; worker **reloads from store** |
| **Migration RPC** | Optional flush of hot buffer; durable keys already in store |

Raft `migration_snapshot` remains for small in-flight buffers only.

### `RedbActorStateStore` (shipped)

Mirror [job-queue voter replication](job-queue.md):

- File: `{data_dir}/actor-store.redb` (or per-tenant prefix table)
- Leader mutations replicate to voters via dedicated RPC (same durability model as queue)
- TTL keys: lazy expiry on `get` + leader periodic GC (replicated deletes, default 60s / 256 keys)

### What external Redis is still for

- Cache shared with **non-trembita** processes (Python monolith, legacy service)
- Operator already runs Redis HA for other reasons — `trembita-store-redis` plugs in via the same trait

Not required for any of the four [product scenarios](product-scenarios.md) when the app is trembita-only.

## Consequences

**Positive**

- One `data_dir`, one backup story ([ops/backup-restore.md](../ops/backup-restore.md))
- Product teams avoid provisioning Redis Sentinel/Cluster
- Same port/adapter pattern as job queue — consistent architecture

**Negative**

- Cross-region active-active still needs explicit architecture (not solved by local redb alone)

## Related

- [actor-state-redis](actor-state-redis.md) — historical Redis-first wording; optional adapter
- [product-scenarios](product-scenarios.md)
- [stateful-workers](../scenarios/stateful-workers.md)
- [cross-node-actors](cross-node-actors.md)
- [job-queue](job-queue.md)
- [backlog.md](../backlog.md) — B-01
