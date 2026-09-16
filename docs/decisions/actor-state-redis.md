# Stateful actors — external store (Redis)

> **Product default:** embedded **redb** via [`ActorStateStore`](actor-state-store.md) — no mandatory Redis.
> This record covers the **optional** Redis adapter (`trembita` feature `redis-store`) and integration with non-trembita services. Scenario: [stateful-workers](../scenarios/stateful-workers.md).

**Status:** Accepted (optional adapter path)  
**Date:** 2026-07-05

## Context

Medium open question **#2**: what happens to **stateful actor** memory on VPS crash?

Options considered: document-only, Raft log snapshots, hybrid. User chose **external database** — **Redis as the primary example** — rather than persisting actor state through the Raft log.

## Decision

### Two layers of state (explicit split)

| Layer | Store | Purpose |
|-------|--------|---------|
| **Authoritative / consensus** | Raft → `StateMachine` ([state-machine](state-machine.md)) | Orders, balances, config — must be linearizable and replicated |
| **Stateful actor / workflow** | **External store** (Redis recommended) | Session progress, job steps, locks, idempotency keys, workflow caches |

**Do not** put routine actor workflow state in the Raft log — avoids log bloat and wrong abstraction.

### Redis as default recommendation

Redis fits trembita’s VPS model:

- Survives **node crash** — new worker on any VPS reads same keys
- Fast read/write for actor `handle` loops
- TTL, pub/sub, locks (Redlock patterns) for distributed workers
- User operates Redis HA (Sentinel/Cluster) — **outside** trembita core

Other backends may implement the same trait (PostgreSQL, Valkey, etc.).

### Framework surface

Shipped as [`trembita-store-redis`](../../crates/trembita-store-redis/) — enable on the facade via `redis-store` ([facade](facade.md)):

```toml
trembita = { version = "0.6", features = ["redis-store"] }
```

```rust
use trembita::{RedisStore, store_redis::RedisTlsConfig};

let store = RedisStore::connect("redis://127.0.0.1:6379").await?.with_prefix("orders:");
```

Trait ([`ActorStateStore`](../../crates/trembita-actor-store/src/store.rs)):

```rust
#[derive(UserActor)]
struct OrderWorker {
    store: Arc<dyn ActorStateStore>,
}

impl OrderWorker {
    async fn handle(&mut self, msg: OrderMsg) -> Result<(), Error> {
        if self.store.get(&format!("order:{id}")).await?.is_some() {
            return Ok(()); // idempotent resume
        }
        // ... work ...
        self.store.set(&format!("order:{id}"), &state, None).await?;
        Ok(())
    }
}
```

### Crash and migration ([cross-node-actors](cross-node-actors.md))

| Event | Behavior |
|-------|----------|
| **Graceful leave** | Drain in-flight; state already in Redis — new worker continues |
| **Crash** | Actor state in Redis **retained**; leader respawns worker on another VPS ([cluster-elasticity](cluster-elasticity.md#supervisor--leader-only-reconciliation)); worker **reloads from Redis** |
| **`migration_snapshot`** | Optional optimization for large in-flight buffer flush to Redis before migrate — not required if handlers write through store |

Raft `migration_snapshot` remains for **small hot buffers**; **Redis is source of truth** for durable actor workflow state.

### What Redis is not

- **Not** a replacement for Raft `StateMachine` for consensus data
- **Not** managed by trembita — user provisions Redis (single instance dev, HA prod)
- **Not** linearizable with Raft reads unless user designs transactions carefully

### Shipped vs backlog

| Shipped | Not in core |
|---------|-------------|
| `ActorStateStore` trait + **`RedisStore`** in `trembita-store-redis` | Built-in PostgreSQL `ActorStateStore` |
| Docs + integration path | Redis Cluster auto-discovery, framework-hosted Redis |

Core trembita **works without Redis** — default stateful path is [`RedbActorStateStore`](actor-state-store.md). Use Redis when you already run it for non-trembita services or need a shared cache outside `data_dir`.

## Consequences

**Positive**

- Clear durability story on crash without Raft log pollution
- Natural fit with 1 worker/VPS + cross-node workers
- Users know Redis ops; trembita stays focused

**Negative**

- Extra dependency and HA burden on user
- Risk of split-brain if actor state contradicts Raft SM — docs must stress boundaries
- Need connection pooling / TLS to Redis in production

## Related

- [cross-node-actors.md](cross-node-actors.md)
- [state-machine.md](state-machine.md)
- [cluster-elasticity.md#auto-spawn-on-join](cluster-elasticity.md#auto-spawn-on-join)
- [cluster-elasticity.md#supervisor--leader-only-reconciliation](cluster-elasticity.md#supervisor--leader-only-reconciliation)
