# Stateful workers — crash-safe actors + migration

**Pattern:** Long-running or resumable work on actors; state survives VPS crash and graceful leave without Redis.

**Status:** **Shipped** in 0.2.x — migration + supervisor + **`RedbActorStateStore`** (voter replication, TTL/GC).

## When to use

- Handler keeps **workflow progress** (steps done, idempotency tokens) outside the Raft SM
- Worker may move to another VPS after crash or `cluster.leave()`
- You want **write-through** keys without bloating the Raft log

**Do not** put high-churn workflow bytes in Raft `propose` — use SM only for **domain entities** you need linearizable ([actor-state-store](../decisions/actor-state-store.md)).

## Three durability levels (no Redis)

| Level | Mechanism | Survives crash? |
|-------|-----------|-----------------|
| **Hot** | Fields on actor struct + [`ActorSession`](../../crates/trembita-actor/src/session.rs) | ❌ (OK for live session) |
| **Workflow keys** | [`ActorStateStore`](../../crates/trembita-actor/src/store.rs) → `RedbActorStateStore` | ✅ |
| **Domain data** | `StateMachine` via `propose` | ✅ (Raft replicated) |

## Architecture

```
                    Supervisor (leader)
                           │
         crash / leave ────┼──── spawn on surviving VPS
                           ▼
              Worker reloads ActorStateStore keys
              (or SM query for domain state)
```

Cross-node paths: [cross-node-actors](../decisions/cross-node-actors.md) — `spawn_remote`, `scale_cluster`, migration RPC.

## Quick start (current API)

### 1. Register workers on `TrembitaApp`

`RedbActorStateStore` is wired automatically with `.data_dir()` — same trait, no extra setup.

```rust
use trembita::{TrembitaApp, RunOpts, WorkerOpts, WorkerScale, workers};

TrembitaApp::builder()
    .data_dir("/var/lib/trembita")
    .workers(workers!(
        WorkerOpts::<OrderProcessor>::new("orders")
            .config(processor_cfg())
            .scale(WorkerScale::Fixed(1))
            .http_cast(true),
    ))
    .run(RunOpts::default())
    .await?;
```

See [`examples/stateful-workers/`](../../examples/stateful-workers/).

### 2. Stateful worker — write-through

```rust
use trembita::actor::{UserActor, actor, store_get, store_set};

#[actor]
impl UserActor for OrderProcessor {
    type Config = Arc<dyn ActorStateStore>;
    type Message = ProcessOrder;
    type Error = WorkerErr;

    fn start(store: Self::Config) -> Result<Self, Self::Error> {
        Ok(Self { store })
    }

    async fn handle(&mut self, msg: ProcessOrder) -> Result<(), Self::Error> {
        let key = format!("order:{}", msg.id);
        if store_get(&*self.store, &key).await?.is_some() {
            return Ok(()); // idempotent
        }
        // ... work ...
        store_set(&*self.store, &key, &done_marker, None).await?;
        Ok(())
    }
}
```

Pass `store` in `WorkerConfig` when spawning — see [`examples/stateful-workers/`](../../examples/stateful-workers/).

### 3. Domain data in StateMachine

For entities the business owns (order status, balance):

```rust
cluster.client().propose(OrderCommand::MarkPaid { id }).await?;
```

Authoritative read: `query`, not actor memory.

### 4. Scale and placement

Production default: **1 worker instance per VPS** per pool name ([cluster-elasticity](../decisions/cluster-elasticity.md)):

```rust
cluster.scale_cluster::<OrderProcessor>("processors", total_nodes, config).await?;
```

Add VPS → increase `total_nodes` (or rely on auto-spawn-on-join).

## Crash and leave behavior

| Event | What happens |
|-------|----------------|
| **Process crash** | Leader respawns worker elsewhere; reload from store / SM |
| **`cluster.leave()`** | Drain ([drain-timeout](../decisions/drain-timeout.md)); migration RPC optional |
| **Stale session** | [`ActorSession`](../../crates/trembita-actor/src/session.rs) expires → client re-opens or handles `NoTarget` |

## Sticky session without external store

For in-memory conversation state during a live connection:

```rust
let session = directory.session_keyed(&user_id, Some(Duration::from_secs(3600)))?;
messaging.ask_session(&session, msg).await?;
```

Documented in [actor-routing](../decisions/actor-routing.md). For durability across reconnect, persist session id client-side and/or store checkpoint in SM/redb.

## Operations

| Concern | Action |
|---------|--------|
| Backup | `actor-store.redb` + `group-*.redb` if using SM |
| Drain | `TREMBITA_DRAIN_TIMEOUT` / per-group override |
| Split brain vs SM | Never contradict SM in actor store — SM wins |

## Examples

| Asset | Notes |
|-------|-------|
| [`examples/stateful-workers/`](../../examples/stateful-workers/) | `ActorStateStore` + idempotent cast + migration demo |
| [`examples/stateful-workers/src/migrate_demo.rs`](../../examples/stateful-workers/src/migrate_demo.rs) | LocalNetwork migration walkthrough |
| `trembita-sim/tests/actor_scenarios.rs` | `scale_cluster`, migration |

## Registration API

Prefer [`.workers()`](../../crates/trembita/src/worker_opts.rs) with explicit [`WorkerScale`](../../crates/trembita/src/worker_opts.rs):

```rust
use trembita::{TrembitaApp, GatewayOpts, RunOpts, WorkerOpts, WorkerScale, workers};

TrembitaApp::builder()
    .data_dir("/var/lib/trembita")
    .workers(workers!(
        WorkerOpts::<OrderProcessor>::new("orders")
            .config(processor_cfg())
            .scale(WorkerScale::Fixed(1))
            .http_cast(true),
    ))
    .gateway(GatewayOpts::new("127.0.0.1:8190".parse()?).with_actors_api(true))
    .run(RunOpts::default())
    .await?;
```

Legacy [`.actors()`](../../crates/trembita/src/app.rs) + [`ActorGroupOpts`](../../crates/trembita/src/actor_group.rs) remain supported.

See [examples/stateful-workers/](../../examples/stateful-workers/).

## Related

- [actor-state-store](../decisions/actor-state-store.md) — redb-first ADR
- [background-jobs](background-jobs.md) — async alternative to long handler
- [background-jobs § Effectively-once recipe](background-jobs.md#effectively-once-recipe) — this store as the CAS layer that makes a redelivered job safe
- [realtime-sessions](realtime-sessions.md) — sticky in-memory state
- [backlog.md](../backlog.md) — polish items
