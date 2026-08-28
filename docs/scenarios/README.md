# Product scenarios

Guides for building on crafty **without mandatory Redis or Kubernetes**. Each scenario uses the same binary and `data_dir`; scale by adding VPSes ([deployment-model](../decisions/deployment-model.md)).

**Decision record:** [product-scenarios](../decisions/product-scenarios.md)  
**Implementation backlog:** [backlog.md](../backlog.md)

## Choose your pattern

| I need… | Guide | Runtime today | Product polish (backlog) |
|---------|-------|---------------|--------------------------|
| Async work, retries, many workers | [Background jobs](background-jobs.md) | ✅ `RedbJobQueue`, E2E | `CraftyApp`, HTTP `202` helper |
| Actor state survives VPS crash | [Stateful workers](stateful-workers.md) | ✅ migration; ⚠️ store = in-memory or SM | `RedbActorStateStore` (B-01) |
| WebSocket / live session to one worker | [Real-time sessions](realtime-sessions.md) | ✅ `ActorSession` | `websocket_gateway` example (B-04) |
| Multi-step process with compensation | [Workflows](workflows.md) | ✅ `run_saga`, Meta-Raft journal | Fluent workflow builder (B-05) |

## Shared persistence model

```
data_dir/
├── group-0.redb           # Raft — StateMachine (domain data)
├── group-meta.redb        # Meta-Raft — saga journal, catalog (multi-Raft)
├── queue-{stream}.redb    # JobQueue backlog
├── mailbox-spool.redb     # durable cross-node deliver (optional)
└── actor-store.redb       # ActorStateStore (backlog B-01)
```

## Three tiers (do not mix)

| Tier | API | When |
|------|-----|------|
| **A** | `propose` / `query`, `run_saga` | Authoritative replicated data |
| **B** | `send` / `ask`, `ActorSession` | Talk to a specific actor now |
| **C** | `enqueue` / `lease` / `ack` | Shared durable backlog |

See [job-queue](../decisions/job-queue.md#three-messaging-tiers-explicit-split).

## Compose scenarios

Typical product stack on one codebase:

1. **HTTP gateway** (any VPS) — sync `ask`, async `enqueue`, WebSocket → session
2. **Workers** — `auto_workers` + optional `scale_cluster`
3. **Domain SM** — orders, accounts via `propose`
4. **Workflows** — onboarding saga calling enqueue + propose steps

```mermaid
flowchart LR
    Client --> Gateway
    Gateway -->|ask / session| Workers
    Gateway -->|enqueue| Queue
    Gateway -->|propose| Raft
    Queue --> Workers
    Saga --> Raft
    Saga --> Queue
    Saga --> Workers
```

## Examples (current)

| Scenario | Example |
|----------|---------|
| Background jobs | `cargo run -p crafty --example job_queue_worker` |
| Jobs + autoscale | `cargo run -p crafty --example job_queue_cluster` |
| Cluster actors | `cargo run -p crafty --example actors_cluster` |
| Actor + store split | `cargo run -p crafty --example hexagonal_actor_store` (use `InMemoryStore`; swap to redb when B-01 lands) |
| VPS join | `cargo run -p crafty --example vps_join` |

E2E: `./e2e/queue.sh` (QUIC/mTLS, failover).

## Related

- [status.md](../status.md) — shipped vs deferred
- [architecture.md](../architecture.md) — crate graph
- [deployment-model](../decisions/deployment-model.md) — one binary, N VPS
