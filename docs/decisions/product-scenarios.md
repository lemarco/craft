# Product scenarios — actor-first platform (no mandatory Redis)

**Status:** Accepted  
**Date:** 2026-08-28

## Context

crafty targets **product teams**, not infra teams running Kubernetes microservices. The deployment model is [library-first](deployment-model.md): **one Rust codebase**, **one binary**, **N identical VPS processes** that join a cluster incrementally. Scale unit = **actors and VPS count**, not new Deployments or service meshes.

Four application patterns cover most distributed product work:

| Scenario | User-facing name | Guide |
|----------|------------------|-------|
| Background jobs | Sidekiq-style durable queue | [background-jobs](../scenarios/background-jobs.md) |
| Stateful workers | Crash-safe actors + migration | [stateful-workers](../scenarios/stateful-workers.md) |
| Real-time / session | Sticky actors + stateless gateway | [realtime-sessions](../scenarios/realtime-sessions.md) |
| Workflow | Saga + queue as mini-Temporal | [workflows](../scenarios/workflows.md) |

All four compose on the same runtime. No separate job server, workflow server, or mandatory external KV.

## Decision

### Positioning

> **Crafty** — write actors; the cluster scales when you add VPSes. Jobs, stateful workers, sessions, and workflows are built in. Persistence defaults to **embedded `redb` + Raft** under `data_dir`. No Kubernetes. No mandatory Redis.

### Three persistence tiers (product view)

| Tier | Mechanism | Product use |
|------|-----------|-------------|
| **A — Consensus** | Raft → `StateMachine` | Orders, balances, config — linearizable domain data |
| **B — Actor mailbox** | `send` / `ask` / `ActorSession` | RPC, sync HTTP, real-time session to a pinned worker |
| **C — Job queue** | `JobQueue` → `RedbJobQueue` | Async backlog, many workers, autoscale |

Actor **workflow keys** (idempotency, step progress outside SM) use [`ActorStateStore`](actor-state-store.md) — default path **`redb`**, not Redis.

See [job-queue](job-queue.md) for why mailboxes and Raft logs are not misused as queues.

### Infrastructure stance

| Required | Optional |
|----------|----------|
| VPS / bare metal (or containers as packaging only) | `crafty-store-redis` — integration with non-crafty services |
| `data_dir` on disk (`group-*.redb`, `queue-*.redb`, …) | External PostgreSQL / Valkey adapters (future) |
| mTLS certs ([certificates](certificates.md)) | Load balancer in front of gateway nodes |

**Non-goals:** Kubernetes as core product, one-container-per-actor microservices, mandatory Redis/PostgreSQL/RabbitMQ.

### Unified product surface (shipped in 0.2.x)

[`CraftyApp`](../../crates/crafty/src/app.rs) wraps the same runtime as `CraftyClusterBuilder`:

```rust
use std::sync::Arc;
use crafty::{CraftyApp, ReadyOpts};

let app: Arc<CraftyApp> = CraftyApp::start_from_env_shared().await?;
app.wait_until_ready(ReadyOpts::default().with_queue("emails")).await;
// spawn consumers, custom http_routes, WebSocket handlers…
CraftyApp::run_until_shutdown_shared(app).await?;
```

Declarative `.jobs(...)` / `.workers(..., scale: Auto)` registration remains aspirational — use `.manage_auto` / `.manage` on the builder today ([examples/](../../examples/README.md)).

### Scenario composition

```mermaid
flowchart TB
    subgraph Gateway["Gateway VPS (stateless)"]
        HTTP[HTTP / WebSocket]
    end

    subgraph Cluster["crafty cluster"]
        B[Tier B — ask / ActorSession]
        C[Tier C — JobQueue enqueue]
        A[Tier A — propose / saga]
        W[Workers — scale_cluster]
    end

    HTTP --> B
    HTTP --> C
    B --> W
    C --> W
    A --> W
```

Typical flows:

- **Async API:** HTTP `202` → `enqueue` (C) → worker `lease`/`ack` (C + W)
- **Sync API:** HTTP `200` → `ask` or `query` (B or A)
- **Session:** WebSocket → `ActorSession` → `ask_session` (B + W)
- **Workflow:** `run_saga` (A journal) with steps calling C, B, or A

### What is shipped vs polish backlog

| Capability | Status | Notes |
|------------|--------|-------|
| `RedbJobQueue`, `ClusterJobQueue`, autoscale | **shipped** | — |
| Saga journal (`MetaRaftSagaJournal`, `CompositeSagaJournal`) | **shipped** | — |
| `ActorSession`, consistent-hash routing | **shipped** | — |
| Actor migration on leave/crash | **shipped** | — |
| `RedbActorStateStore` + voter replication + TTL/GC | **shipped** | B-01 ✅ |
| `CraftyApp` product facade + gateway | **shipped** | B-02 ✅ |
| HTTP jobs API (`202`, batch, DLQ requeue) | **shipped** | B-03 ✅ |
| Real-time showcase + `ActorsApi` on gateway | **shipped** | B-04 ✅ — [examples/realtime/](../examples/realtime/) |
| `WorkflowBuilder` + resume CLI | **shipped** | B-05 ✅ |
| `crafty init` template | **shipped** | B-06 ✅ — polish: richer worker stubs |
| Dashboard: queue depth + saga status | **shipped** | B-07 ✅ |
| Scenario docs (redb-first, no mandatory Redis) | **shipped** | B-08 ✅ |
| Gateway auth (beyond `GATEWAY_TOKEN` stub) | **polish** | — |
| E2E HTTP batch via gateway in docker | **polish** | QUIC queue E2E exists |

Full epic list: [backlog.md](../backlog.md) (P0–P3 ✅).

## Consequences

**Positive**

- Single story for product teams: actors + disk, not microservices + Redis cluster
- All four scenarios share ops (backup `data_dir`, rolling upgrade, certs)
- Clear boundary: consensus in SM, work in queue, session in actor + optional store

**Negative**

- Declarative `.jobs()` / `.workers()` builder sugar still aspirational — use `manage_auto` today
- WebSocket gateway auth remains a thin stub (`GATEWAY_TOKEN`) — production apps add their own layer
- Stateful workers need `RedbActorStateStore` + SM discipline — keys without SM still require explicit design

## Related

- [deployment-model](deployment-model.md)
- [actor-state-store](actor-state-store.md)
- [job-queue](job-queue.md)
- [cross-node-actors](cross-node-actors.md)
- [actor-routing-tier3](actor-routing-tier3.md)
- [multi-raft](multi-raft.md#cross-shard-transactions)
- [scenarios/README.md](../scenarios/README.md)
- [backlog.md](../backlog.md)
