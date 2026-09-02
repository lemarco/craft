# Product scenarios — actor-first platform (no mandatory Redis)

**Status:** Accepted  
**Date:** 2026-08-28

## Context

trembita targets **product teams**, not infra teams running Kubernetes microservices. The deployment model is [library-first](deployment-model.md): **one Rust codebase**, **one binary**, **N identical VPS processes** that join a cluster incrementally. Scale unit = **actors and VPS count**, not new Deployments or service meshes.

Five application patterns cover most distributed product work:

| Scenario | User-facing name | Guide |
|----------|------------------|-------|
| Background jobs | Sidekiq-style durable queue | [background-jobs](../scenarios/background-jobs.md) |
| Event topics | Pub/sub with independent subscribers | [event-topics](../scenarios/event-topics.md) |
| Stateful workers | Crash-safe actors + migration | [stateful-workers](../scenarios/stateful-workers.md) |
| Real-time / session | Sticky actors + stateless gateway | [realtime-sessions](../scenarios/realtime-sessions.md) |
| Workflow | Saga coordination (not embedded DB) | [workflows](../scenarios/workflows.md) |

All five compose on the same runtime. No separate job server, workflow server, or mandatory external KV.

## Decision

### Positioning

> **Trembita** — a **distributed coordination runtime**: cache hooks, job queue, actors, workflow machinery, cron. **Same [`TrembitaApp`](../../crates/trembita/src/app.rs) API** on one laptop or N VPSes. Domain data stays in **your** Postgres / services — trembita is not an application database. Cluster membership is **automatic** (seed + join); graceful shutdown drains actors and can leave the cluster. No Kubernetes. No mandatory Redis.

### Coordination vs domain data

| Built into trembita | Stays external |
|-------------------|----------------|
| Job queue, cron, lease/ack | Business tables (Postgres, …) |
| Actors, sessions, directory | Authoritative domain SM as product DB |
| Saga / workflow **journal** | Long-running side effects via enqueue / HTTP / cast |
| `ActorStateStore` (idempotency, step keys) | Mandatory Redis |

Advanced teams may embed a custom [`StateMachine`](../../crates/trembita-core/src/lib.rs) via [`TrembitaCluster`](../../crates/trembita/src/cluster.rs) — that is **not** the default [`TrembitaApp`](../../crates/trembita/src/app.rs) product path.

### Three messaging layers (product view)

| Layer | Mechanism | Product use |
|-------|-----------|-------------|
| **Actor mailbox** | `send` / `ask` / `ActorSession` | RPC, sync HTTP, real-time session to a pinned worker |
| **Job queue** | `JobQueue` → `RedbJobQueue` | Async backlog, many workers, autoscale |
| **Event topic** | `EventTopic` → `RedbEventTopic` | Fan-out domain events; per-subscription cursors ([event-topics](event-topics.md)) |
| **Workflow machinery** | Meta-Raft saga journal + steps | Multi-step processes with compensators; steps call mailbox/queue/topic or external APIs |

Actor **workflow keys** (idempotency, step progress outside SM) use [`ActorStateStore`](actor-state-store.md) — default path **`redb`**, not Redis.

See [job-queue](job-queue.md) for why mailboxes and Raft logs are not misused as queues.

### Infrastructure stance

| Required | Optional |
|----------|----------|
| VPS / bare metal (or containers as packaging only) | `trembita-store-redis` — integration with non-trembita services |
| `data_dir` on disk (`group-*.redb`, `queue-*.redb`, `topic-*.redb`, …) | [`trembita-backlog-postgres`](../../crates/trembita-backlog-postgres/) — optional `ExternalBacklog` adapter; Valkey/other adapters as needed |
| mTLS certs ([certificates](certificates.md)) | Load balancer in front of gateway nodes |

**Non-goals:** Kubernetes as core product, one-container-per-actor microservices, mandatory Redis/PostgreSQL/RabbitMQ, **static node roles** as the primary scaling model (removed — use homogeneous nodes + `.workload()`).

### Homogeneous nodes — compute tokens (B-16)

Every VPS runs the **same binary** (gateway when configured + job consumers + actors). There is no fleet-wide “gateway pool vs worker pool” env switch.

When ingress is quiet, **job consumers use spare CPU** on that node (night batch scenario). When gateway load rises, a per-node **workload governor** throttles consumer parallelism so API latency stays bounded — without rescaling the cluster.

See [workload-governor](workload-governor.md). Static role env vars (`TREMBITA_ROLE`, etc.) were **removed** in B-16g.

### Unified product surface (0.3.0+)

[`TrembitaApp`](../../crates/trembita/src/app.rs) wraps the same runtime as `TrembitaClusterBuilder`:

```rust
use std::time::Duration;
use trembita::{TrembitaApp, GatewayOpts, JobOpts, RunOpts, consumer};

#[consumer("emails")]
async fn send_email(_payload: &[u8]) -> Result<(), ()> {
    Ok(())
}

TrembitaApp::builder()
    .data_dir("/var/lib/trembita")
    .jobs([JobOpts::new("emails")
        .lease(Duration::from_secs(300))
        .consumer(&SendEmailConsumer)
        .http_enqueue(true)])
    .gateway(GatewayOpts::new("0.0.0.0:8090".parse()?))
    .run(RunOpts::default().with_wait_queue("emails"))
    .await?;
```

Declarative [`.jobs()`](../../crates/trembita/src/job_opts.rs) registers queue + consumer (+ optional HTTP enqueue). [`.workers()`](../../crates/trembita/src/worker_opts.rs) registers actor groups with explicit [`WorkerScale`](../../crates/trembita/src/worker_opts.rs) (`Fixed` / `PerNode` / queue `Auto`). Legacy [`.actors()`](../../crates/trembita/src/app.rs) + [`ActorGroupOpts`](../../crates/trembita/src/actor_group.rs) remain supported ([examples/](../../examples/README.md)).

### Scenario composition

```mermaid
flowchart TB
    subgraph Gateway["Gateway VPS (stateless)"]
        HTTP[HTTP / WebSocket]
    end

    subgraph Cluster["trembita cluster"]
        B[Actor mailbox — ask / ActorSession]
        C[Job queue — enqueue]
        W[Workflow journal + steps]
        A[Workers — scale_cluster]
    end

    HTTP --> B
    HTTP --> C
    B --> A
    C --> A
    W --> A
```

Typical flows:

- **Async API:** HTTP `202` → `enqueue` (job queue) → worker `lease`/`ack`
- **Sync API:** HTTP `200` → `ask` or `query` (actor mailbox or Raft SM)
- **Session:** WebSocket → `ActorSession` → `ask_session` (actor mailbox + workers)
- **Workflow:** `run_workflow` / HTTP `/workflows/*` — journal in Meta-Raft; steps call actors, queue, or external HTTP

### What is shipped vs polish backlog

| Capability | Status | Notes |
|------------|--------|-------|
| `RedbJobQueue`, `ClusterJobQueue`, autoscale | **shipped** | — |
| Saga journal (`MetaRaftSagaJournal`, `CompositeSagaJournal`) | **shipped** | — |
| `ActorSession`, consistent-hash routing | **shipped** | — |
| Actor migration on leave/crash | **shipped** | — |
| `RedbActorStateStore` + voter replication + TTL/GC | **shipped** | B-01 ✅ |
| `TrembitaApp` product facade + gateway | **shipped** | B-02 ✅ |
| HTTP jobs API (`202`, batch, DLQ requeue) | **shipped** | B-03 ✅ |
| Real-time showcase + `ActorsApi` on gateway | **shipped** | B-04 ✅ — [examples/realtime/](../../examples/realtime/) |
| `WorkflowBuilder` + resume CLI | **shipped** | B-05 ✅ |
| `trembita init` template | **shipped** | B-06 ✅ — polish: richer worker stubs |
| Dashboard: queue depth + saga status | **shipped** | B-07 ✅ |
| Scenario docs (redb-first, no mandatory Redis) | **shipped** | B-08 ✅ |
| Gateway bearer auth + `protect_product_apis` | **shipped** | B-14a ✅ |
| E2E HTTP batch via product gateway | **shipped** | B-14c ✅ — `trembita/tests/gateway_jobs_http.rs` |
| Gateway auth (custom beyond bearer stub) | **polish** | apps add `AuthFn` / custom identity |

Full epic list: [backlog.md](../backlog.md) (P0–P3 ✅).

## Consequences

**Positive**

- Single story for product teams: actors + disk, not microservices + Redis cluster
- All product scenarios share ops (backup `data_dir`, rolling upgrade, certs)
- Clear boundary: consensus in SM, work in queue, session in actor + optional store

**Negative**

- Declarative `.jobs()` / `.workers()` builder sugar shipped — legacy `.queue`, `.consumer`, and `.actors(name, ActorGroupOpts::…)` remain supported
- WebSocket gateway auth: [`GatewayBearerIdentity`](../../crates/trembita/src/gateway/identity.rs) covers bearer tokens on product routes; session/OAuth/JWT for custom WebSocket handlers remains app-owned via `.identity()` and custom routes
- Stateful workers need `RedbActorStateStore` + SM discipline — keys without SM still require explicit design

## Related

- [deployment-model](deployment-model.md)
- [actor-state-store](actor-state-store.md)
- [job-queue](job-queue.md)
- [cross-node-actors](cross-node-actors.md)
- [actor-routing](actor-routing.md)
- [multi-raft](multi-raft.md#cross-shard-transactions)
- [scenarios/README.md](../scenarios/README.md)
- [backlog.md](../backlog.md)
