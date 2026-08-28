# Durable job queue — workers, mailbox, and autoscale

**Status:** Accepted  
**Date:** 2026-08-28

## Context

craft ships **cross-node actor mailboxes** ([cross-node-actors](cross-node-actors.md)): `send` / `ask` deliver messages to a **specific** actor instance over HTTP/3. Mailboxes are **in-memory**, serial per instance, and optimized for **low-latency point-to-point** work — especially **`ask`** (request/reply), which maps naturally to HTTP handlers that must respond before the connection closes.

Many applications also need a **durable, shared work buffer**:

- Producers enqueue work without waiting for a free worker.
- **Many** actor instances of the **same type** consume from **one** logical queue (`lease` / `ack`).
- Backlog survives process crash; at-least-once delivery with idempotent handlers.
- **Queue depth** drives **worker pool size** via the existing leader supervisor ([supervisor-leader](supervisor-leader.md), [cluster-elasticity](cluster-elasticity.md)).

Options considered:

| Option | Verdict |
|--------|---------|
| Use actor mailbox as the only queue | Rejected — unbounded RAM, no durable backlog, poor multi-consumer semantics |
| Put every job in the Raft log | Rejected — R1 write ceiling; wrong abstraction ([future-work-and-risks](future-work-and-risks.md)) |
| Reuse [`ActorStateStore`](actor-state-redis.md) as a queue | Rejected — KV get/set/CAS, not FIFO lease/ack |
| **Dedicated `JobQueue` port** + embedded disk + optional Redis adapter | **Accepted** |
| Replace all mailboxes with a durable queue | Rejected — local `ask`/control paths must stay fast; tiered model instead |

Relationship to [actor-state-redis](actor-state-redis.md): Redis remains an **optional** adapter for workflow keys and, if desired, a remote queue backend. The **default embedded path** is **`redb` on local disk** — no mandatory external dependency.

## Decision

### Three messaging tiers (explicit split)

| Tier | Mechanism | Purpose | Durability |
|------|-----------|---------|------------|
| **A — Consensus** | Raft `propose` / `query` → `StateMachine` | Authoritative replicated state | Raft log |
| **B — Actor mailbox** | `send` / `ask` → serial instance mailbox | RPC, control, coordination, “talk to this actor now” | In-memory (cross-node best-effort + `ask` dedup) |
| **C — Job queue** | `enqueue` / `lease` / `ack` / `nack` | Shared async backlog, worker pool consumption | Disk (`redb`) by default |

**Do not** route routine job payloads through tier B or tier A.

Typical HTTP mapping:

| User intent | Path |
|-------------|------|
| Synchronous API (`200` + body) | HTTP → **`ask`** (tier B) or **`query`** (tier A) |
| Async work (`202` + job id) | HTTP → **`enqueue`** (tier C) |
| Authoritative mutation | HTTP → **`propose`** (tier A) |

### `JobQueue` port

New trait in **`craft-actor`** (object-safe, boxed futures — same pattern as [`ActorStateStore`](../../crates/craft-actor/src/store.rs)):

```rust
pub struct JobId(u64);
pub struct LeaseId(u64);
pub struct WorkerId { pub node: NodeId, pub instance: u32 }

pub struct LeasedJob {
    pub lease_id: LeaseId,
    pub job_id: JobId,
    pub payload: Vec<u8>,
}

pub struct QueueMetrics {
    pub pending: u64,
    pub leased: u64,
    pub oldest_pending_age: Duration,
}

pub trait JobQueue: Send + Sync {
    fn enqueue(&self, payload: &[u8]) -> BoxFuture<'_, Result<JobId, QueueError>>;
    fn lease(&self, worker: WorkerId, max: usize) -> BoxFuture<'_, Result<Vec<LeasedJob>, QueueError>>;
    fn ack(&self, worker: WorkerId, lease_id: LeaseId) -> BoxFuture<'_, Result<(), QueueError>>;
    fn nack(&self, worker: WorkerId, lease_id: LeaseId) -> BoxFuture<'_, Result<(), QueueError>>;
    fn metrics(&self) -> BoxFuture<'_, Result<QueueMetrics, QueueError>>;
}
```

Semantics:

- **At-least-once** delivery; handlers must be **idempotent** (optionally aided by `ActorStateStore` keys).
- **`lease`** assigns up to `max` jobs exclusively to `worker` until **`ack`**, **`nack`**, or **visibility timeout** (after which the job returns to pending).
- **`nack`** requeues immediately (subject to retry policy).

### Adapters (ports & adapters litmus test)

| Adapter | Crate | Role |
|---------|-------|------|
| **`InMemoryJobQueue`** | `craft-actor` | Tests, sim, single-node dev |
| **`RedbJobQueue`** | `craft-actor` | Default production backend (embedded `redb`) |
| **`RedisJobQueue`** (optional) | `craft-store-redis` or sibling | Remote/shared queue when user already runs Redis |

**Default:** `RedbJobQueue` — `{data_dir}/queue-{stream}.redb`, separate from `group-*.redb` Raft files.

#### `RedbJobQueue` layout (sketch)

- **`jobs`** table: `job_id → postcard(JobRecord { payload, state })`
- **`pending`** index: ready `job_id`s (FIFO by id)
- **`leased`** index: `lease_id → job_id` + expiry
- **`meta`**: `next_id`, compaction `head`
- Mutations commit in **one `redb` write transaction** each (same durability model as [`RedbStorage`](../../crates/craft-storage/src/redb_store.rs)).
- Optional **in-memory prefetch** batch in the queue service — hot `lease` reads from RAM, flushes acks to disk.

Compaction trims acknowledged jobs (analogous to log purge), preventing unbounded file growth.

### Shared queue without Redis

A **shared logical queue** does not require a shared filesystem or Redis:

- **`QueueService`** runs on the **Raft leader** node (leader-only, like [`ClusterSupervisor`](../../crates/craft-actor/src/supervisor.rs)).
- Workers on **any** VPS call queue RPCs over mTLS (same transport class as `/actor/deliver`).
- Followers **forward** queue mutations to the leader ([client-routing](client-routing.md) forward pattern).

Wire routes (under `/raft/v1/queue/`):

| Route | Purpose |
|-------|---------|
| `POST .../enqueue` | Append job |
| `POST .../lease` | Worker pull |
| `POST .../ack` / `.../nack` | Complete or requeue |
| `GET .../metrics` | Depth for autoscale & observability |

**v2 (deferred):** sharded streams or partitioned queues when a single leader queue becomes a hotspot.

### Worker consumption model

Queue-backed workers are still **`UserActor`** instances, but **jobs are not pushed into the mailbox**:

- A **consumer loop** (spawned from `UserActor::start`) calls `lease` → user handler → `ack`/`nack`.
- The **mailbox** handles **control** messages only: drain, health, migration snapshot ([cross-node-actors](cross-node-actors.md)).

```rust
// Facade helper (sketch)
CraftCluster::builder()
    .job_queue_stream("workers", JobQueueStreamConfig {
        path: data_dir.join("queue-workers.redb"),
        lease_timeout: Duration::from_secs(60),
        autoscale: Some(AutoscalePolicy { .. }),
    })
    .auto_workers([AutoWorkerSpec::queue_consumer("workers", WorkerConfig::...)])
```

### Queue-driven autoscale (leader)

Extend leader reconciliation ([supervisor-leader](supervisor-leader.md)) with **`QueueAutoscaler`**:

1. Poll `QueueMetrics` on a interval (with **cooldown** and **hysteresis**).
2. Compute `desired_workers = f(pending, leased, policy)`.
3. Clamp to `[min, min(max, live_node_count)]` — production still obeys [one worker per VPS](one-worker-per-vps.md).
4. Call existing **`scale_cluster(name, desired)`** when `desired ≠ directory_count`.

**Autoscale policy** (thresholds, min/max, cooldown) may be stored in **Meta-Raft** SM for failover; **queue depth** is read live from the queue service, not replicated per job.

Scaling **out beyond node count** in production still means **add VPS + join** ([cluster-elasticity](cluster-elasticity.md)); autoscale only raises worker count up to available nodes unless `--dev-multi-workers`.

### What the queue is not

- **Not** a replacement for actor mailboxes or Raft.
- **Not** linearizable with SM state unless the application designs commit ordering.
- **Not** a global serializable transaction log across shards (see [cross-shard-transactions](cross-shard-transactions.md)).

## v1 implementation scope

| In v1 | Status |
|-------|--------|
| `JobQueue` trait + `InMemoryJobQueue` | **landed** |
| `RedbJobQueue` + crash/reopen tests | **landed** |
| Worker consumer helper + example | **landed** |
| Leader `QueueService` + wire routes | deferred |
| `QueueAutoscaler` → `scale_cluster` | deferred |
| Facade builder + metrics hook | partial (facade re-exports) |

Implementation status: **core port landed**; leader wire + autoscale deferred — see [status.md](../status.md).

### Deferred (post-v1)

| Item | Notes |
|------|-------|
| Sharded / multi-stream federation | Hotspot mitigation |
| `RedisJobQueue` adapter | Optional remote backend |
| Cross-node durable mailbox outbox/inbox | Tier B spool |
| Auto membership scale from queue depth | Join VPS when workers = nodes |
| Priority queues, delayed jobs | — |

## Consequences

**Positive**

- Clear separation: RPC (mailbox), authority (Raft), backlog (queue).
- Embedded default — no Redis required; fits library-first VPS deploys.
- Reuses leader supervisor and `scale_cluster` instead of a new placement system.
- Worker pool + shared queue matches BEAM-style “many consumers, one backlog” without conflating tiers.

**Negative**

- Leader-hosted queue is a **throughput hotspot** at very large enqueue rates (mitigation: batch append, prefetch, future sharding).
- At-least-once requires **idempotent** handlers and visibility-timeout tuning.
- Two durability stories (`ActorStateStore` vs `JobQueue`) — docs must keep boundaries explicit ([R4](future-work-and-risks.md)).
- Extra wire surface and ops metrics for queue lag.

## Related

- [cross-node-actors.md](cross-node-actors.md) — mailbox, `scale_cluster`, migration
- [cluster-elasticity.md](cluster-elasticity.md) — one worker/VPS, auto-spawn, scale targets
- [supervisor-leader.md](supervisor-leader.md) — leader-only reconcile
- [actor-state-redis.md](actor-state-redis.md) — workflow KV (complementary, not queue)
- [architecture-style.md](architecture-style.md) — ports & adapters
- [future-work-and-risks.md](future-work-and-risks.md) — R1 (do not put jobs in Raft log)
- [wire-protocol](wire-protocol.md) — HTTP/3 route namespace
