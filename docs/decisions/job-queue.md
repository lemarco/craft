# Durable job queue — workers, mailbox, and autoscale

**Status:** Accepted  
**Date:** 2026-08-28

## Context

trembita ships **cross-node actor mailboxes** ([cross-node-actors](cross-node-actors.md)): `send` / `ask` deliver messages to a **specific** actor instance over HTTP/3. Mailboxes are **in-memory**, serial per instance, and optimized for **low-latency point-to-point** work — especially **`ask`** (request/reply), which maps naturally to HTTP handlers that must respond before the connection closes.

Many applications also need a **durable, shared work buffer**:

- Producers enqueue work without waiting for a free worker.
- **Many** actor instances of the **same type** consume from **one** logical queue (`lease` / `ack`).
- Backlog survives process crash; at-least-once delivery with idempotent handlers.
- **Queue depth** drives **worker pool size** via the existing leader supervisor ([cluster-elasticity](cluster-elasticity.md)).

Options considered:

| Option | Verdict |
|--------|---------|
| Use actor mailbox as the only queue | Rejected — unbounded RAM, no durable backlog, poor multi-consumer semantics |
| Put every job in the Raft log | Rejected — R1 write ceiling; wrong abstraction ([future-work-and-risks](future-work-and-risks.md)) |
| Reuse [`ActorStateStore`](actor-state-redis.md) as a queue | Rejected — KV get/set/CAS, not FIFO lease/ack |
| **Dedicated `JobQueue` port** + embedded disk + optional Redis adapter | **Accepted** |
| Replace all mailboxes with a durable queue | Rejected — local `ask`/control paths must stay fast; layered model instead |

Relationship to [actor-state-redis](actor-state-redis.md): Redis is an **optional** adapter for workflow keys only. Queue backlog is **`redb` on local disk** by default — no mandatory external dependency. A remote Redis queue adapter is not shipped; track open work in [backlog.md](../backlog.md#open-work).

## Decision

### Three messaging layers (explicit split)

| Layer | Mechanism | Purpose | Durability |
|-------|-----------|---------|------------|
| **Consensus** | Raft `propose` / `query` → `StateMachine` | Authoritative replicated state | Raft log |
| **Actor mailbox** | `send` / `ask` → serial instance mailbox | RPC, control, coordination, “talk to this actor now” | In-memory (cross-node best-effort + `ask` dedup) |
| **Job queue** | `enqueue` / `lease` / `ack` / `nack` | Shared async backlog, worker pool consumption | Disk (`redb`) by default |

**Do not** route routine job payloads through the mailbox or consensus layers.

**Event topics** are a separate fan-out layer (one publish, many named subscriptions with
independent cursors) — see [event-topics](event-topics.md). Do not model pub/sub as multiple
queue enqueues.

Typical HTTP mapping:

| User intent | Path |
|-------------|------|
| Synchronous API (`200` + body) | HTTP → **`ask`** (mailbox) or **`query`** (consensus) |
| Async work (`202` + job id) | HTTP → **`enqueue`** (job queue) |
| Authoritative mutation | HTTP → **`propose`** (consensus) |

### `JobQueue` port

New trait in **`trembita-jobs`** (object-safe, boxed futures — same pattern as [`ActorStateStore`](../../crates/trembita-capstore/src/store.rs)):

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
| **`InMemoryJobQueue`** | `trembita-jobs` | Tests, sim, single-node dev |
| **`RedbJobQueue`** | `trembita-jobs` | Default production backend (embedded `redb`) |
| **`ClusterJobQueue`** | `trembita-jobs` + runtime | Leader `QueueService` + follower forward/replicate |

**Default:** `RedbJobQueue` — `{data_dir}/queue-{stream}.redb`, separate from `group-*.redb` Raft files.

#### `RedbJobQueue` layout (sketch)

- **`jobs`** table: `job_id → postcard(JobRecord { payload, state })`
- **`pending`** index: ready `job_id`s (FIFO by id)
- **`leased`** index: `lease_id → job_id` + expiry
- **`meta`**: `next_id`, compaction `head`
- Mutations commit in **one `redb` write transaction** each (same durability model as [`RedbStorage`](../../crates/trembita-storage/src/redb_store.rs)).
- Optional **in-memory prefetch** batch in the queue service — hot `lease` reads from RAM, flushes acks to disk.

Compaction trims acknowledged jobs (analogous to log purge), preventing unbounded file growth. **`RedbJobQueue`** runs `Database::compact()` every 64 acks on the dedicated queue file.

### Shared queue without Redis

A **shared logical queue** does not require a shared filesystem or Redis:

- **`QueueService`** runs on the **Raft leader** node (leader-only, like [`ClusterSupervisor`](../../crates/trembita-runtime/src/supervisor.rs)).
- Workers on **any** VPS call queue RPCs over mTLS (same transport class as `/actor/deliver`).
- Followers **forward** queue mutations to the leader ([client-and-routing](client-and-routing.md) forward pattern).
- After each leader mutation, **`QueueReplicateOp`** batches are pushed **in parallel** to every other **reachable voter** (`POST /raft/v1/queue/replicate`); the client only receives success once all peers ack — so a newly elected leader serves the same backlog from its local `redb`.
- **`/queue/replicate` is leader-authenticated**: the transport must tag the caller (`LocalTransport`, QUIC mTLS peer id); followers reject replicate unless `from == current Raft leader`.

Wire routes (under `/raft/v1/queue/`):

| Route | Purpose |
|-------|---------|
| `POST .../enqueue` | Append job |
| `POST .../lease` | Worker pull |
| `POST .../ack` / `.../nack` | Complete or requeue |
| `GET .../metrics` | Depth for autoscale & observability |
| `POST .../replicate` | Leader → voter idempotent state sync |

Cross-node mailbox spool: **[`TrembitaConfigure::with_durable_mailbox`](../../crates/trembita/src/configure.rs)** on product apps (B-41) or **[`TrembitaClusterBuilder::durable_mailbox`](../../crates/trembita-assembly/src/builder/cluster/config.rs)** in assembly tests — `{data_dir}/mailbox-spool.redb` ([capabilities § B-41](../scenarios/capabilities.md#product-surface-gaps-b-41)).

### Sharded streams

- **`job_queue_sharded(name, shard_count, lease_timeout)`** — `{name}~0` … `{name}~{N-1}` independent redb files; logical [`ShardedJobQueue`](../../crates/trembita-jobs/src/sharded_queue.rs) federates enqueue/lease/ack.
- Enqueue routes by hash of `shard_key` (or payload); replication runs per shard stream.
- Spreads leader write + replicate load without putting jobs in the Raft log.

### Priority and delayed jobs

- [`EnqueueOptions`](../../crates/trembita-jobs/src/queue/mod.rs): `priority` (higher first), `not_before_ms` / `EnqueueOptions::delayed`.
- Wire: optional fields on `QueueEnqueueRequest`; replicated in `QueueReplicateOp::Enqueue`.

### Membership autoscale

- [`MembershipAutoscalePolicy`](../../crates/trembita-jobs/src/queue_autoscale.rs) + [`job_queue_membership_autoscale`](../../crates/trembita-assembly/src/builder/cluster/mod.rs): when `(pending + leased) / live_nodes` exceeds threshold and `live_nodes < max_nodes`, invoke user `join` hook (deploy VPS + dynamic join).
- Complements worker [`AutoscalePolicy`](../../crates/trembita-jobs/src/queue_autoscale.rs) capped at `reachable_nodes`.

### Worker consumption model

Job **consumers** run as runtime tasks (often backed by an internal worker host), but **jobs are not actor mailboxes**:

- A **consumer loop** calls `lease` → user handler (`#[consumer]` or capability bridge) → `ack`/`nack`.
- The **mailbox** handles **control** messages only: drain, health, migration snapshot ([cross-node-actors](cross-node-actors.md)).

```rust
TrembitaApp::from_env()?
    .manifest(
        AppManifest::new().jobs([
            JobOpts::new("workers", Duration::from_secs(60))
                .consumer(/* ConsumerOpts or #[consumer] */),
        ]),
    )
    .configure(TrembitaConfigure::default().with_data_dir("/var/trembita"))
    .run()
    .await?;
```

Leader-side autoscale hooks live on [`trembita-assembly`](../../crates/trembita-assembly/src/builder/cluster/mod.rs) [`TrembitaClusterBuilder`](../../crates/trembita-assembly/src/builder/mod.rs) (`job_queue_at`, membership autoscale) — not product API.

### Queue-driven autoscale (leader)

Extend leader reconciliation ([cluster-elasticity](cluster-elasticity.md)) with **`QueueAutoscaler`**:

1. Poll `QueueMetrics` on a interval (with **cooldown** and **hysteresis**).
2. Compute `desired_workers = f(pending, leased, policy)`.
3. Clamp to `[min, min(max, live_node_count)]` — production still obeys [one worker per VPS](cluster-elasticity.md).
4. Call existing **`scale_cluster(name, desired)`** when `desired ≠ directory_count`.

**Autoscale policy** (thresholds, min/max, cooldown) is stored in **Meta-Raft** (`QueueAutoscalePolicyCommand`) and applied via `QueueAutoscaleRegistry` on every node — failover-safe without re-reading builder config. **Queue depth** is read live from the queue service, not replicated per job.

Scaling **out beyond node count** in production still means **add VPS + join** ([cluster-elasticity](cluster-elasticity.md)); autoscale only raises worker count up to available nodes unless `--dev-multi-workers`.

### What the queue is not

- **Not** a replacement for actor mailboxes or Raft.
- **Not** linearizable with SM state unless the application designs commit ordering.
- **Not** a global serializable transaction log across shards (see [multi-raft § cross-shard transactions](multi-raft.md#cross-shard-transactions)).

## Shipped capabilities

The workspace ships the full embedded queue stack: `JobQueue` port, `InMemoryJobQueue`, `RedbJobQueue`, leader `QueueService` with `/raft/v1/queue/*` and voter `/queue/replicate`, `ClusterJobQueue` + follower forward, worker consumer helpers, facade `job_queue` / `job_queue_autoscale` / `job_queue_sharded` / `job_queue_membership_autoscale`, priority and delayed enqueue, Meta-Raft autoscale policy, and periodic `RedbJobQueue` compaction.

For a product-oriented checklist and gaps, see [status.md](../status.md) and [backlog.md](../backlog.md).

## Consequences

**Positive**

- Clear separation: RPC (mailbox), authority (Raft), backlog (queue).
- Embedded default — no Redis required; fits library-first VPS deploys.
- Reuses leader supervisor and `scale_cluster` instead of a new placement system.
- Worker pool + shared queue matches BEAM-style “many consumers, one backlog” without conflating messaging layers.

**Negative**

- Leader-hosted queue is a **throughput hotspot** at very large enqueue rates (mitigation: batch append, prefetch, [`job_queue_sharded`](../../crates/trembita-assembly/src/builder/cluster/products.rs), [`job_queue_auto_shard`](../../crates/trembita-assembly/src/builder/cluster/products.rs)). Replication adds one RTT to each reachable voter before client ack. Measure with `benchmarks/benches/queue.rs` (criterion) and `soak_queue` (sustained enqueue + follower drain).
- At-least-once requires **idempotent** handlers and visibility-timeout tuning. Optional **`dedup_key`** on enqueue makes client retries safe while the job exists; the key is **released on ack** (job removed) and **held on dead letter** until `requeue_dead_letter` — see [background-jobs § `dedup_key` lifecycle](../scenarios/background-jobs.md#dedup_key-lifecycle).
- **`LeaseId` is monotonic per stream** (replicated `next_lease_id` with `max()` on failover). Redelivery issues a new id; stale tokens return `InvalidLease`. It is **not** documented as an external fencing token — side-effect guards belong in the handler ([effectively-once recipe](../scenarios/background-jobs.md#effectively-once-recipe)).
- **An exactly-once delivery mode is not planned.** `dedup_key` deduplicates *enqueues*, not *deliveries*; effectively-once remains a handler-side property. The recipe (enqueue key → CAS marker in a store → side effect → durable `done` → ack) is in [background-jobs § Effectively-once recipe](../scenarios/background-jobs.md#effectively-once-recipe).
- Two durability stories (`ActorStateStore` vs `JobQueue`) — docs must keep boundaries explicit ([R4](future-work-and-risks.md)).
- Extra wire surface and ops metrics for queue lag.
- Enqueue is unavailable while any **reachable** voter cannot accept replication (strict sync); unreachable departed nodes are excluded via `reachable_nodes()`.

## Automatic queue sharding (shipped)

| API | Behaviour |
|-----|-----------|
| **Product `TrembitaApp` (B-32)** — [`QueueOpts::sharded`](../../crates/trembita/src/queue_opts.rs) / [`.auto_shard()`](../../crates/trembita/src/queue_opts.rs) on manifest, or env-only `TREMBITA_JOB_QUEUE_*` | Same physical streams as assembly helpers; wired via [`mount_queue_stream`](../../crates/trembita/src/app/builder.rs). |
| **`job_queue_sharded(name, N, …)`** | Fixed `N` physical streams (`{name}~0` …) at boot — hash routing via [`ShardedJobQueue`](../../crates/trembita-jobs/src/sharded_queue.rs). |
| **`job_queue_auto_shard(name, …, AutoShardPolicy)`** | Starts as one shard (`{name}~0`); leader coordinator adds `{name}~1`, … when logical `pending` stays above policy thresholds (cap `max_shards`). Followers **lazy-open** new physical streams on first replicate. Existing backlog stays on the shard that holds it — no cross-shard migration. |

Trade-offs: no global FIFO across shards; autoscale and governor depth aggregation sum shards (logical client view).

**Future:** operator-triggered split via ops HTTP; enqueue-latency / replicate-stall signals in addition to depth.

## Related

- [cross-node-actors.md](cross-node-actors.md) — mailbox, `scale_cluster`, migration
- [cluster-elasticity.md](cluster-elasticity.md) — one worker/VPS, auto-spawn, scale targets
- [cluster-elasticity.md](cluster-elasticity.md) — leader-only reconcile, one worker per VPS
- [actor-state-redis.md](actor-state-redis.md) — workflow KV (complementary, not queue)
- [architecture-style.md](architecture-style.md) — ports & adapters
- [future-work-and-risks.md](future-work-and-risks.md) — R1 (do not put jobs in Raft log)
- [wire-protocol](wire-protocol.md) — HTTP/3 route namespace
