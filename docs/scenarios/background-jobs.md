# Background jobs — distributed durable queue

**Pattern:** Sidekiq / Celery-style async work — enqueue now, workers consume later, survive crash and leader failover.

**Status:** **Shipped** in 0.2.x — `CraftyApp`, HTTP `202`/batch, `#[crafty::consumer]`, gateway, DLQ requeue, cron schedules.

## When to use

- Producer must not wait for a free worker
- Many workers of the **same type** share **one** backlog
- At-least-once delivery with **idempotent** handlers
- Queue depth should drive **autoscale** ([job-queue](../decisions/job-queue.md))

**Do not** use actor mailboxes (`send`/`cast`) as a job queue — unbounded RAM, no FIFO lease/ack ([job-queue](../decisions/job-queue.md)).

## Architecture

```
Producer (any node)          Leader QueueService          Workers (any node)
      │                            │                            │
      └── enqueue ──► redb ──► replicate to voters ◄── lease / ack
                         queue-{stream}.redb
```

- **Default backend:** `RedbJobQueue` — no Redis
- **Cross-node consume:** `ClusterJobQueue` on followers
- **Failover:** voter replication; new leader serves same backlog from local redb

## Quick start (`CraftyApp`)

### 1. Builder — register queue + consumer

Prefer [`.jobs()`](../../crates/crafty/src/job_opts.rs) to register stream, lease, consumer, and optional HTTP enqueue together:

```rust
use std::time::Duration;

use crafty::{CraftyApp, CronOpts, GatewayOpts, JobOpts, RecurringJob, RunOpts, consumer};

#[consumer("emails")]
async fn send_email(_payload: &[u8]) -> Result<(), ()> {
    Ok(())
}

CraftyApp::builder()
    .data_dir("/var/lib/crafty")
    .jobs([JobOpts::new("emails")
        .lease(Duration::from_secs(300))
        .consumer(&SendEmailConsumer)
        .http_enqueue(true)])
    .cron([CronOpts::new(
        "emails",
        RecurringJob {
            name: "daily-digest".into(),
            cron: "0 9 * * *".into(),
            payload: br#"{"kind":"digest"}"#.to_vec(),
            priority: 0,
            max_attempts: 0,
            enabled: true,
        },
    )])
    .gateway(GatewayOpts::new("127.0.0.1:8090".parse()?))
    .run(RunOpts::default().with_wait_queue("emails"))
    .await?;
```

Lower-level [`.queue()`](../../crates/crafty/src/app.rs) + [`.consumer()`](../../crates/crafty/src/app.rs) remain available. Cluster-level autoscale, sharded streams, priority, dedup: see [background-jobs showcase](../../examples/background-jobs/) and [job-queue](../decisions/job-queue.md).

### 2. Enqueue (from any node)

```rust
use crafty::cluster::EnqueueOptions;

let job_id = app.enqueue("emails", br#"{"to":"user@example.com"}"#).await?;

// Priority, dedup, delayed, max attempts:
app.enqueue_opts(
    "emails",
    payload,
    EnqueueOptions::dedup_key("invoice-7").max_attempts(5),
)
.await?;

// Batch (one leader transaction, capped at DEFAULT_QUEUE_BATCH_MAX):
app.enqueue_batch("emails", &[b"a".as_slice(), b"b"]).await?;
```

### 3. Consume — `#[crafty::consumer]` (recommended)

Annotate an async handler; the macro generates a `JobConsumer` adapter. Spawn with `CraftyApp::spawn_consumer` — no manual `tokio::spawn` + `run_queue_consumer` boilerplate ([backlog B-03b](../backlog.md)).

```rust
use std::sync::Arc;

use crafty::{consumer, ConsumerOpts, CraftyApp};

#[crafty::consumer("emails")]
async fn handle_email(payload: &[u8]) -> Result<(), MyError> {
    // decode payload, send mail, …
    Ok(())
}

let app = Arc::new(app);
let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);

let worker = app.spawn_consumer(
    HandleEmailConsumer,
    ConsumerOpts {
        instance: 0,
        batch: 4,
        ..ConsumerOpts::default()
    },
    stop_rx,
);

// … later: stop_tx.send(true)?; worker.await?;
```

**Lower level:** [`run_queue_consumer`](../../crates/crafty-actor/src/queue.rs) on `cluster.job_queue("stream")` when you need a custom loop. See [background-jobs showcase](../../examples/background-jobs/) or `./e2e/queue.sh` for QUIC failover.

**Idempotency:** use `EnqueueOptions::dedup_key` and/or store processed ids in your `StateMachine` or `ActorStateStore`.

### Queue → actor bridge

Use the queue for **durability and retry**; delegate **stateful side effects** to a worker group via `cast` / `ask` so you do not duplicate handler logic in the consumer:

```rust
#[consumer("orders")]
async fn fulfill_order(payload: &[u8], ctx: JobContext<'_>) -> Result<(), MyError> {
    let order_id = std::str::from_utf8(payload)?;
    // Idempotent enqueue key already guards duplicate jobs; cast is fire-and-forget.
    app.cast("inventory", format!("reserve:{order_id}").into_bytes()).await?;
    app.ask("billing", format!("charge:{order_id}").into_bytes()).await?;
    Ok(())
}
```

Patterns:

| Need | Use |
|------|-----|
| Fire-and-forget to a stateful worker | `app.cast(group, bytes)` |
| Read-modify-write with reply | `app.ask(group, bytes)` |
| Cross-shard saga step | `app.enqueue_workflow_step` + [`WorkflowBuilder::step_dedup_key`](../../crates/crafty/src/workflow.rs) |

See [state placement cheat sheet](state-placement.md) for where queue backlog vs actor state vs saga journal live.

### 4. HTTP mapping (recommended)

Wire the gateway (`http-jobs` feature) with [`GatewayOpts`](../../crates/crafty/src/gateway.rs) — built-in `/jobs/*` routes are **opt-in** (`.with_jobs_api(true)` or `CRAFTY_GATEWAY_JOBS=1`). Request bodies: raw bytes or JSON `{ "payload": "…" }` / `{ "payload_b64": "…" }` ([`crafty-http` README](../../crates/crafty-http/README.md)).

| Intent | Response | Route / API |
|--------|----------|-------------|
| Accept work | `202 Accepted` + `job_id` | `POST /jobs/{stream}` · `app.enqueue` |
| Batch accept | `202` + `job_ids` | `POST /jobs/{stream}/batch` · `app.enqueue_batch` |
| Batch ack (workers) | `200 OK` | `POST /jobs/{stream}/ack-batch` · `app.ack_batch` |
| Job metadata | `200 OK` | `GET /jobs/{stream}/{id}` · `app.job_status` |
| Requeue dead letter | `200 OK` | `POST /jobs/{stream}/{id}/requeue` · `app.requeue_dead_letter` |
| Sync read / RPC | `200 OK` | `POST /actors/{group}/ask` · `app.ask` |
| Fire-and-forget to workers | `202 Accepted` | `POST /actors/{group}/cast` · `app.cast` |
| Raft-linearizable read | `200 OK` | `app.propose` / SM `query` (not HTTP yet) |

## Delivery semantics

crafty delivers **at-least-once**. A job is acked only after the handler returns
`Ok`, so any crash, lease expiry, or `nack` between "handler ran" and "ack landed"
redelivers the job. There is no exactly-once delivery mode, and there will not be
one — see [job-queue](../decisions/job-queue.md). Exactly-once is a property you
build in the handler, not a flag you set on the queue.

What that splits into:

| Guaranteed by crafty | Your handler must do |
|----------------------|----------------------|
| A job is never silently lost once `enqueue` returns | Tolerate seeing the same job **more than once** |
| A job is delivered to one worker at a time (lease) | Tolerate a **partially applied** previous attempt |
| Redelivery after lease expiry, `nack`, or worker crash | Make the side effect **idempotent or CAS-guarded** |
| `dedup_key` collapses duplicate *enqueues* into one job | Not assume `dedup_key` protects *processing* |
| Attempt ceiling → dead letter instead of infinite retry | Decide what a dead-lettered job means for your data |

The redelivery window is real: a handler that charges a card and then loses its
node before the ack will charge again on redelivery unless you guard it.

### Three layers of idempotency

Each layer stops duplicates at a different point. They compose; none is sufficient alone.

1. **Enqueue** — `EnqueueOptions::dedup_key` (HTTP: `?dedup=`). Two enqueues with
   the same key produce one job and return the same `JobId`. Stops *duplicate
   submissions* (client retries, double-clicked buttons). Does nothing about
   redelivery of an already-accepted job.
2. **Processing** — a CAS in an [`ActorStateStore`](stateful-workers.md) or your
   own state machine, keyed by something stable about the job. Stops *duplicate
   side effects* from redelivery. This is the layer that gives you effectively-once.
3. **Workflow step** — saga steps carry their own step keys ([workflows](workflows.md)),
   so a resumed workflow does not re-run committed steps.

### Effectively-once recipe

The pattern, in order — the ack is last, and that ordering is the whole point:

1. **Enqueue with a `dedup_key`** derived from the business event, not from the
   payload bytes (`order-4711:charge`, not a hash of the JSON).
2. **In the handler, CAS a marker** into a durable store before doing the work:
   `absent → processing`. If the marker already says `done`, return `Ok`
   immediately — this is the redelivery arriving, and returning `Ok` acks it.
3. **Do the side effect.**
4. **Mark `done` durably**, then return `Ok` so the job is acked.

A crash between 3 and 4 still redelivers, and the marker still says `processing`
— so the recipe is only as good as your ability to tell "in flight" from
"finished". Where the side effect is externally idempotent (an upsert, a PUT, a
provider-side idempotency key), let the external system settle it and keep the
marker as a fast path. Where it is not (charging a card), pass a provider
idempotency key derived from the same job key.

Worked implementations:

- [`crafty-store-redis/examples/idempotent_worker.rs`](../../crates/crafty-store-redis/examples/idempotent_worker.rs) — CAS in a shared store
- [`examples/stateful-workers/`](../../examples/stateful-workers/) — durable per-actor state
- [`examples/background-jobs/`](../../examples/background-jobs/) — `?dedup=` retry plus simulated redelivery

### Knowing you are a redelivery

A handler can take a second argument to receive [`JobContext`](../../crates/crafty-actor/src/queue.rs):

```rust
#[consumer("emails")]
async fn send_email(payload: &[u8], ctx: JobContext<'_>) -> Result<(), ()> {
    if ctx.is_redelivery() {
        tracing::warn!(job = ctx.job_id.0, attempts = ctx.attempts, "retrying");
    }
    Ok(())
}
```

It carries `job_id`, `lease_id`, `stream`, `attempts` (`1` on first delivery), and
the enqueue-time `dedup_key`. Single-argument handlers keep working unchanged.

`attempts > 1` tells you a previous attempt did not ack — **not** that it did
nothing. Treat it as a signal to log or to check your marker, never as permission
to skip work.

### Built-in effectively-once guard

[`ConsumerOpts::idempotency`](../../crates/crafty/src/consumer.rs) wires the recipe
above for you, backed by any [`ActorStateStore`](stateful-workers.md):

```rust
.jobs([JobOpts::new("emails")
    .idempotency(IdempotencyOpts::by_dedup_key(store, "idem:emails:"))
    .consumer(&SendEmailConsumer)])
```

It runs: check `done` → CAS-claim `processing` → your handler → mark `done` → ack.
A redelivery whose key is already `done` is acked without re-entering the handler.
Supply your own key with `IdempotencyOpts::new(store, prefix, |payload, ctx| …)`,
returning `None` to leave a job unguarded, and `.ttl(..)` to expire markers.

This is still not exactly-once. It is the recipe with the ordering enforced, and it
is only as strong as the store and the key you choose:

| Situation | Result |
|-----------|--------|
| Store unreachable | Job is nacked and retried — never acked on a failed marker write |
| Another worker holds `processing` | Job is nacked; the holder finishes it |
| Marker TTL expires before the last redelivery | Duplicate window reopens — size the TTL against lease × attempts |
| Handler crashes the process mid-side-effect | Marker stays `processing`; the job is redelivered but not auto-skipped |

Regression coverage: [`crafty/tests/consumer_idempotency.rs`](../../crates/crafty/tests/consumer_idempotency.rs)
asserts one side effect across a redelivery, with an unguarded control case.

### Spotting duplicates in production

Redelivery is invisible until you measure it. Three signals, all per `stream`:

| Signal | Kind | Means |
|--------|------|-------|
| `crafty_queue_redeliveries_total` | counter | Deliveries that were not the first attempt |
| `crafty_queue_job_attempts` | histogram | Attempt number per delivery (`1` = first) |
| `crafty_queue_redelivered_jobs` | gauge | Jobs in the queue that already failed an attempt |

The counter and histogram are recorded once per delivery from the queue lifecycle
hook; the gauge is sampled with the other queue depths.

A non-zero `crafty_queue_redelivered_jobs` is an **idempotency smell**, not
necessarily a bug: it means handlers on that stream are being re-run, so they had
better be safe to re-run. A steadily climbing
`crafty_queue_redeliveries_total` with a flat dead-letter count usually means a
handler that fails after its side effect — exactly the case the
[recipe](#effectively-once-recipe) is for.

The same number appears per stream in `/introspect/queues` and in the admin
dashboard's **Job queues** table, highlighted when non-zero.

```console
$ curl -s localhost:9080/introspect/queues | jq '.streams[]'
{ "stream": "emails", "pending": 0, "leased": 1, "dead_letter": 0,
  "oldest_pending_age_ms": 0, "redelivered": 2 }
```

### Attempt ceilings

`max_attempts` bounds redelivery. Per job it is an
[`EnqueueOptions`](../../crates/crafty-actor/src/queue.rs) field; per stream it is
a default, which is what HTTP enqueues and cron ticks get since they cannot pass
per-job options:

```rust
.jobs([JobOpts::new("emails")
    .default_max_attempts(5)   // unset-on-the-job → 5 attempts, then dead letter
    .http_enqueue(true)])
```

Resolution is: an explicit per-job `max_attempts` wins; otherwise the stream
default applies; `0` in either position means unlimited retries. Leaving both at
`0` means a poison job retries forever — set a ceiling on any stream reachable
from untrusted input.

## Autoscale

Leader supervisor can scale worker pool from queue depth:

```rust
.job_queue_autoscale::<EmailWorker>("emails", AutoscaleConfig { ... })
.job_queue_membership_autoscale("emails", MembershipAutoscaleConfig { ... })
```

Policy can persist in Meta-Raft ([job-queue](../decisions/job-queue.md)).

## Operations

| Concern | Action |
|---------|--------|
| Backup | Include `queue-*.redb` in `data_dir` ([backup-restore](../ops/backup-restore.md)) |
| Failover | E2E `./e2e/queue.sh`; follower leases after leader loss |
| Compaction | Automatic every N acks on queue file |
| Poison messages | `nack` / lease timeout → redelivery; set `max_attempts` per job or [`default_max_attempts`](#attempt-ceilings) per stream → dead letter |
| Dead letter retry | `app.requeue_dead_letter("stream", job_id)` or HTTP requeue route |

## Examples & tests

| Asset | Purpose |
|-------|---------|
| [`examples/background-jobs/`](../../examples/background-jobs/) | Background jobs showcase — HTTP `202`, `#[consumer]` |
| `e2e/queue.sh` | Real QUIC/mTLS, follower worker + leader failover |
| `crafty/tests/queue.rs` | Integration |
| `crafty/tests/consumer.rs` | `#[crafty::consumer]` + `spawn_consumer` |
| `crafty/tests/consumer_idempotency.rs` | Redelivery → one side effect (guard + control) |
| `crafty/tests/http_jobs.rs` | HTTP enqueue, batch, DLQ requeue |

See [examples/background-jobs/](../../examples/background-jobs/) for the full [`JobOpts`](../../crates/crafty/src/job_opts.rs) showcase.

## Related

- [job-queue](../decisions/job-queue.md) — ADR
- [stateful-workers](stateful-workers.md) — durable handler state, and the CAS layer for [effectively-once](#effectively-once-recipe)
- [workflows](workflows.md) — saga step → enqueue, step keys as a third idempotency layer
- [backlog.md](../backlog.md) — B-02, B-03, B-13
