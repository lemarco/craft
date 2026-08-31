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

### 1. Builder — register queue + recurring schedules

```rust
use std::time::Duration;

use crafty::{CraftyApp, CronOpts, QueueOpts, RecurringJob, RunOpts};

CraftyApp::builder()
    .queue([QueueOpts::new("emails", Duration::from_secs(300))])
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
    .run(RunOpts::default().with_wait_queue("emails"))
    .await?;
```

Cluster-level autoscale, sharded streams, priority, dedup: see [background-jobs showcase](../../examples/background-jobs/) and [job-queue](../decisions/job-queue.md#v2-features).

### 2. Enqueue (from any node)

```rust
use crafty::EnqueueOptions;

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
| Poison messages | `nack` / lease timeout → redelivery; set `max_attempts` → dead letter |
| Dead letter retry | `app.requeue_dead_letter("stream", job_id)` or HTTP requeue route |

## Examples & tests

| Asset | Purpose |
|-------|---------|
| [`examples/background-jobs/`](../../examples/background-jobs/) | Tier C showcase — HTTP `202`, `#[consumer]` |
| `e2e/queue.sh` | Real QUIC/mTLS, follower worker + leader failover |
| `crafty/tests/queue.rs` | Integration |
| `crafty/tests/consumer.rs` | `#[crafty::consumer]` + `spawn_consumer` |
| `crafty/tests/http_jobs.rs` | HTTP enqueue, batch, DLQ requeue |

## Future polish

Declarative `.jobs(...)` / attribute macros for workers remain aspirational. Today:

```rust
CraftyApp::builder()
    .queue([QueueOpts::new("emails", Duration::from_secs(300))])
    .consumer(SendEmailConsumer, ConsumerOpts::default())
    .gateway(GatewayOpts::new("127.0.0.1:8090".parse()?).with_jobs_api(true))
    .run(RunOpts::default().with_wait_queue("emails"))
    .await?;
```

See [examples/background-jobs/](../../examples/background-jobs/).

## Related

- [job-queue](../decisions/job-queue.md) — ADR
- [stateful-workers](stateful-workers.md) — durable handler state
- [workflows](workflows.md) — saga step → enqueue
- [backlog.md](../backlog.md) — B-02, B-03
