# Background jobs — distributed durable queue

**Pattern:** Sidekiq / Celery-style async work — enqueue now, workers consume later, survive crash and leader failover.

**Status:** Runtime **shipped** (0.1.0). Product helpers (`CraftyApp`, HTTP `202`) — [backlog B-03](../backlog.md).

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

## Quick start (current API)

### 1. Builder — register queue + workers

```rust
use std::sync::Arc;
use std::time::Duration;

use crafty::{CraftyCluster, NodeId, run_queue_consumer, WorkerId};
use crafty::actor::{EnqueueOptions, UserActor, remote_actor};

let cluster = CraftyCluster::builder(node_id, my_state_machine)
    .data_dir("/var/lib/crafty")
    .job_queue("emails", Duration::from_secs(300))  // lease timeout
    .auto_workers([AutoWorkerSpec::new("email-workers", WorkerConfig::default())])
    .start_quic(listen, members, transport)
    .await?;
```

Sharded streams, priority, dedup, autoscale: see `job_queue_cluster` example and [job-queue](../decisions/job-queue.md#v2-features).

### 2. Enqueue (from any node)

```rust
let queue = cluster.job_queue("emails").expect("stream registered");

let job_id = queue.enqueue(b"{\"to\":\"user@example.com\"}").await?;

// Priority, dedup, delayed:
queue.enqueue_opts(payload, EnqueueOptions::dedup_key("invoice-7")).await?;
```

### 3. Consume (worker loop)

Use [`run_queue_consumer`](../../crates/crafty/src/queue.rs) or manual lease/ack:

```rust
run_queue_consumer(
    queue,
    WorkerId { node: cluster.node_id(), instance: 0 },
    |payload| async move {
        handle_email(payload).await?;
        Ok(())
    },
).await?;
```

**Idempotency:** use `EnqueueOptions::dedup_key` and/or store processed ids in your `StateMachine` or `ActorStateStore`.

### 4. HTTP mapping (recommended)

| Intent | Response | crafty API |
|--------|----------|------------|
| Accept work | `202 Accepted` + `job_id` | `queue.enqueue(...)` |
| Sync read | `200 OK` | `query` or `ask` |

Backlog **B-03:** shipped helper crate/module for axum/hyper routes.

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
| Poison messages | `nack` / lease timeout → redelivery; handler must be idempotent |

## Examples & tests

| Asset | Purpose |
|-------|---------|
| `examples/job_queue_worker.rs` | 3-node, follower consumer, leader failover |
| `examples/job_queue_cluster.rs` | Sharded, priority, dedup, autoscale |
| `e2e/queue.sh` | Real QUIC/mTLS |
| `crafty/tests/queue.rs` | Integration |

## Target product API (backlog)

```rust
CraftyApp::from_env()
    .jobs("emails", EmailWorker::handle)
    .workers(EmailWorker, scale: Auto)
    .run()
    .await?;

app.enqueue("emails", SendEmail { .. }).await?;
```

## Related

- [job-queue](../decisions/job-queue.md) — ADR
- [stateful-workers](stateful-workers.md) — durable handler state
- [workflows](workflows.md) — saga step → enqueue
- [backlog.md](../backlog.md) — B-02, B-03
