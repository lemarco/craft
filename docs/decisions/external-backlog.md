# External backlog port

**Status:** Accepted (implemented)

## Context

The job queue ([`JobQueue`](../../crates/trembita-jobs/src/queue.rs)) is an **in-flight window**: durable lease/ack semantics, leader replication, autoscale hooks. Many teams already store the **authoritative backlog** in Postgres or MySQL (`status = pending`, business columns, operator dashboards).

Today each such team reimplements the same glue:

1. Leader-elected top-up loop (`claim` → enqueue with dedup key)
2. Bounded in-flight window sizing
3. Settlement back to the source on terminal outcomes
4. A honest depth signal for [`AutoscalePolicy`](../../crates/trembita-jobs/src/queue_autoscale.rs) (otherwise autoscaler sees only the small redb window)

## Decision

Add an [`ExternalBacklog`](../../crates/trembita-jobs/src/external_backlog.rs) port:

| Method | Role |
|--------|------|
| `depth()` | Outstanding demand in the source of truth → worker/membership autoscale |
| `claim(max)` | Leader claims up to `max` items (impl owns `SKIP LOCKED` / CAS) |
| `settle(key, outcome)` | Terminal callback after queue ack or dead-letter |

Product wiring:

```rust
JobOpts::new("imports")
    .backlog(Arc::new(pg_backlog), BacklogFeedOpts::default().pending_target_per_consumer(2))
    .consumer(&ImportConsumer)
```

Runtime behaviour ([`run_backlog_feeder`](../../crates/trembita-jobs/src/external_backlog.rs)):

- **Leader only** — same gate as cron schedules and queue mutations
- Target in-flight: `pending_target_per_consumer × consumer_instances` (recomputed each poll)
- Top-up: `claim(need)` → `enqueue_opts(dedup_key = item.key)`
- **Settle** on ack, nack, and lease-timeout **reclaim** when a dedup key is present — durable **outbox** at `{data_dir}/backlog-settle-outbox.redb` + leader [`run_backlog_settle_drainer`](../../crates/trembita-jobs/src/external_backlog.rs)
- **Autoscale** reads `depth()` when a backlog is registered for the stream; otherwise falls back to queue `pending + leased`

Optional adapter: [`trembita-backlog-postgres`](../../crates/trembita-backlog-postgres/) (`FOR UPDATE SKIP LOCKED`).

## Consequences

- trembita fits teams with an existing work table without moving backlog into redb
- HTTP `POST /jobs/*` and external backlog can coexist (direct enqueue bypasses feeder)
- Settlement is **at-least-once** via the settle outbox; `ExternalBacklog::settle` should be idempotent for repeated terminal outcomes on the same generation
- **`Settlement::Done`** carries the queue attempt counter at ack (`0` on first-try success). Adapters (e.g. [`PgBacklog`](../../crates/trembita-backlog-postgres/src/lib.rs)) should apply `Done` only when the row is still **claimed** and `attempts` matches — stale outbox entries after key reuse are ignored
- **`dedup_key = item.key`** ties each claimed row to one in-flight queue job. On ack the queue **releases** the dedup slot (job removed) and the drainer settles `Done` — the source row can be claimed again. On **dead letter** the external source is settled, but the queue **still holds** the dedup key until the dead-letter job is requeued or removed; see [background-jobs § `dedup_key` lifecycle](../scenarios/background-jobs.md#dedup_key-lifecycle)
- `consumer_instances` defaults to [`ConsumerCount::Live`](../../crates/trembita-jobs/src/external_backlog.rs) — `reachable_nodes × per_node`, where `per_node` comes from `JobOpts::instances()` at registration. Use `ConsumerCount::Fixed(n)` to pin a static cluster-wide count

## Alternatives considered

| Option | Verdict |
|--------|---------|
| Require all backlog in the job queue | Rejected — poor fit for existing Postgres/MySQL deployments |
| User-written feeder only | Rejected — duplicates leader election, dedup, autoscale wiring |
| Put backlog rows in Raft log | Rejected — R1 write ceiling ([future-work-and-risks](future-work-and-risks.md)) |
