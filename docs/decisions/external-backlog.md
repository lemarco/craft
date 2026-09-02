# External backlog port

**Status:** Accepted (implemented)

## Context

Tier C ([`JobQueue`](../../crates/crafty-actor/src/queue.rs)) is an **in-flight window**: durable lease/ack semantics, leader replication, autoscale hooks. Many teams already store the **authoritative backlog** in Postgres or MySQL (`status = pending`, business columns, operator dashboards).

Today each such team reimplements the same glue:

1. Leader-elected top-up loop (`claim` → enqueue with dedup key)
2. Bounded in-flight window sizing
3. Settlement back to the source on terminal outcomes
4. A honest depth signal for [`AutoscalePolicy`](../../crates/crafty-actor/src/queue_autoscale.rs) (otherwise autoscaler sees only the small redb window)

## Decision

Add an [`ExternalBacklog`](../../crates/crafty-actor/src/external_backlog.rs) port:

| Method | Role |
|--------|------|
| `depth()` | Outstanding demand in the source of truth → worker/membership autoscale |
| `claim(max)` | Leader claims up to `max` items (impl owns `SKIP LOCKED` / CAS) |
| `settle(key, outcome)` | Terminal callback after tier-C ack or dead-letter |

Product wiring:

```rust
JobOpts::new("imports")
    .backlog(Arc::new(pg_backlog), BacklogFeedOpts::default().pending_target_per_consumer(2))
    .consumer(&ImportConsumer)
```

Runtime behaviour ([`run_backlog_feeder`](../../crates/crafty-actor/src/external_backlog.rs)):

- **Leader only** — same gate as cron schedules and queue mutations
- Target in-flight: `pending_target_per_consumer × consumer_instances`
- Top-up: `claim(need)` → `enqueue_opts(dedup_key = item.key)`
- **Settle** on ack, nack, and lease-timeout **reclaim** when a dedup key is present — durable **outbox** at `{data_dir}/backlog-settle-outbox.redb` + leader [`run_backlog_settle_drainer`](../../crates/crafty-actor/src/external_backlog.rs)
- **Autoscale** reads `depth()` when a backlog is registered for the stream; otherwise falls back to tier-C `pending + leased`

Optional adapter: [`crafty-backlog-postgres`](../../crates/crafty-backlog-postgres/) (`FOR UPDATE SKIP LOCKED`).

## Consequences

- crafty fits teams with an existing work table without moving backlog into redb
- HTTP `POST /jobs/*` and external backlog can coexist (direct enqueue bypasses feeder)
- Settlement is **at-least-once** via the settle outbox; `ExternalBacklog::settle` should be idempotent for repeated `Done` / terminal outcomes
- `consumer_instances` in [`BacklogFeedOpts`](../../crates/crafty-actor/src/external_backlog.rs) is static at registration — multi-node apps should set it to total consumer loops cluster-wide

## Alternatives considered

| Option | Verdict |
|--------|---------|
| Require all backlog in tier C | Rejected — poor fit for existing Postgres/MySQL deployments |
| User-written feeder only | Rejected — duplicates leader election, dedup, autoscale wiring |
| Put backlog rows in Raft log | Rejected — R1 write ceiling ([future-work-and-risks](future-work-and-risks.md)) |
