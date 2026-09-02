# Dynamic schedule source

**Status:** Accepted (implemented)

## Context

Recurring jobs ([`RecurringJob`](../../crates/crafty-actor/src/queue_schedule.rs)) were registered only at build time via [`.cron()`](../../crates/crafty/src/app.rs). Operators who store schedules in Postgres (admin UI toggles) had to reimplement the leader-only ticker, replication, and restart survival crafty already provides for job queues.

This mirrors the gap [`ExternalBacklog`](external-backlog.md) closed for job backlogs: **data assumed static config** instead of a port.

## Decision

Add a [`ScheduleSource`](../../crates/crafty-actor/src/schedule_source.rs) port:

| Method | Role |
|--------|------|
| `schedules()` | Return the desired recurring-job set for this poll |

Product wiring:

```rust
CraftyApp::builder()
    .queue([QueueOpts::new("jobs", lease)])
    .schedule_source("jobs", Arc::new(pg_schedules), SchedulePoll::secs(10))
```

Runtime behaviour:

- **Leader only** — polled on the same loop as cron ticks ([`run_queue_schedule_ticker`](../../crates/crafty-actor/src/queue_schedule.rs))
- **Diff reconcile** — upsert new/changed, remove disappeared names, honour `enabled`
- **Replicate** — mutations use existing `QueueReplicateOp::UpsertSchedule` / `RemoveSchedule`
- **Errors never clear** — log, keep last good redb set, retry next poll
- **Bootstrap `Ok([])`** — first successful empty snapshot does not wipe schedules already in redb (restart-safe)
- **`.cron()`** — unchanged API; implemented as [`StaticScheduleSource`](../../crates/crafty-actor/src/schedule_source.rs) merged into the same reconcile path

## Consequences

- Apps with DB-backed schedules avoid bespoke leader-elected tickers
- crafty does not depend on Postgres — adapters live in application code
- Imperative `CraftyApp::upsert_schedule` deferred; the port covers the “notify crafty after DB write” case via polling

## Alternatives considered

| Option | Verdict |
|--------|---------|
| HTTP schedule admin on crafty | Rejected — operator UI stays in the app |
| Keep build-time-only schedules | Rejected — forces redeploy for checkbox toggles |
| Imperative API only (no port) | Acceptable interim; port is the goal |
