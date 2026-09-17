# Dynamic schedule source

**Status:** Accepted (implemented)

## Context

Recurring jobs ([`RecurringJob`](../../crates/trembita-jobs/src/queue_schedule.rs)) were registered only at build time via [`.cron()`](../../crates/trembita/src/app/mod.rs). Operators who store schedules in Postgres (admin UI toggles) had to reimplement the leader-only ticker, replication, and restart survival trembita already provides for job queues.

This mirrors the gap [`ExternalBacklog`](external-backlog.md) closed for job backlogs: **data assumed static config** instead of a port.

## Decision

Add a [`ScheduleSource`](../../crates/trembita-jobs/src/schedule_source.rs) port:

| Method | Role |
|--------|------|
| `schedules()` | Return the desired recurring-job set for this poll |

Product wiring:

```rust
TrembitaApp::builder()
    .queue([QueueOpts::new("jobs", lease)])
    .schedule_source("jobs", Arc::new(pg_schedules), SchedulePoll::secs(10))
```

Runtime behaviour:

- **Leader only** — polled on the same loop as cron ticks ([`run_queue_schedule_ticker`](../../crates/trembita-jobs/src/queue_schedule.rs))
- **Diff reconcile** — upsert new/changed, remove disappeared names, honour `enabled`
- **Replicate** — mutations use existing `QueueReplicateOp::UpsertSchedule` / `RemoveSchedule`
- **Errors never clear** — log, keep last good redb set, retry next poll
- **Bootstrap `Ok([])`** — first successful empty snapshot does not wipe schedules already in redb (restart-safe)
- **`.cron()`** — unchanged API; implemented as [`StaticScheduleSource`](../../crates/trembita-jobs/src/schedule_source.rs) merged into the same reconcile path

## Consequences

- Apps with DB-backed schedules avoid bespoke leader-elected tickers
- trembita does not depend on Postgres — adapters live in application code
- Imperative schedule admin: [`TrembitaApp::upsert_schedule`](../../crates/trembita/src/app/runtime.rs) / HTTP `PUT /jobs/{stream}/schedules/{name}` (see [triggers-and-pipelines](../scenarios/triggers-and-pipelines.md)); DB bulk sync still uses [`ScheduleSource`](../../crates/trembita-jobs/src/schedule_source.rs) polling
- Custom leader-only loops beyond schedules use [`trembita-assembly`](../../crates/trembita-assembly/src/builder/cluster/mod.rs) [`TrembitaClusterBuilder::on_leader`](../../crates/trembita-assembly/src/builder/cluster/mod.rs) / [`run_leader_loop`](../../crates/trembita-runtime/src/leader_task.rs) ([leader-task](leader-task.md)) — not product API

## Related

- [leader-task.md](leader-task.md) — leader-only loop primitive (shipped)
- [triggers-and-pipelines](../scenarios/triggers-and-pipelines.md) — `SchedulesApi` HTTP + facade `upsert_schedule`

## Alternatives considered

| Option | Verdict |
|--------|---------|
| HTTP schedule admin on trembita | **Shipped** — `SchedulesApi` on unified listener; app mounts routes + auth ([triggers-and-pipelines](../scenarios/triggers-and-pipelines.md)) |
| Keep build-time-only schedules | Rejected alone — use `.cron()` or [`ScheduleSource`](../../crates/trembita-jobs/src/schedule_source.rs) for DB sync |
| Imperative API only (no port) | Superseded — both port and HTTP/facade mutations ship |
