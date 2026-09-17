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
- Custom leader-only loops: [`TrembitaAppBuilder::on_leader`](../../crates/trembita/src/app/builder.rs) + [`LeaderLoopOpts`](../../crates/trembita-runtime/src/leader_task.rs) ([leader-task](leader-task.md)) — B-41 product surface

### Postgres adapter (B-41)

Optional crate [`trembita-schedule-postgres`](../../crates/trembita-schedule-postgres/) — facade feature **`schedule-postgres`** ([external-backlog](external-backlog.md) pattern). Core trembita stays Postgres-free; apps opt in via one dependency feature.

```toml
trembita = { version = "0.6", features = ["schedule-postgres"] }
```

```rust
use std::sync::Arc;
use trembita::{AppManifest, QueueOpts, SchedulePoll, ScheduleSourceOpts, TrembitaApp};
use trembita::PgScheduleSource;

AppManifest::new()
    .queue([QueueOpts::new("jobs", lease)])
    .schedule_source(ScheduleSourceOpts {
        stream: "jobs".into(),
        source: Arc::new(PgScheduleSource::connect(&database_url, "trembita_schedules").await?),
        poll: SchedulePoll::secs(30),
    });
```

Default table schema and column map: [`trembita-schedule-postgres/README.md`](../../crates/trembita-schedule-postgres/README.md). Dynamic table names pass through [`PgScheduleSource::ident`](../../crates/trembita-schedule-postgres/src/lib.rs) (SQL identifier guard).

**Related B-41 product APIs** (not schedule-specific): [`TrembitaConfigure::with_durable_mailbox`](../../crates/trembita/src/configure.rs), [`TrembitaAppBuilder::on_leader`](../../crates/trembita/src/app/builder.rs) — [capabilities § B-41](../scenarios/capabilities.md#product-surface-gaps-b-41).

### Automated regression (B-41)

| Scenario | Regression |
|----------|------------|
| `with_durable_mailbox` true/false/default | `b41_configure_durable_mailbox_scenarios_table` |
| Configure → cluster builder durable mailbox | `b41_durable_mailbox_applies_to_cluster_builder` |
| Boot spool file + builder alias | `b41_durable_mailbox_boot_creates_spool_redb`, `b41_builder_with_durable_mailbox_matches_configure_flag` |
| `on_leader` while Raft leader | `b41_on_leader_ticks_while_raft_leader` (`tests/app_cluster.rs`) |
| Manifest schedule source wiring | `b41_manifest_schedule_source_chains_into_builder`, `b41_schedule_source_manifest_boots_without_panic` |
| Postgres ident + default schema | `b41_rejects_invalid_table_ident`, `b41_sql_ident_validation_scenarios_table`, `b41_default_schema_matches_documented_columns` |

```bash
./scripts/test-fast.sh -p trembita --lib b41_
./scripts/test-fast.sh -p trembita-schedule-postgres --lib b41_
./scripts/test-fast.sh -p trembita --test app_cluster b41_
./scripts/test-fast.sh -p trembita --test schedule_source b41_
```

## Related

- [leader-task.md](leader-task.md) — leader-only loop primitive (shipped)
- [triggers-and-pipelines](../scenarios/triggers-and-pipelines.md) — `SchedulesApi` HTTP + facade `upsert_schedule`

## Alternatives considered

| Option | Verdict |
|--------|---------|
| HTTP schedule admin on trembita | **Shipped** — `SchedulesApi` on unified listener; app mounts routes + auth ([triggers-and-pipelines](../scenarios/triggers-and-pipelines.md)) |
| Keep build-time-only schedules | Rejected alone — use `.cron()` or [`ScheduleSource`](../../crates/trembita-jobs/src/schedule_source.rs) for DB sync |
| Imperative API only (no port) | Superseded — both port and HTTP/facade mutations ship |
