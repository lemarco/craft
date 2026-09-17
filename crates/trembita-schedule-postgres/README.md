# trembita-schedule-postgres

PostgreSQL [`ScheduleSource`](https://docs.rs/trembita-jobs/latest/trembita_jobs/trait.ScheduleSource.html)
adapter for dynamic recurring jobs.

## Product apps — use the facade

```toml
trembita = { version = "0.6", features = ["schedule-postgres"] }
```

```rust
use trembita::{PgScheduleSource, ScheduleSourceOpts, SchedulePoll};
```

See [schedule-source](../../docs/decisions/schedule-source.md) and [external-backlog](../../docs/decisions/external-backlog.md) (same optional-adapter pattern).

## Expected schema (default)

```sql
CREATE TABLE trembita_schedules (
    name TEXT PRIMARY KEY,
    cron TEXT NOT NULL DEFAULT '',
    payload BYTEA NOT NULL DEFAULT '\x',
    priority SMALLINT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 0,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    every_days INT NOT NULL DEFAULT 0,
    anchor_ms BIGINT NOT NULL DEFAULT 0
);
```

Wire on [`AppManifest`](../../crates/trembita/src/app/manifest.rs) (or builder) with [`.schedule_source`](https://docs.rs/trembita/latest/trembita/struct.TrembitaAppBuilder.html):

```rust
.schedule_source(ScheduleSourceOpts {
    stream: "jobs".into(),
    source: Arc::new(PgScheduleSource::connect(&database_url, "trembita_schedules").await?),
    poll: SchedulePoll::secs(30),
})
```

**Regression (B-41):**

```bash
./scripts/test-fast.sh -p trembita-schedule-postgres --lib b41_
./scripts/test-fast.sh -p trembita --test schedule_source b41_
```

Index: [capabilities § B-41](../../docs/scenarios/capabilities.md#product-surface-gaps-b-41) · [schedule-source § B-41](../../docs/decisions/schedule-source.md#postgres-adapter-b-41).
