# trembita-backlog-postgres

PostgreSQL [`ExternalBacklog`](https://docs.rs/trembita-jobs/latest/trembita_jobs/trait.ExternalBacklog.html)
adapter for trembita job queue streams.

## Product apps — use the facade

```toml
trembita = { version = "0.3", features = ["external-backlog"] }
```

```rust
use trembita::{PgBacklog, JobOpts, BacklogFeedOpts};
```

See [facade ADR](../../docs/decisions/facade.md) and [external-backlog](../../docs/decisions/external-backlog.md).

Direct dependency on `trembita-backlog-postgres` is for advanced/workspace use.

## Expected schema (default)

```sql
CREATE TABLE trembita_jobs (
    id TEXT PRIMARY KEY,
    payload BYTEA NOT NULL,
    priority SMALLINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    error TEXT,
    attempts INT NOT NULL DEFAULT 0
);
CREATE INDEX trembita_jobs_pending ON trembita_jobs (status) WHERE status = 'pending';
```

Wire with [`JobOpts::backlog`](https://docs.rs/trembita/latest/trembita/struct.JobOpts.html):

```rust
JobOpts::new("imports")
    .backlog(
        Arc::new(PgBacklog::connect(&database_url, "trembita_jobs").await?),
        BacklogFeedOpts::default().pending_target_per_consumer(2),
    )
    .consumer(&ImportConsumer)
```
