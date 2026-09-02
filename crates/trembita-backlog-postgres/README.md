# trembita-backlog-postgres

PostgreSQL [`ExternalBacklog`](https://docs.rs/trembita-actor/latest/trembita_actor/trait.ExternalBacklog.html)
adapter for trembita job queue streams.

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

See [external-backlog](../../docs/decisions/external-backlog.md).
