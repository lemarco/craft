# crafty-backlog-postgres

PostgreSQL [`ExternalBacklog`](https://docs.rs/crafty-actor/latest/crafty_actor/trait.ExternalBacklog.html)
adapter for crafty job queue streams.

## Expected schema (default)

```sql
CREATE TABLE crafty_jobs (
    id TEXT PRIMARY KEY,
    payload BYTEA NOT NULL,
    priority SMALLINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    error TEXT,
    attempts INT NOT NULL DEFAULT 0
);
CREATE INDEX crafty_jobs_pending ON crafty_jobs (status) WHERE status = 'pending';
```

Wire with [`JobOpts::backlog`](https://docs.rs/crafty/latest/crafty/struct.JobOpts.html):

```rust
JobOpts::new("imports")
    .backlog(
        Arc::new(PgBacklog::connect(&database_url, "crafty_jobs").await?),
        BacklogFeedOpts::default().pending_target_per_consumer(2),
    )
    .consumer(&ImportConsumer)
```

See [external-backlog](../../docs/decisions/external-backlog.md).
