# trembita-events-postgres

PostgreSQL [`EventOutboxSource`](https://docs.rs/trembita-events/latest/trembita_events/trait.EventOutboxSource.html)
for the [transactional outbox](https://docs.rs/trembita/latest/trembita/struct.TopicOpts.html) drainer.

## Product apps — use the facade

```toml
trembita = { version = "0.5", features = ["domain-outbox"] }
```

```rust
use trembita::{PgEventOutboxSource, TopicOpts, EventOutboxDrainOpts};
```

See [facade ADR](../../docs/decisions/facade.md) and [event-outbox](../../docs/decisions/event-outbox.md).

Direct dependency on `trembita-events-postgres` is for advanced/workspace use.

## Default table shape

```sql
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_events_unpublished_idx ON outbox_events (created_at)
    WHERE published_at IS NULL;
```

Wire into [`TrembitaApp::topics`](https://docs.rs/trembita/latest/trembita/struct.TrembitaAppBuilder.html):

```rust
use std::sync::Arc;
use trembita::{EventOutboxDrainOpts, TopicOpts, TrembitaApp, PgEventOutboxSource};

let outbox = PgEventOutboxSource::connect(&database_url).await?;
TrembitaApp::builder()
    .topics([TopicOpts::topic("platform.events")
        .outbox(Arc::new(outbox), EventOutboxDrainOpts::default())])
    // ...
```

Custom column names via [`PgEventOutboxSchema`].
