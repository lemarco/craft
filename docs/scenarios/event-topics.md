# Event topics scenario

Durable pub/sub for domain events — one publish, many named subscriptions with independent
cursors. See [event-topics](../decisions/event-topics.md).

## When to use

- Several parts of the system must react to the same fact (created order → analytics, search, audit).
- Subscribers evolve independently; the publisher must not list them.

## When not to use

- Point-to-point work distribution → [background jobs](background-jobs.md) (`JobQueue`).
- Atomic “update DB + emit event” → transactional outbox in **your** database, not the topic alone.

## Setup

```rust
use std::time::Duration;
use trembita::{TopicOpts, TrembitaAppBuilder, TopicContext};

TrembitaAppBuilder::new()
    .data_dir("/var/lib/myapp")
    .topics([TopicOpts::topic("platform.events")
        .lease(Duration::from_secs(300))
        .subscriptions(["analytics", "search-index", "audit"])])
    // …
```

## Publish

```rust
app.publish("platform.events", postcard::to_allocvec(&event)?).await?;
```

## Subscribe

```rust
#[trembita::consumer("platform.events", subscription = "analytics")]
async fn track(payload: &[u8], ctx: TopicContext<'_>) -> Result<(), MyError> {
    if ctx.is_redelivery() {
        // at-least-once — handler must be idempotent
    }
    Ok(())
}
```

Spawn with the same `spawn_consumer` / `ConsumerOpts` path as queue workers.

## Semantics

- **At-least-once** per subscription (lease / ack / nack, attempts, dead letter) — same family as queues.
- **Slow subscriber** does not block others from leasing; it only delays **compaction** for shared event storage.
- **Retention** force-advances a lagging cursor when age or retained event count exceeds configured limits; check `TopicMetrics` / `retention_discards`.
- **Late subscription** with `SubscriptionStart::Latest` (default when added via builder) sees only new events; `Earliest` fans out the retained backlog.

## Metrics

Call `EventTopic::metrics()` (cluster client or local redb) for:

- `event_count`, `head`, `compact_head`, `oldest_event_age`
- Per subscription: `lag`, `pending`, `leased`, `retention_discards`

## Leader failover

Topic mutations replicate to voters before the leader acks. After election, the new leader
continues from replicated cursors (see `trembita-actor/tests/topic_failover.rs`).
