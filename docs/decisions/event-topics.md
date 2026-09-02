# Durable event topics

**Status:** Accepted (2026-09-02)

## Context

Background job queues are point-to-point: one `enqueue`, one consumer `lease`. Event-driven
applications need **pub/sub**: one domain fact must reach several independent consumers
(analytics, search reindex, audit), each with its own read position.

Publishing the same payload into three queue streams pushes subscriber knowledge into the
publisher and breaks when a fourth subscriber appears.

## Decision

Add a separate **EventTopic** entity (not a queue mode):

| Concern | Queue | Topic |
|---------|-------|-------|
| Delivery | One consumer per message | All subscriptions see every event |
| Cursor | N/A (job removed on ack) | Per subscription |
| Compaction | On ack | When **all** subscriptions pass `min(cursor)` |
| Wire | `/raft/v1/queue/*` | `/raft/v1/topic/*` |

### API (facade)

```rust
.topics([TopicOpts::topic("platform.events")
    .subscriptions(["analytics", "search-index", "audit"])])

app.publish("platform.events", payload).await?;

#[consumer("platform.events", subscription = "analytics")]
async fn on_event(payload: &[u8], ctx: TopicContext<'_>) -> Result<(), MyError> { … }
```

Subscriptions are declared at **build time** for v1 (code property, not operational data).

### Retention

Compaction uses `min(cursor)` across subscriptions. A slow subscription holds events for
everyone until it catches up or hits a **retention threshold** (`max_event_age`,
`max_retained_events`). When exceeded, the lagging subscription cursor is force-advanced;
`retention_discards` is incremented (visible in metrics). This is explicit data loss for
that subscription — not silent truncation.

Removing a subscription advances `min(cursor)` if it was the minimum.

### Replication

Leader applies mutations locally, then synchronously replicates `TopicReplicateOp` batches to
reachable voters (same pattern as `QueueService`). Cursors and pending/lease state survive
leader election.

### Out of scope (v1)

- Kafka-style partitioning or external offset replay
- Cross-cluster topics
- Dynamic subscription registration at runtime
- **Transactional publish** with application state — use the [transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html) pattern: write the event in the same DB transaction as domain data, then publish from the outbox. Topics alone do not guarantee atomicity with your database.

## Consequences

- Publishers stay unaware of subscribers.
- One stuck subscription can grow disk until retention fires; operators must monitor per-subscription lag.
- Ordinary job streams are unchanged.

## References

- [`crates/trembita-events/src/topic.rs`](../../crates/trembita-events/src/topic.rs) — port
- [`crates/trembita-events/src/redb_topic.rs`](../../crates/trembita-events/src/redb_topic.rs) — storage
- [`crates/trembita-events/src/topic_service.rs`](../../crates/trembita-events/src/topic_service.rs) — leader service
- [`docs/scenarios/event-topics.md`](../scenarios/event-topics.md) — product scenario
