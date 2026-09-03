# Transactional event outbox port

**Status:** Accepted (implemented)

## Context

[`EventTopic`](../../crates/trembita-events/src/topic.rs) is durable pub/sub inside trembita — not a
participant in the application database transaction. Atomic “update domain row + emit event” requires
the [transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html) in
**your** store; the topic receives events after commit ([event-topics](event-topics.md)).

Today each team hand-rolls the same leader drainer:

1. Read unpublished rows after cursor
2. Publish to the topic
3. Mark rows published / advance cursor
4. Run only on the Raft leader
5. Tolerate at-least-once (idempotent publish + idempotent `mark_published`)

This mirrors the gap [`ScheduleSource`](schedule-source.md) and [`ExternalBacklog`](external-backlog.md)
closed for schedules and job backlogs.

## Decision

Add an [`EventOutboxSource`](../../crates/trembita-events/src/event_outbox.rs) port:

| Method | Role |
|--------|------|
| `poll(after, max)` | Return unpublished rows strictly after `after` (exclusive) |
| `mark_published(ids)` | Mark rows published after successful topic publish — idempotent |

Product wiring:

```rust
TrembitaApp::builder()
    .topics([TopicOpts::topic("platform.events")
        .subscriptions(["analytics"])
        .outbox(Arc::new(pg_outbox), EventOutboxDrainOpts::default())])
```

Or imperative:

```rust
    .event_outbox_source("platform.events", Arc::new(pg_outbox), EventOutboxPoll::secs(1))
```

Runtime behaviour ([`run_event_outbox_drainer`](../../crates/trembita-events/src/event_outbox.rs)):

- **Leader only** — same gate as backlog feeder and schedule reconcile
- **Local topic publish** — leader writes to `RedbEventTopic` directly (mirrors [`run_backlog_feeder`](../../crates/trembita-jobs/src/external_backlog.rs)); cluster clients use `ClusterEventTopic` as usual
- **Cursor checkpoint** — `{data_dir}/event-outbox-cursors.redb` stores last published id per topic (restart-safe)
- **At-least-once** — crash after publish but before `mark_published` may duplicate topic events; subscribers must be idempotent
- **Stop on publish failure** — remaining batch retried next tick

`trembita` does not depend on Postgres — adapters live in application code (same as `ScheduleSource`).

## Consequences

- Event-driven apps stop copying leader-elected outbox drainers
- Direct [`TrembitaApp::publish`](../../crates/trembita/src/app.rs) and outbox drainer can coexist on the same topic
- Cursor checkpoint is an optimization; source `mark_published` is the source of truth for unpublished rows

## Related

- [leader-task.md](leader-task.md) — execution primitive shared by the drainer loop
- [event-topics.md](event-topics.md) — topic semantics and explicit non-atomicity with app DB

## Alternatives considered

| Option | Verdict |
|--------|---------|
| Document the hand-rolled drainer only | Rejected — library ADR already points operators at outbox; port closes the loop |
| Put outbox rows in Raft / topic log | Rejected — R1 write ceiling; app DB owns domain transaction |
| Imperative publish hook only (no port) | Rejected — misses cursor + leader wiring |
