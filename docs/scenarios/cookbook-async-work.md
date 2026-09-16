# Cookbook — async work composition

Copy-paste templates for **trigger → unit of work → engine**. trembita keeps separate engines (queue, topic, saga); this doc unifies **how you wire them in app code**. See [triggers-and-pipelines](triggers-and-pipelines.md) for the mental model.

**Related:** [background-jobs](background-jobs.md) · [workflows](workflows.md) · [event-topics](event-topics.md) · [state-placement](state-placement.md)

## Recommended layout

| Stream / surface | Role |
|------------------|------|
| `orchestration` | Cron bootstrap only — short jobs, [`WorkTrigger`](../../crates/trembita-jobs/src/work_trigger.rs) or your enum |
| `seo-parse`, `emails`, … | Domain work units |
| `.workflows([…])` | Multi-step sagas with compensation |
| `platform.events` topic | Fan-out after DB commit (outbox) |

Define **one app enum per stream** when you outgrow JSON triggers:

```rust
enum SeoJob {
    StartRun { run_id: Uuid },
    ParseKeyword { run_id: Uuid, keyword_id: i64 },
}
```

## Template A — Cron → workflow

**Simplest (recommended):** one builder call — queue, cron, and dispatcher consumer are wired for you.

```rust
use trembita::{ScheduledWorkflowOpts, TrembitaAppBuilder, WorkflowOpts, journal_workflow};

fn builder(b: TrembitaAppBuilder) -> TrembitaAppBuilder {
    b.scheduled_workflows(
        ScheduledWorkflowOpts::new().workflow("weekly", "0 3 * * 1", "weekly-report"),
    )
    .workflows([WorkflowOpts::new(plan_weekly, journal_workflow)])
}
```

No `#[consumer("orchestration")]` unless you put **custom** bytes on the same stream.

**Manual compose** (same behavior, more control):

```rust
use trembita::{CronOpts, QueueOpts, TrembitaAppBuilder};

b.queue([QueueOpts::new("orchestration", Duration::from_secs(300))])
    .cron([CronOpts::starts_workflow(
        "orchestration", "weekly", "0 3 * * 1", "weekly-report",
    )])
// + your consumer calling dispatch_work_trigger
```

Calendar maintenance → workflow:

```rust
ScheduledWorkflowOpts::new()
    .workflow_every_calendar_days_at_utc("log-cleanup", 3, 4, 0, "log-cleanup-saga")
```

## Template B — Cron → pipeline (follow-up enqueue)

Start a run without a saga — enqueue the first unit job on another stream.

```rust
.cron([CronOpts::starts_enqueue(
    "orchestration",
    "weekly",
    "0 3 * * 1",
    "seo-parse",
    br#"{"action":"start_run"}"#,
)])
```

Same consumer: `dispatch_work_trigger` handles [`WorkTrigger::Enqueue`](../../crates/trembita-jobs/src/work_trigger.rs). Or decode your `enum SeoJob` after `NotTrigger`.

## Template C — Domain commit → queue (Raft)

After a successful `propose`, enqueue — **not** cron.

```rust
// inside SM command handler or actor after replicated write
app.enqueue("emails", &encode_email_job(&order_id)).await?;
```

Use `dedup_key` in [`EnqueueOptions`](../../crates/trembita-jobs/src/queue/mod.rs) for idempotent side effects.

## Template D — Postgres outbox → topic → queue

1. Same SQL transaction: update row + insert outbox row.
2. [`EventOutboxSource`](../decisions/event-outbox.md) drainer publishes to `platform.events`.
3. Topic subscriber enqueues:

```rust
async fn on_order_created(event: &[u8], app: Arc<TrembitaApp>) -> Result<(), SubErr> {
    app.enqueue("notifications", &build_email_payload(event)).await?;
    Ok(())
}
```

## Template E — Saga step → queue or actor

Inside a workflow step closure:

```rust
// enqueue async side effect
app.enqueue("exports", &payload).await?;

// or synchronous actor RPC
app.send_actor("billing", &cmd).await?;
```

Journal lives in Meta-Raft; the queue holds **units** of async work between steps.

## Template F — One-shot at T

Not recurring — [`enqueue_at`](../../crates/trembita/src/app/runtime.rs) or HTTP `run_at_ms`. See [triggers-and-pipelines § one-shot](triggers-and-pipelines.md#one-shot-schedule-at-time-t).

## When to pick which engine

| Need | Engine |
|------|--------|
| Shared backlog, many workers, retry | Job queue |
| Fan-out, independent subscribers | Event topic (+ outbox for atomic emit) |
| Ordered steps + compensate | Workflow / saga |
| Talk to one actor now | Mailbox `ask` / session |
| Authoritative domain row | Raft SM or **your** Postgres |

## Anti-patterns (recap)

- One cron handler that loops for hours — use self-enqueue or external backlog.
- Platform `JobKind` — use streams + payload enum.
- Cron for reactive email — enqueue on commit.

## Examples

| Path | Shows |
|------|--------|
| [cron_bootstrap.rs](../../examples/background-jobs/src/cron_bootstrap.rs) | `CronOpts::starts_workflow` |
| [examples/workflows/](../../examples/workflows/) | Saga + HTTP `/workflows/run` |
| [triggers-and-pipelines.md](triggers-and-pipelines.md) | Diagram + operator HTTP |
