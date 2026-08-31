# Workflows — saga + job queue (mini-Temporal, no server)

**Pattern:** Multi-step business processes with forward steps and compensators; journal survives process restart; steps can call queue, actors, or Raft.

**Status:** **Shipped** in 0.2.x — `WorkflowBuilder`, `CraftyApp::run_workflow`, Meta-Raft journal, [examples/workflows/](../../examples/workflows/).

## When to use

- Onboarding, checkout, provisioning — **ordered steps** with rollback
- Partial failure must run **compensators** in reverse
- Process must **resume** after crash without double-charging

**Do not** run a separate Temporal/Camunda server — journal lives in **`group-meta.redb`** ([multi-raft](../decisions/multi-raft.md)).

## Architecture

```
Client / API
     │
     └── run_saga(plan, journal) ──► step 1 ──► step 2 ──► step 3
              │                         │          │
              │                         │          └── enqueue (tier C)
              │                         └── propose (tier A)
              │
              └── journal: MetaRaftSagaJournal / CompositeSagaJournal
                        (group-meta.redb)
```

Cross-shard: `run_keyed_saga`, optional [2PC](../decisions/multi-raft.md#cross-shard-transactions).

## Journal backends (no Redis)

| Journal | Storage | When |
|---------|---------|------|
| `MetaRaftSagaJournal` | Meta-Raft log | **Default** multi-node |
| `CompositeSagaJournal` | Meta-Raft + optional store | Mirror to store for ops visibility |
| `StoreSagaJournal` | `ActorStateStore` | Single-node / tests (`InMemoryStore`) |
| `InMemorySagaJournal` | RAM | Unit tests only |

Product path: **Meta-Raft** — no external DB.

## Quick start (current API)

### 1. Obtain journal from cluster

```rust
use crafty::{CraftyCluster, CompositeSagaJournal, MetaRaftSagaJournal};
use crafty_client::{run_saga, resume_saga, SagaPlan, SagaStep, RunSagaOpts};

let journal = cluster.saga_journal();  // CompositeSagaJournal in multi-Raft mode
```

Built from [`CraftyCluster::saga_journal`](../../crates/crafty/src/cluster.rs) — wires Meta-Raft + optional store.

### 2. Define steps

```rust
let plan = SagaPlan::new("onboard-user-42")
    .step(SagaStep::new("create_account", || async {
        client.propose(CreateAccount { user_id: 42 }).await?;
        Ok(())
    }))
    .step(SagaStep::new("send_welcome", || async {
        queue.enqueue(welcome_email_bytes).await?;
        Ok(())
    }))
    .compensate("create_account", || async {
        client.propose(DeleteAccount { user_id: 42 }).await?;
        Ok(())
    });
```

Steps must be **idempotent** where at-least-once retry applies.

### 3. Run and resume

```rust
match run_saga(&plan, journal.as_ref(), RunSagaOpts::default()).await {
    Ok(outcome) => { /* done */ }
    Err(e) if e.is_resumable() => {
        resume_saga(&plan, journal.as_ref(), ResumeSagaOpts::default()).await?;
    }
    Err(e) => return Err(e.into()),
}
```

After leader restart, journal record persists in Meta-Raft log.

### 4. Compose with job queue

Long-running side effects → enqueue in a step; worker acks independently:

```rust
// Step 2: enqueue only (fast saga progress)
queue.enqueue_opts(payload, EnqueueOptions::dedup_key(saga_step_key)).await?;

// Worker: perform effect, idempotent on dedup key
```

See [background-jobs](background-jobs.md).

### 5. Compose with actors

Synchronous side effect in saga step:

```rust
.cluster_ref("provisioner").ask(Provision { user_id }).await?;
```

Prefer queue when step can take minutes or must survive worker crash without blocking saga coordinator.

## Cross-shard workflows

| API | Guarantee |
|-----|-----------|
| `run_saga` (single group) | Sequential steps on one client |
| `run_keyed_saga` | Keyed routing per step |
| `run_saga` + `CompositeSagaJournal` | Journal on Meta-Raft |
| `propose_cross_shard_2pc` | Optional atomic commit (≤3 groups) |

Global serializable isolation across shards is **not** a goal ([multi-raft](../decisions/multi-raft.md)).

## Observability

- Metrics: `crafty_saga_*` (see dashboard / telemetry)
- Backlog **B-07:** workflow status in admin UI

## Operations

| Concern | Action |
|---------|--------|
| Backup | Include `group-meta.redb` ([backup-restore](../ops/backup-restore.md)) |
| Stuck saga | `resume_saga` with same plan id |
| Compensator failure | Recorded in journal — manual ops / alert |

## Examples & tests

| Asset | Purpose |
|-------|---------|
| [`examples/workflows/`](../../examples/workflows/) | Meta-Raft saga + compensators + resume |
| `crafty/tests/saga.rs` | Facade integration |
| `crafty/tests/two_phase.rs` | Cross-shard 2PC (advanced) |
| `crafty-client` | `run_saga`, `SagaPlan` API |

## Future polish

```rust
app.workflow("onboard_user", |w| async move {
    w.step("create_account", || sm.create(user)).await?;
    w.step("send_welcome", || w.enqueue("emails", welcome)).await?;
    w.compensate("create_account", || sm.delete(user.id));
}).await?;
```

## Related

- [multi-raft](../decisions/multi-raft.md#cross-shard-transactions)
- [job-queue](../decisions/job-queue.md)
- [background-jobs](background-jobs.md)
- [stateful-workers](stateful-workers.md)
- [backlog.md](../backlog.md) — B-05, B-07
