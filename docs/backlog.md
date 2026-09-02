# Backlog

Product and implementation backlog for trembita. Shipped capabilities stay in [status.md](status.md); design rationale in [decisions/](decisions/).

**Product vision:** [decisions/product-scenarios.md](decisions/product-scenarios.md) — jobs, event topics, workers, sessions, workflows; **no mandatory Redis**.

**Scenario guides:** [scenarios/](scenarios/README.md)

---

## Open work

Epics **B-01 … B-17** are **shipped** (see [Shipped epics](#shipped-epics-archive) below). Remaining items are optional integrations and maintenance — not blockers for product scenarios.

| Id | Item | Status | Notes |
|----|------|--------|-------|
| O-01 | `trembita-store-redis` maintenance | ongoing | Keep as optional adapter |
| O-02 | PostgreSQL `ActorStateStore` | deferred | Only if external integration demand |
| O-03 | Optional OTLP metrics adapter (`trembita-metrics-otlp`) | deferred | Builds on shipped [`MetricsSink`](decisions/observability.md#metrics-export-port) port |



For new feature epics, use the next **B-NN** id and link the scenario + ADR.

---

## Summary (shipped epics)


| Priority     | Count | Items                           |
| ------------ | ----- | ------------------------------- |
| **P0**       | 2     | B-01 ✅, B-02 ✅                  |
| **P1**       | 6     | B-03 ✅ … B-06 ✅, B-14 ✅, B-16 ✅ |
| **P2**       | 4     | B-07 ✅ … B-09 ✅, B-13 ✅        |
| **P3**       | 3     | B-10 ✅ … B-12 ✅                 |
| **Optional** | 4 open | O-01 … O-04 (O-05 ✅ shipped) |
| **Subtasks** | 58 | B-01a … B-16i — all shipped (see archive) |


---

## Shipped epics (archive)

Historical record of product epics B-01 … B-16. Current capabilities: [status.md](status.md).

## Priority legend


| Priority | Meaning                                                     |
| -------- | ----------------------------------------------------------- |
| **P0**   | Blocks “product team out of the box” for a shipped scenario |
| **P1**   | Strong DX improvement; should land in 0.2.x–0.3.x           |
| **P2**   | Polish, docs, observability                                 |
| **P3**   | 1.0 stabilization / aspirational                            |



| Status | Meaning                                                     |
| ------ | ----------------------------------------------------------- |
| 🔲     | Not started                                                 |
| 🚧     | In progress                                                 |
| ✅      | Shipped (move note to [status.md](status.md) when released) |


---



## Epic map (product scenarios)

```mermaid
flowchart TB
    subgraph P0["P0 — foundation"]
        B01[B-01 RedbActorStateStore]
        B02[B-02 TrembitaApp]
    end

    subgraph Jobs["Background jobs"]
        B03[B-03 HTTP jobs API]
        B03a[B-03a enqueue helper]
        B03b[B-03b consumer macro ✅]
        B07a[B-07a queue dashboard]
        B13[B-13 queue idempotency DX ✅]
        B14[B-14 product polish & composition]
        B14a[B-14a gateway auth]
        B14b[B-14b trembita init v2]
        B16[B-16 workload governor]
    end

    subgraph Workers["Stateful workers"]
        B01
        B01a[B-01a redb schema]
        B01b[B-01b replicate RPC]
        B01c[B-01c default store wiring]
        B10a[B-10a worker failover soak]
        B14k[B-14k queue → actor bridge]
    end

    subgraph Sessions["Real-time / session"]
        B04[B-04 websocket example]
        B04a[B-04a WS gateway example]
        B04b[B-04b session helpers]
        B10b[B-10b session migration soak]
        B14a
    end

    subgraph Workflows["Workflows"]
        B05[B-05 fluent builder]
        B05a[B-05a step/compensate DSL]
        B05b[B-05b resume CLI]
        B07b[B-07b saga dashboard]
        B10c[B-10c saga resume soak]
        B14i[B-14i saga step idempotency]
    end

    B02 --> Jobs
    B02 --> Workers
    B02 --> Sessions
    B02 --> Workflows
```



---



## P0 — Core product gaps (Redis-free)



### B-01 ✅ `RedbActorStateStore` + voter replication

**Shipped:** 2026-08-28 — `RedbActorStateStore`, `StoreService`, `ClusterActorStateStore`, wire routes, builder auto-wire.


| Subtask       | Status |
| ------------- | ------ |
| B-01a … B-01f | ✅      |


**Acceptance:** `trembita/tests/store.rs`, `trembita-actor/src/redb_store.rs` tests.

---



### B-02 ✅ `TrembitaApp` product facade

**Shipped:** 2026-08-28 — `TrembitaApp`, `TrembitaAppBuilder`, `env_config`, [getting-started.md](getting-started.md).


| Subtask       | Status                    |
| ------------- | ------------------------- |
| B-02a … B-02h | ✅ (B-02h getting-started) |


**Acceptance:** `trembita/tests/app.rs`, docs/getting-started.md.

---



## P1 — Scenario polish



### B-03 ✅ Background jobs — HTTP + DX

**Shipped:** 2026-08-28 — `trembita-http`, `http-jobs` feature, `trembita/tests/http_jobs.rs`. **2026-08-29** — `#[trembita::consumer]`, `TrembitaApp::spawn_consumer`, DLQ requeue HTTP + `TrembitaApp` parity.


| Subtask | Description                                                                 | Status                        |
| ------- | --------------------------------------------------------------------------- | ----------------------------- |
| B-03a   | Axum route `POST /jobs/{stream}` → `202` + `{ "job_id": … }`; raw or JSON envelope body | ✅ `trembita-http`               |
| B-03b   | `#[trembita::consumer("stream")]` + `TrembitaApp::spawn_consumer` (no manual `run_queue_consumer` + `tokio::spawn`) | ✅ `trembita-macros`, `trembita/tests/consumer.rs` |
| B-03c   | Optional `GET /jobs/{stream}/{id}` if queue metadata extended               | ✅ `JobQueue::job_status` + HTTP GET |
| B-03d   | Optional `trembita/http-jobs` feature or `trembita-http` crate (decide in impl) | ✅ `trembita-http` + feature     |
| B-03e   | Integration test: HTTP enqueue → worker ack                                 | ✅ `trembita/tests/http_jobs.rs` |
| B-03f   | `TrembitaApp` parity: batch enqueue/ack, `requeue_dead_letter`, `recurring_job` builder; HTTP `POST /jobs/{stream}/{id}/requeue` | ✅ `TrembitaApp`, `trembita-http` |


---



### B-04 ✅ Real-time — WebSocket gateway

**Shipped:** 2026-08-28 — [`examples/realtime/`](../examples/realtime/).


| Subtask | Description                                                     | Status |
| ------- | --------------------------------------------------------------- | ------ |
| B-04a   | [`examples/realtime/`](../examples/realtime/) — axum WS + `ChatWorker` | ✅      |
| B-04b   | Homogeneous cluster showcases (same binary every node; no role env) | ✅ (superseded role docs → B-16) |
| B-04c   | Auth stub + `ActorSession` open on connect                      | ✅ `GATEWAY_TOKEN` |
| B-04d   | Reconnect: handle `NoTarget`, session TTL expiry                | ✅ auto reopen in example |
| B-04e   | Optional: checkpoint last N messages to SM (comment in example) | ✅ comment on `ChatWorker` |
| B-04f   | Product HTTP `POST /actors/{group}/ask` + `/cast` on gateway (`ActorsApi`) | ✅ `trembita-http`, `TrembitaApp::actors_api` |


---



### B-05 ✅ Workflows — fluent builder

**Shipped:** 2026-08-28 — `WorkflowBuilder`, `TrembitaApp::run_workflow` / `resume_workflow`.


| Subtask | Description                                                                 | Status           |
| ------- | --------------------------------------------------------------------------- | ---------------- |
| B-05a   | `WorkflowBuilder` — named `.step(id, fn)`, `.compensate(id, fn)`            | ✅                |
| B-05b   | Builds `SagaPlan`; runs via `run_saga` / `CompositeSagaJournal`             | ✅                |
| B-05c   | `TrembitaApp::workflow(name, builder_fn)` registration                        | ✅ `run_workflow` |
| B-05d   | Example: [`examples/workflows/`](../examples/workflows/) — saga + enqueue + propose steps | ✅                |
| B-05e   | `trembita workflow resume <id>` CLI stub (optional, via `trembita-node` or ops) | ✅ `scripts/trembita-workflow.sh` |


---



### B-06 ✅ `trembita init` project template

**Shipped:** 2026-08-28 — `scripts/trembita-init.sh`, `templates/trembita-app/`.


| Subtask | Description                                                         | Status     |
| ------- | ------------------------------------------------------------------- | ---------- |
| B-06a   | `scripts/trembita-init.sh` or cargo-template: main + one worker       | ✅          |
| B-06b   | Generated: job stream stub + optional saga stub                     | ✅ template |
| B-06c   | `docker-compose.yml` 3-node local (dev-certs)                       | ✅          |
| B-06d   | Zero `redis://`; README points to [scenarios/](scenarios/README.md) | ✅          |


---



## P2 — Docs & observability



### B-07 ✅ Dashboard — queue + workflows

**Shipped:** 2026-08-28 — `/introspect/queues`, `/introspect/sagas`, dashboard panels, Prometheus gauges.


| Subtask | Description                                                                | Status |
| ------- | -------------------------------------------------------------------------- | ------ |
| B-07a   | Admin HTML: per-stream queue depth, active leases                          | ✅      |
| B-07b   | Admin HTML: saga records (running / done / failed) from journal or metrics | ✅      |
| B-07c   | Wire existing `trembita_saga_*` / queue metrics to dashboard views           | ✅      |


---



### B-08 ✅ De-emphasize Redis in docs & examples

**Scenario:** all


| Subtask | Description                                                                                                      | Status |
| ------- | ---------------------------------------------------------------------------------------------------------------- | ------ |
| B-08a   | Product scenario ADR + four scenario guides                                                                      | ✅      |
| B-08b   | [actor-state-redis](decisions/actor-state-redis.md) banner → [actor-state-store](decisions/actor-state-store.md) | ✅      |
| B-08c   | [status.md](status.md), [README.md](README.md), [AGENTS.md](../AGENTS.md) links                                  | ✅      |
| B-08d   | Product showcases document redb prod path (`examples/stateful-workers`, …) | ✅      |
| B-08e   | `docs/getting-started.md` — full tutorial, no Redis                                                              | ✅      |
| B-08f   | README root: link scenarios + positioning paragraph                                                              | ✅      |


---



### B-09 ✅ Production runbook bundle

**Shipped:** 2026-08-28 — [ops/production-runbook.md](ops/production-runbook.md).


| Subtask | Description                                                                                                           | Status |
| ------- | --------------------------------------------------------------------------------------------------------------------- | ------ |
| B-09a   | `docs/ops/production-runbook.md` — scale VPS, seeds, firewall UDP 7443                                                | ✅      |
| B-09b   | Merge pointers: [backup-restore](ops/backup-restore.md), [rolling-upgrade](ops/rolling-upgrade.md), [certs](certs.md) | ✅      |
| B-09c   | Multi-Raft rebalance pointer (when to add groups)                                                                     | ✅      |
| B-09d   | Link from [scenarios/README](scenarios/README.md)                                                                     | ✅      |


---



### B-13 ✅ Job queue — delivery semantics & idempotency DX

**Scenario:** [background-jobs](scenarios/background-jobs.md)  
**ADR:** [job-queue](decisions/job-queue.md) — at-least-once is intentional; **exactly-once as a queue toggle is out of scope**.

Document the contract, show effectively-once patterns, and improve consumer ergonomics without promising false delivery guarantees.

**Non-goals:** `QueueOpts { exactly_once: true }`, auto-dedup by payload hash, unbounded processed-key table inside queue redb.


| Subtask | Description | Priority slice | Status |
| ------- | ----------- | -------------- | ------ |
| B-13a   | [background-jobs.md](scenarios/background-jobs.md): **Delivery semantics** — at-least-once vs effectively-once; table of what trembita guarantees vs what the app must do; three idempotency layers (`dedup_key` enqueue, `ActorStateStore`/SM processing, saga step keys) | MR-1 | ✅ |
| B-13b   | **Effectively-once recipe** — short guide (section or `docs/scenarios/` doc): enqueue `dedup_key` + worker CAS in store + ack after durable mark; link [stateful-workers example](../examples/stateful-workers/) and [`idempotent_worker`](../crates/trembita-store-redis/examples/idempotent_worker.rs) | MR-1 | ✅ |
| B-13c   | [job-queue.md](decisions/job-queue.md): one-line consequence — exactly-once delivery mode not planned; point to B-13 recipe | MR-1 | ✅ |
| B-13d   | `QueueOpts` / `JobOpts::default_max_attempts(u32)` — stream default when `EnqueueOptions::max_attempts` is unset (`0` = inherit stream default; explicit `0` on enqueue still means unlimited) | MR-1 | ✅ |
| B-13e   | `JobContext` in `#[consumer]` — expose `job_id`, `attempts`, optional `dedup_key` from [`LeasedJob`](../crates/trembita-jobs/src/queue.rs); thread through `run_queue_consumer` → `JobConsumer` | MR-2 | ✅ |
| B-13f   | `ConsumerOpts::idempotency(...)` helper — key fn + `ActorStateStore` prefix; CAS `processing` → handler → `done` → ack (effectively-once, not a magic flag) | MR-2 | ✅ |
| B-13g   | [background-jobs example](../examples/background-jobs/): idempotent handler demo — HTTP `?dedup=` retry + simulated redelivery; `trigger.sh` like stateful-workers | MR-1 | ✅ |
| B-13h   | Cross-links: background-jobs ↔ [stateful-workers](scenarios/stateful-workers.md) ↔ [workflows](scenarios/workflows.md) (step `dedup_key`) | MR-1 | ✅ |
| B-13i   | Prometheus: `trembita_queue_redeliveries_total{stream}` + attempts histogram (optional) | later | ✅ |
| B-13j   | Dashboard / `/introspect/queues`: surface jobs with `attempts > 1` as idempotency smell | later | ✅ |
| B-13k   | **Queue token semantics** — `lease_id` monotonicity + stale-token invalidation; `dedup_key` lifecycle (ack release, dead-letter hold); external-backlog note; rustdoc on `LeaseId`; regression tests in [`queue.rs`](../crates/trembita/tests/queue.rs) | MR-1 | ✅ |


**Suggested MR slices**

| MR | Subtasks | Effort |
| -- | -------- | ------ |
| **MR-1** (minimal useful) ✅ | B-13a, B-13b, B-13c, B-13d, B-13g, B-13h | ~1–2 days |
| **MR-2** (consumer DX) ✅ | B-13e, B-13f + integration test (redelivery → side effect once) | ~2–3 days |
| **MR-3** (observability) ✅ | B-13i, B-13j | backlog |


**Acceptance:** MR-1 docs + example runnable; MR-2 `trembita/tests/` redelivery idempotency regression; no public API promising exactly-once.


---



### B-14 ✅ Product polish & cross-scenario composition

**Scenarios:** [background-jobs](scenarios/background-jobs.md), [realtime-sessions](scenarios/realtime-sessions.md), [workflows](scenarios/workflows.md), [stateful-workers](scenarios/stateful-workers.md)  
**Follows:** B-13 ✅ (delivery semantics + consumer idempotency DX)

Gateway production readiness, queue lifecycle polish, and glue between product scenarios — without new delivery guarantees or mandatory external infra.


| Subtask | Wave | Description | Status |
| ------- | ---- | ----------- | ------ |
| B-14a   | 1 | **Gateway auth hook** — middleware slot on [`GatewayOpts`](../crates/trembita/src/gateway/mod.rs) (JWT / API key / custom Axum layer); document pattern; extend [`examples/realtime/`](../examples/realtime/) beyond `GATEWAY_TOKEN` query stub ([product-scenarios](decisions/product-scenarios.md)) | ✅ |
| B-14b   | 1 | **`trembita init` v2** — [`templates/trembita-app/`](../templates/trembita-app/): `JobOpts` + `#[consumer]` + `IdempotencyOpts::by_dedup_key` + `default_max_attempts(5)`; remove bare `TODO` stub ([B-06](backlog.md#b-06--trembita-init-project-template)) | ✅ |
| B-14c   | 1 | **E2E HTTP jobs via gateway (docker)** — `POST /jobs/{stream}/batch` through product gateway in `e2e/` (QUIC queue E2E exists; HTTP gateway path does not) | ✅ |
| B-14d   | 2 | **`IdempotencyOpts` TTL** — optional `retain_for` / `with_ttl` on done markers in `ActorStateStore`; default forever for payment-style keys; doc high-volume cleanup | ✅ |
| B-14e   | 2 | **Graceful consumer drain** — on shutdown: stop leasing, wait for in-flight handlers (timeout), then ack/nack; `RunOpts` or `ConsumerOpts` hook to avoid noisy redelivery metrics | ✅ |
| B-14f   | 2 | **Typed job payloads** — optional serde envelope for `#[consumer]` (e.g. `#[consumer_json("emails", WelcomeEmail)]` or generic `JobConsumer` payload decode); keep raw `&[u8]` as default | ✅ |
| B-14g   | 2 | **HTTP queue parity** — stream `default_max_attempts` as query/body on enqueue; expose `attempts`, `dedup_key`, redelivery hints on `GET /jobs/{stream}/{id}` where metadata exists | ✅ |
| B-14h   | 2 | **E2E idempotency + failover** — extend `./e2e/queue.sh` (or sibling): redelivery under leader kill with `IdempotencyOpts` → one side effect | ✅ |
| B-14i   | 3 | **Saga step idempotency helper** — `WorkflowBuilder` sugar wrapping enqueue + `dedup_key(step_key)` (parallel to B-13f for consumers); aligns with [workflows § Future polish](scenarios/workflows.md#future-polish) | ✅ |
| B-14j   | 3 | **State placement cheat sheet** — one doc (`docs/scenarios/state-placement.md` or scenarios README section): SM vs `JobQueue` vs `ActorStateStore` vs saga journal — when to use which | ✅ |
| B-14k   | 3 | **Queue → actor bridge** — example + doc pattern: job consumer delegates side effects via `TrembitaApp::cast` / `ask` to stateful worker group (orchestration without duplicating handler logic) | ✅ |


**Suggested MR slices**

| MR | Subtasks | Wave | Effort |
| -- | -------- | ---- | ------ |
| **MR-1** (gateway + onboarding) | B-14a, B-14b, B-14c | 1 | ~3–4 days |
| **MR-2** (queue lifecycle) | B-14d, B-14e, B-14g, B-14h | 2 | ~3–4 days |
| **MR-3** (composition) | B-14i, B-14j, B-14k | 3 | ~2–3 days |
| **MR-4** (typed jobs, optional) | B-14f | 2 | ~2 days — can ship independently |


**Acceptance:** MR-1 gateway auth documented + init template runnable; MR-2 no unbounded idempotency key growth for TTL users + clean shutdown story; MR-3 one cross-scenario doc + one bridge example.


---


### B-15 ✅ External backlog port (Postgres / existing work table)

**Scenario:** [background-jobs](scenarios/background-jobs.md)  
**ADR:** [external-backlog](decisions/external-backlog.md)

Teams with backlog in Postgres/MySQL get leader-fed queue windows, dedup on re-enqueue, settlement, and honest autoscale depth — without reimplementing the feeder loop.


| Subtask | Wave | Description | Status |
| ------- | ---- | ----------- | ------ |
| B-15a   | 1 | **`ExternalBacklog` trait** — `depth`, `claim`, `settle`; `InMemoryExternalBacklog` for tests | ✅ |
| B-15b   | 1 | **`run_backlog_feeder`** — leader-only top-up to `pending_target × consumers` | ✅ |
| B-15c   | 1 | **Settlement wiring** — ack/nack/reclaim → durable outbox → `settle` drainer | ✅ |
| B-15d   | 1 | **Autoscale depth** — `effective_queue_depth` feeds worker + membership policies | ✅ |
| B-15e   | 1 | **`JobOpts::backlog`** + `TrembitaClusterBuilder::job_queue_external_backlog` | ✅ |
| B-15f   | 2 | **`trembita-backlog-postgres`** — `PgBacklog` with `SKIP LOCKED` | ✅ |
| B-15g   | 2 | **Integration test** — `trembita/tests/external_backlog.rs` | ✅ |


**Acceptance:** In-memory backlog → consumer → `Settlement::Done`; autoscale reads external `depth()` when registered.


---


### B-16 ✅ Workload governor (compute tokens)

**Scenario:** all four — homogeneous nodes, no static roles  
**ADR:** [workload-governor](decisions/workload-governor.md)

Every VPS runs the same binary. **Compute tokens** arbitrate gateway ingress vs job/actor work on one node: API protected when hot; spare capacity goes to jobs when ingress is quiet (e.g. overnight) — **without** cluster rescale or `TREMBITA_ROLE`.


| Subtask | Wave | Description | Status |
| ------- | ---- | ----------- | ------ |
| B-16a   | 1 | **ADR + scenario notes** — compute token model, signal/action table, deprecate roles | ✅ |
| B-16b   | 1 | **`ComputeTokenPool`** — process-wide semaphore, RAII `ComputeGuard`, configurable `max_tokens` | ✅ |
| B-16c   | 1 | **Acquire hooks** — gateway product routes + `run_queue_consumer` handler wrap + optional actor ask | ✅ |
| B-16d   | 1 | **`WorkloadGovernor`** — per-node loop: `ConnectionTracker` + queue depth → tune consumers / token ceiling | ✅ |
| B-16e   | 1 | **`WorkloadOpts`** + `TrembitaAppBuilder::workload` — presets (`Balanced`, `ApiFirst`, `JobsOpportunistic`) | ✅ |
| B-16f   | 2 | **Deprecate `TREMBITA_ROLE`** — `#[deprecated]` on `NodeRole` / env helpers; update docs & examples | ✅ |
| B-16g   | 2 | **Remove role env** — delete `TREMBITA_ROLE`, `TREMBITA_GATEWAY_ONLY`, `TREMBITA_NO_CONSUMER` (semver major) | ✅ |
| B-16h   | 2 | **Tests** — governor lowers consumer batch when connections high; boosts when idle + depth | ✅ |
| B-16i   | 3 | **Metrics** — `trembita_compute_tokens_in_use`, throttle/tune event counters | ✅ |


**Acceptance:** Single-node soak: simulated idle gateway → consumer throughput rises; simulated connection load → API p99 stable and job poll throttles. No `TREMBITA_ROLE` in docs or showcases.


---

### B-17 ✅ External compute load

**Scenario:** background jobs — subprocess-heavy handlers colocated with gateway  
**ADR:** [external-load](decisions/external-load.md)

Weighted token acquire and an optional load port so the workload governor accounts for Chromium/ffmpeg/shell-out work the cooperative pool cannot observe.


| Subtask | Wave | Description | Status |
| ------- | ---- | ----------- | ------ |
| B-17a   | 1 | **ADR** — `compute_cost`, `ExternalLoad`, governor mapping | ✅ |
| B-17b   | 1 | **`ComputeTokenPool::acquire_weighted`** + `JobOpts::compute_cost` | ✅ |
| B-17c   | 1 | **`ExternalLoad` port** + `WorkloadOpts::external_load` | ✅ |
| B-17d   | 1 | **Governor** — map external units to protective tune | ✅ |
| B-17e   | 2 | **Tests** — weighted acquire; external load triggers protection | ✅ |
| B-17f   | 2 | **Metrics** — `trembita_compute_external_load_units` | ✅ |


**Acceptance:** `compute_cost(4)` on an 8-token pool limits concurrent handlers to 2; `ManualExternalLoad` at ceiling triggers protective consumer tune with zero gateway connections.


---



## P3 — 1.0 stabilization



### B-10 ✅ Soak / chaos per scenario

**Shipped:** 2026-08-28 — `benchmarks/src/bin/soak_{actor_store,saga,session}.rs`, scheduled CI.


| Subtask | Description                                                       | Status                                |
| ------- | ----------------------------------------------------------------- | ------------------------------------- |
| B-10a   | Soak: stateful worker + `RedbActorStateStore` failover loop       | ✅ `soak_actor_store`                  |
| B-10b   | Soak: WebSocket session + worker kill → reconnect path            | ✅ `soak_session` (in-process session) |
| B-10c   | Soak: saga mid-flight + leader kill → `resume_saga`               | ✅ `soak_saga`                         |
| B-10d   | Soak: job queue (extend existing `soak_queue`) — document CI lane | ✅ `.gitlab-ci.yml` `bench` job        |


---



### B-11 ✅ API freeze + semver

**Shipped:** 2026-08-28 — ADRs + CHANGELOG policy.


| Subtask | Description                                       | Status                                                                        |
| ------- | ------------------------------------------------- | ----------------------------------------------------------------------------- |
| B-11a   | Public API audit (`TrembitaApp`, facade re-exports) | ✅ [public-api-1.0.md](decisions/public-api-1.0.md)                            |
| B-11b   | `missing_docs = deny` on published crates         | ✅ shipped 2026-08-29 |
| B-11c   | CHANGELOG policy for 1.0 breaking changes         | ✅ [CHANGELOG.md](../CHANGELOG.md)                                             |


---



### B-12 ✅ Jepsen / Antithesis (aspirational)

**Shipped:** 2026-08-28 — evaluation + go/no-go ([jepsen-1.0.md](decisions/jepsen-1.0.md)).


| Subtask | Description                                                                     | Status |
| ------- | ------------------------------------------------------------------------------- | ------ |
| B-12a   | Evaluate Jepsen harness scope (Raft + queue + saga)                             | ✅      |
| B-12b   | Document go/no-go criteria in [testing-strategy](decisions/testing-strategy.md) | ✅      |


---



## Optional integrations (explicitly not P0)

Moved to [Open work](#open-work). **O-05** (self-update coordinator) shipped — see [upgrade-coordinator](decisions/upgrade-coordinator.md).

---

## Per-scenario checklist (what “done” looks like)



### Background jobs ✅ runtime / ✅ product layer


| Done when | Item                                                        |
| --------- | ----------------------------------------------------------- |
| ✅         | `RedbJobQueue`, `ClusterJobQueue`, autoscale, E2E           |
| ✅         | B-03 HTTP `202`, B-02c jobs on `TrembitaApp`, B-07a dashboard |
| ✅         | B-13 delivery semantics docs + effectively-once recipe      |
| ✅         | B-13k queue token semantics (`lease_id`, `dedup_key` lifecycle) |
| ✅         | B-13g idempotent consumer example; B-13e/f consumer DX      |
| ✅         | B-14d/e/g/h queue lifecycle + HTTP parity + failover E2E    |
| ✅         | B-14k queue → actor orchestration example                   |




### Stateful workers ✅ store + migration


| Done when | Item                                                  |
| --------- | ----------------------------------------------------- |
| ✅         | Migration, supervisor, `RedbActorStateStore`, SM path |
| ✅         | B-02d `worker_groups`, `workers`, `cast`, `ask` on `TrembitaApp` |
| ✅         | B-10a soak                                            |
| ✅         | B-14k consumer delegates to actor group               |




### Real-time / session ✅ routing / ✅ gateway


| Done when | Item                                                          |
| --------- | ------------------------------------------------------------- |
| ✅         | `ActorSession`, consistent-hash, cross-node ask, B-04 example |
| ✅         | B-02g `app.session` + `cast_session`                          |
| ✅         | B-10b soak                                                    |
| ✅         | B-14a gateway auth beyond `GATEWAY_TOKEN` stub                |




### Workflows ✅ journal / ✅ builder


| Done when | Item                                                             |
| --------- | ---------------------------------------------------------------- |
| ✅         | `run_saga`, Meta-Raft journal, cross-shard saga, B-05 fluent API |
| ✅         | B-05d example, B-07b dashboard, B-10c soak                       |
| ✅         | B-14i saga step idempotency helper                             |
| ✅         | B-14j state placement cheat sheet (cross-scenario)               |


### Event topics ✅ runtime


| Done when | Item                                                        |
| --------- | ----------------------------------------------------------- |
| ✅         | `EventTopic`, `RedbEventTopic`, voter replication, compaction |
| ✅         | `TopicOpts`, `.topics()`, `TrembitaApp::publish`, subscription consumers |
| ✅         | ADR + [event-topics.md](scenarios/event-topics.md) scenario guide |
| ✅         | `topic_failover` integration test                           |


---



## How to update this file

1. Pick **B-NN** or **B-NNx** subtask; set 🚧 while working.
2. On release, note version in item or update [status.md](status.md).
3. New items: next id; link scenario + ADR.
4. Reference GitLab issues as `#<number>` in commits/MRs.



## Related

- [status.md](status.md) · [scenarios/README.md](scenarios/README.md) · [decisions/product-scenarios.md](decisions/product-scenarios.md)

