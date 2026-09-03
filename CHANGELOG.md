# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the workspace
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) with all
`trembita-*` crates sharing a synchronized version ([library-and-publishing](docs/decisions/library-and-publishing.md)).

Under [Semantic Versioning](https://semver.org/spec/v2.0.0.html), `0.x` releases may include breaking changes on minor bumps; each is noted here.

**Crates.io:** [`0.2.0`](https://crates.io/crates/trembita) (2026-09-03) is the first published trembita release. Git tags `v0.1.0`–`v0.6.1` from the prior **crafty**-era development were removed on 2026-09-03; they were never on crates.io. Historical notes for those internal milestones live under [Archive](#archive--pre-cratesio-development-crafty-era) below.

## [Unreleased]

### Added

- **Leader task primitive** — [`LeaderSession`](crates/trembita-runtime/src/leader_task.rs),
  [`run_leader_loop`](crates/trembita-runtime/src/leader_task.rs), and
  [`TrembitaClusterBuilder::on_leader`](crates/trembita/src/builder.rs) for periodic
  leader-only work with `first_in_term` ([`leader-task`](docs/decisions/leader-task.md));
  internal feeder, drainer, autoscaler, supervisor, schedule, GC, and topic loops migrated.

- **`EventOutboxSource` port** — leader drainer from application transactional outbox into
  [`EventTopic`](crates/trembita-events/src/topic.rs)
  ([`event-outbox`](docs/decisions/event-outbox.md)); [`TopicOpts::outbox`](crates/trembita/src/topic_opts.rs),
  [`.event_outbox_source()`](crates/trembita/src/app.rs), cursor checkpoint at
  `{data_dir}/event-outbox-cursors.redb`.

## [0.2.1] — 2026-09-03

### Changed (breaking)

- **`Settlement::Done` / `BacklogSettleOutcome::Done`** — now carry `attempts` (queue attempt counter at ack). [`PgBacklog`](crates/trembita-backlog-postgres/src/lib.rs) applies `Done` only when the row is still `claimed` and `attempts` matches, ignoring stale settle-outbox entries after key reuse.

### Fixed

- **`EnqueueOptions::dedup_key` rustdoc** — documents that the key is held while a job exists and released after ack ([CF-010](docs/backlog.md#cf-010--dedup_key-lifecycle-docs)).

## [0.2.0] — 2026-09-03

**First trembita release on [crates.io](https://crates.io/crates/trembita).** Synchronized workspace `0.2.0` (16 published crates). Capabilities below include work accumulated during the pre-crates.io internal line (see archive).

### Changed (breaking)

- **`trembita-actor` removed** — split into [`trembita-runtime`](crates/trembita-runtime/),
  [`trembita-jobs`](crates/trembita-jobs/), [`trembita-events`](crates/trembita-events/),
  and [`trembita-actor-store`](crates/trembita-actor-store/).
- **Facade imports** — `trembita::actor::*` → `trembita::runtime::*`; job/topic/store types on
  `trembita::jobs`, `trembita::events`, `trembita::actor_store`.
- **Dev binaries** — `trembita-node`, `trembita-ops`, showcase clients merged into unpublished
  [`trembita-tools`](crates/trembita-tools/); examples use `trembita_tools::showcase_*`.

### Added

- **Learner join (elastic scale-out)** — `/cluster/join` defaults to [`JoinRole::Learner`](crates/trembita-proto/src/join.rs); voters only with `allow_voter_join`. Full peer for workers/ingress; queue replication fan-out stays O(voters). See [cluster-elasticity § voters vs learners](docs/decisions/cluster-elasticity.md#voters-vs-learners-elastic-scale-out).
- **Automatic voter replacement** — leader promotes the lowest-id caught-up learner when a voter is permanently unreachable (`voter_replacement`, default on). Pure planner in `trembita-core::membership_repair`.
- **External compute load** — [`JobOpts::compute_cost`](crates/trembita/src/job_opts.rs) reserves
  weighted units from [`ComputeTokenPool`](crates/trembita-runtime/src/compute_token.rs) for
  subprocess-heavy handlers; optional [`ExternalLoad`](crates/trembita-runtime/src/external_load.rs)
  port on [`WorkloadOpts`](crates/trembita-jobs/src/workload.rs) feeds the governor when child
  processes compete with the gateway ([`external-load`](docs/decisions/external-load.md)).

### Changed

- **`trembita-node`** — not published; build from [`trembita-tools`](crates/trembita-tools/) or repo.
- **`BacklogFeedOpts::consumer_instances`** — defaults to `ConsumerCount::Live`
  (`reachable_nodes × per_node` each poll); use `ConsumerCount::Fixed(n)` to opt out of
  elastic window sizing.

## Archive — pre-crates.io development (crafty era)

Internal git milestones only (tags removed 2026-09-03). **Not** the crates.io `0.2.0` release above.

### 0.1.0 (internal patch, 2026-09-02)

**Rustdoc fixes for docs.rs 0.6.0 builds.**

### Fixed

- Intra-doc links in `trembita`, `trembita-actor`, and `trembita-backlog-postgres` that failed
  `cargo doc` / docs.rs for 0.6.0 (`crate::…` paths, redundant cross-crate links).

### Changed

- Release and CI gates unified (`ci-fast-lane.sh`, `gate.sh` tiers, lefthook per-step output);
  `trembita-node` explicitly `publish = false`.

### 0.6.0 (internal, 2026-09-02)

**Event topics, external backlog, workload governor, and dynamic schedules.**

### Added

- **Durable event topics** — [`EventTopic`](crates/trembita-events/src/topic.rs) pub/sub with named
  subscriptions, independent cursors, compaction by `min(cursor)`, retention thresholds, and
  voter replication ([`event-topics`](docs/decisions/event-topics.md)); [`TopicOpts`](crates/trembita/src/topic_opts.rs),
  [`.topics()`](crates/trembita/src/app.rs), [`TrembitaApp::publish`](crates/trembita/src/app.rs),
  `#[consumer(..., subscription = "...")]` for subscribers. Not a replacement for transactional
  outbox — see ADR.
- **`ScheduleSource` port** — dynamic recurring-job schedules polled on the queue
  leader ([`schedule-source`](docs/decisions/schedule-source.md)); [`.schedule_source()`](crates/trembita/src/app.rs)
  on `TrembitaAppBuilder`; [`.cron()`](crates/trembita/src/app.rs) reimplemented via
  [`StaticScheduleSource`](crates/trembita-jobs/src/schedule_source.rs). Source errors and
  bootstrap empty polls never wipe persisted schedules; diff reconcile replicates
  `UpsertSchedule` / `RemoveSchedule`.
- **`ExternalBacklog` port** — leader feeder + settle outbox for in-flight queue windows over an
  external source of truth ([`external-backlog`](docs/decisions/external-backlog.md));
  [`JobOpts::backlog`](crates/trembita/src/job_opts.rs), honest autoscale depth via
  `effective_queue_depth`.
- **`trembita-backlog-postgres`** — optional published crate with [`PgBacklog`](crates/trembita-backlog-postgres/src/lib.rs)
  (`SKIP LOCKED` claim + idempotent settle).
- **Workload governor** — per-node [`ComputeTokenPool`](crates/trembita-runtime/src/compute_token.rs) +
  [`WorkloadGovernor`](crates/trembita-jobs/src/workload.rs) consumer tuning from gateway load
  ([`workload-governor`](docs/decisions/workload-governor.md)); [`WorkloadOpts`](crates/trembita/src/workload.rs),
  [`.workload()`](crates/trembita/src/app.rs). Removed static role env vars (`TREMBITA_ROLE`, etc.).
- **`HostRouter`** — virtual-host dispatch for product gateway HTTP ([`trembita-http`](crates/trembita-http/src/host_router.rs));
  strict default with loopback dev fallback.

### Removed

- **`TREMBITA_ROLE`**, **`TREMBITA_GATEWAY_ONLY`**, **`TREMBITA_NO_CONSUMER`** — use homogeneous nodes +
  `.workload()` and deployment choice (register consumers or not) instead.

### Changed

- **Documentation** — contributor guide, doc link checker, descriptive messaging layer
  names (dropped Tier labels); `actor-routing` ADR rename.

### 0.5.2 (internal, 2026-09-02)

**Product polish & cross-scenario composition (B-14).** Gateway auth for built-in
product APIs, HTTP queue metadata parity, typed JSON consumers, graceful consumer
drain, saga step dedup helpers, and runnable cross-scenario examples.

### Added

- **Gateway bearer auth (B-14a)** — [`GatewayBearerIdentity`](crates/trembita/src/gateway/identity.rs)
  (`GATEWAY_TOKEN` / `Authorization: Bearer …`); [`GatewayOpts::protect_product_apis`](crates/trembita/src/gateway/mod.rs)
  requires identity on built-in `/jobs/*`, `/actors/*`, `/workflows/*`. Optional `AuthFn`
  hook on `trembita-http` API state for custom checks.
- **`trembita init` v2 (B-14b)** — template ships `JobOpts`, `#[consumer]`,
  `IdempotencyOpts::by_dedup_key`, `default_max_attempts(5)`, and gateway auth defaults.
- **E2E gateway jobs (B-14c)** — `trembita/tests/gateway_jobs_http.rs`, `./e2e/gateway_jobs.sh`
  (in-process HTTP batch enqueue through product gateway).
- **`IdempotencyOpts::retain_for` (B-14d)** — alias for marker TTL on done keys
  (high-volume cleanup; default remains forever).
- **Graceful consumer drain (B-14e)** — `run_queue_consumer` finishes the in-flight
  lease batch on stop; [`ShutdownOpts::consumer_drain_timeout`](crates/trembita/src/app.rs)
  bounds shutdown wait.
- **`#[consumer_json]` (B-14f)** — deserializes JSON payloads before the handler;
  raw `&[u8]` remains the default for `#[consumer]`.
- **HTTP queue parity (B-14g)** — enqueue accepts per-job `max_attempts`; job status
  exposes `dedup`, `attempts`, and `is_redelivery`.
- **Idempotency + failover test (B-14h)** — `dedup_key_survives_leader_failover` in
  `trembita/tests/queue.rs`; `./e2e/queue_idempotency.sh` runs idempotency regression.
- **Saga step dedup helper (B-14i)** — [`WorkflowBuilder::step_dedup_key`](crates/trembita/src/workflow.rs),
  [`TrembitaApp::enqueue_workflow_step`](crates/trembita/src/app.rs).
- **State placement cheat sheet (B-14j)** — [docs/scenarios/state-placement.md](docs/scenarios/state-placement.md)
  (SM vs queue vs actor store vs saga journal).
- **Queue → actor bridge (B-14k)** — `examples/background-jobs/src/bridge.rs` +
  [`ConsumerOpts::on_app`](crates/trembita/src/consumer.rs) pattern; consumer delegates
  side effects via `TrembitaApp::cast`.
- **`ConsumerOpts` builders** — `.instance()`, `.batch()`, `.idle_sleep()` replace
  struct-literal fields for test and app code.

### Changed

- **`examples/realtime/`** — `ShowcaseGatewayIdentity` replaced with
  `GatewayBearerIdentity` + `protect_product_apis(true)`; docs and `trigger-http.sh`
  updated.
- **`QueueJobStatusReply::dedup_key`** — always serialized on the wire (fixes postcard
  decode 503 when the field was omitted).

### 0.5.1 (internal, 2026-09-01)

**Job queue delivery semantics & idempotency (B-13).** At-least-once is now
documented as a contract rather than a caveat, consumers can tell a redelivery
from a first attempt, and redelivery is visible in metrics and the dashboard.

### Added

- **Stream-level attempt ceilings (B-13d)** — [`JobOpts::default_max_attempts`](crates/trembita/src/job_opts.rs) /
  [`QueueOpts::default_max_attempts`](crates/trembita/src/queue_opts.rs) apply to enqueues that cannot
  pass per-job options (HTTP `POST /jobs/{stream}`, cron ticks). An explicit per-job
  `max_attempts` still wins; `0` in either position means unlimited retries.
- **Delivery-semantics docs (B-13a/B-13b)** — [background-jobs](docs/scenarios/background-jobs.md)
  gains *Delivery semantics*, the three idempotency layers, and an *Effectively-once recipe*
  (enqueue `dedup_key` → CAS marker → side effect → durable mark → ack).
- **Idempotency demo (B-13g)** — `examples/background-jobs/trigger-idempotent.sh` plus an
  effectively-once handler: two deliveries, one side effect.
- **`JobContext` for consumers (B-13e)** — `#[consumer]` handlers may take a second argument
  exposing `job_id`, `lease_id`, `stream`, `attempts`, `dedup_key`, and `is_redelivery()`.
  Single-argument handlers are unchanged.
- **`ConsumerOpts::idempotency` / `JobOpts::idempotency` (B-13f)** — the effectively-once recipe
  wired up over any `ActorStateStore`: check `done` → CAS `processing` → handler → mark `done`
  → ack. `IdempotencyOpts::by_dedup_key` covers the common case; custom key functions and
  marker TTLs are supported. Not an exactly-once mode.

- **Redelivery observability (B-13i/B-13j)** — `trembita_queue_redeliveries_total{stream}` counter,
  `trembita_queue_job_attempts{stream}` histogram (both recorded once per delivery), and a
  `trembita_queue_redelivered_jobs{stream}` gauge. The same count is exposed per stream in
  `/introspect/queues` and highlighted in the admin dashboard's Job queues table.

### Changed

- **`EnqueueOptions::max_attempts` is now `Option<u32>` (breaking on `0.x`)** — `None` inherits
  the stream default, `Some(0)` explicitly requests unlimited retries. Construct with
  `EnqueueOptions::max_attempts(n)` as before. Queue wire format is unchanged: the ceiling is
  resolved on the enqueueing node.
- **`RecurringJob::max_attempts(0)`** now inherits the stream default instead of forcing
  unlimited retries.
- **`JobConsumer::handle` takes a `JobContext` (breaking on `0.x`)** — generated by
  `#[consumer]`; only hand-written `JobConsumer` impls need updating.
- **`ConsumerOpts` is `Clone`, no longer `Copy`/`PartialEq`** — it now carries optional
  idempotency configuration.
- **`LeasedJob` and the queue lease wire format carry `attempts` + `dedup_key`.**
- **`QueueLifecycleEvent::Leased` carries `attempts`**; `QueueMetrics` and the metrics wire reply
  carry `redelivered`.
- **Exactly-once is explicitly not planned** — recorded in the [job-queue ADR](docs/decisions/job-queue.md)
  consequences, pointing at the effectively-once recipe.

### Fixed

- **MSRV gate never ran** — `scripts/check-msrv.sh` passed `--installed` to `rustup toolchain
  list`, which rustup 1.29 rejects; the error was swallowed and the check exited `0` claiming
  the toolchain was missing. Verified: the workspace checks clean on 1.90.

### 0.5.0 (internal, 2026-09-01)

### Changed

- **`remote_actor` → `actor` (breaking on `0.x`)** — attribute macro renamed; use `use trembita::actor::{UserActor, actor}` and `#[actor]` / `#[actor(migratable)]`.
- **Worker registration examples** — prefer `workers!(…)` call syntax (same macro; parentheses instead of brackets in docs and showcases).

### 0.4.1 (internal, 2026-09-01)

### Changed

- **API surface cleanup (breaking on `0.x`)**
  - Removed root re-export of `TrembitaCluster` / `TrembitaClusterBuilder` — use `trembita::cluster::…`.
  - Renamed `trembita::advanced` → [`trembita::cluster`](crates/trembita/src/cluster.rs) (no `advanced` alias); implementation in private `cluster_handle`.
  - **`TrembitaApp` delegates** — `node_id`, `control`, `registry`, `supervisor`, `job_queue`, `is_leader`, `shutdown` (product path).
  - **`#[doc(hidden)]`** — `TrembitaApp::cluster`, `into_cluster`, `TrembitaAppBuilder::inner_mut` (tests / custom SM only).

### Added

- **`JobOpts` + `TrembitaAppBuilder::jobs`** — declarative queue + handler + HTTP enqueue in one call.
- **`WorkerOpts` + `WorkerScale` + `TrembitaAppBuilder::workers`** — declarative actor groups (`Fixed` / `PerNode` / queue `Auto`) via [`workers!`](crates/trembita/src/worker_opts.rs) macro.
- `trembita_test_support::wait_for_trembita_app_leader` — poll leader election on a running `TrembitaApp`.

### 0.4.0 (internal, 2026-08-31)

**Gateway + ops release.** Sticky gateway sessions (HTTP/WS), optional gateway TLS,
graceful drain, self-update coordinator, and a narrower public re-export surface
(`prelude` / `advanced` / `env`).

### Added

- **Self-update coordinator** — reference [`UpgradeMachine`](crates/trembita-core/src/upgrade.rs),
  leader reconcile + local executor ([`spawn_upgrade_runtime`](crates/trembita/src/upgrade/mod.rs)),
  HTTP [`GET/POST /cluster/upgrade*`](crates/trembita-http/src/upgrade_routes.rs),
  showcase [`examples/self-update/`](examples/self-update/).
  ADR: [upgrade-coordinator](docs/decisions/upgrade-coordinator.md).
- **Gateway identity + sticky sessions** — [`GatewayIdentity`](crates/trembita/src/gateway/identity.rs),
  [`TrembitaGatewayState::extract_session`](crates/trembita/src/gateway/mod.rs),
  [`extract_session_parts`](crates/trembita/src/gateway/mod.rs) / [`open_actor_session_parts`](crates/trembita/src/gateway/mod.rs) for WebSocket handlers,
  [`extract_session_from`](crates/trembita/src/gateway/mod.rs) / [`open_actor_session_from`](crates/trembita/src/gateway/mod.rs) for plain HTTP GET,
  [`SessionHandle`](crates/trembita/src/gateway/session.rs) (cast/ask + auto-reopen),
  showcases: HTTP + WS in [`examples/realtime/`](examples/realtime/), auth submit in [`examples/stateful-workers/`](examples/stateful-workers/).
  [`GatewayHandle`](crates/trembita/src/gateway/drain.rs) graceful drain,
  [`GatewayOpts::identity_mapped`](crates/trembita/src/gateway/mod.rs) for custom session keys.
  ADR: [gateway-identity](docs/decisions/gateway-identity.md).
- **`TREMBITA_GATEWAY_DRAIN_TIMEOUT`** — product gateway connection drain (default 30s).
- **Gateway TLS (HTTPS / WSS)** — [`GatewayOpts::tls`](crates/trembita/src/gateway/mod.rs);
  `TREMBITA_GATEWAY_TLS_CERT` + `TREMBITA_GATEWAY_TLS_KEY` (server-only PEM, same semantics as admin TLS).

### Changed

- **`GatewayOpts::routes`** — closure receives [`TrembitaGatewayState`] (not `Arc<TrembitaApp>`).
  Use [`.routes_with_app`](crates/trembita/src/gateway/mod.rs) for app-only routes.
- **`spawn_gateway`** — returns [`GatewayHandle`]; [`ShutdownOpts::drain_gateway`] (default `true`).
- **`GatewayOpts`** — listen address moved into `GatewayOpts::new(addr)`; `.gateway(opts)` takes
  a single argument (matches `QueueOpts::new`, `CronOpts::new`, …). Public bool fields removed;
  use `.with_jobs_api` / `.with_actors_api` / `.with_workflows_api` only.
- **`TrembitaAppBuilder` validation** — boot fails fast when `.cron()` or `.consumer()` reference
  a stream missing from `.queue()`, or when `.workflows([…])` is set without
  `.gateway(…).with_workflows_api(true)`.
- **`.consumers(ConsumerGroup)`** — register multiple queue workers in one call (replaces
  looping `.consumer()`).
- **`.workflows([WorkflowOpts::…])`** — vector of workflow configs; optional `WorkflowOpts::named`
  prefix for multi-workflow dispatch.
- **`trembita::prelude` / `trembita::cluster` / `trembita::env`** — product imports via `prelude`;
  cluster/journal/queue internals under `cluster` (named `advanced` in 0.4.0, renamed in 0.4.1); `TREMBITA_*` helpers under `env`.
- **`http-jobs` default feature** — gateway API (`GatewayOpts`, `.gateway()`) always available;
  built-in `/jobs/*`, `/actors/*`, `/workflows/*` routes require `http-jobs` (now default).
- **Root re-exports narrowed** — advanced types moved to the `advanced` module (renamed to [`cluster`](crates/trembita/src/cluster.rs) in 0.4.1);
  [`lib.rs`](crates/trembita/src/lib.rs) rustdoc centers on [`TrembitaApp`](crates/trembita/src/app.rs).

### 0.3.0 (internal, 2026-08-31)

**Breaking product API release.** Single boot path (always QUIC cluster from env), config
structs instead of long builder chains, gateway built-in routes opt-in by default, and
auto-assigned node ids for joiners.

### Added

- **`TrembitaConfigure`** — runtime tuning via `.configure()`: `tick_period`,
  `reconcile_period`, `directory_publish_period`, `raft_config`, `admin_addr`, optional
  `node_id` override.
- **`QueueOpts` / `.queue([…])`** — durable stream registration (`name`, `lease`,
  `prefetch`); replaces `.job_stream` / `.job_queue_prefetch`.
- **`CronOpts` / `.cron([…])`** — recurring enqueue schedules, separate from queue
  registration.
- **`ActorGroupOpts`** — `.actors(name, ActorGroupOpts::new(config))` (one worker per live
  node) or `.fixed(config, n)` (fixed pool); replaces `.manage_auto` / `.manage` on the
  product builder.
- **`GatewayOpts`** — fluent `.with_jobs_api` / `.with_actors_api` /
  `.with_workflows_api` and `.routes(|app| Router)`; replaces `.gateway_addr`,
  `.gateway_routes`, and `TREMBITA_GATEWAY_NO_*` env vars.
- **Auto node id** — seed defaults to `NodeId(1)`; joiners request assignment from the
  leader; persisted at `{data_dir}/node-id`. `TREMBITA_NODE_ID` remains an optional override.
- **Join wire** — `JoinRequest.node_id: Option<NodeId>`; `JoinResponse::Accepted` carries
  assigned `node_id`.
- **Queue replicate auth** — `QueueReplicateRequest.leader_id` for leader-only apply on
  followers.

### Changed

- **Always cluster** — `RunOpts::default()` always boots a QUIC member (seed or joiner);
  topology from `TREMBITA_*` env only. `RunOpts::local()` is `#[doc(hidden)]` for tests.
- **Gateway security defaults** — built-in `/jobs/*`, `/actors/*`, `/workflows/*` are
  **disabled** unless opted in via `GatewayOpts` or `TREMBITA_GATEWAY_JOBS=1` /
  `TREMBITA_GATEWAY_ACTORS=1` / `TREMBITA_GATEWAY_WORKFLOWS=1`.
- **Removed from product builder** — `from_config`, `from_env`, `members`, `.http_routes`,
  `.gateway_jobs_api` / `.gateway_actors_api` / `.gateway_workflows_api`,
  `.recurring_job` (use `.cron`), separate actor registration helpers.
- **Showcases + template** — all four examples and `templates/trembita-app` use the unified
  `TrembitaApp::builder()…run(RunOpts::default())` pattern.
- **Docs** — getting-started, scenarios, product-scenarios ADR updated for the new API.

### Removed

- **`RunOpts::local()`** from public docs (still available for integration tests).
- **`members`** from `TrembitaConfigure` / product env surface (advanced: `TrembitaClusterBuilder`).
- **`TREMBITA_GATEWAY_NO_*`** env vars (replaced by opt-in `TREMBITA_GATEWAY_*=1` flags).
### 0.2.2 (internal, 2026-08-29)

Product throughput and ops release: queue batch/prefetch, dead-letter recovery,
cron scheduling, actor-store TTL/GC, and a dedicated **`TrembitaApp` HTTP gateway**.

### Added

- **Queue batch enqueue/ack** — wire routes `POST /raft/v1/queue/enqueue-batch` and
  `ack-batch`; `JobQueue::enqueue_batch_opts_replicated` / `ack_batch_replicated`;
  single-transaction redb paths in `RedbJobQueue`; sharded batch routing;
  `TrembitaCluster` / `TrembitaApp` `enqueue_batch` / `ack_batch`; defaults
  `DEFAULT_QUEUE_BATCH_MAX = 256`.
- **Leader prefetch cache** — `QueuePrefetchCache` on the queue leader; leases
  served from RAM when possible (`lease_prefetched`); cache fill on enqueue,
  eviction on ack; `TrembitaClusterBuilder::job_queue_prefetch` (default 256);
  safe disk fallback after leader failover.
- **Dead letter queue (DLQ)** — jobs moved to dead-letter after max attempts;
  `requeue_dead_letter` on cluster/facade; wire `QueueRequeueDeadLetter`;
  `POST /jobs/{stream}/{id}/requeue` in `trembita-http`.
- **Recurring / cron jobs** — `RecurringJob` + `parse_cron`; leader
  `queue_schedule` ticker enqueues on schedule; `TrembitaAppBuilder::recurring_job`.
- **Actor store TTL + GC** — `set_with_ttl` on `RedbActorStateStore`; periodic
  leader GC removes expired keys and replicates deletes; builder wires
  `run_actor_store_gc_ticker` with `data_dir`.
- **`TrembitaApp` gateway** — `gateway` module: separate public HTTP listener
  (`.gateway_addr`, `TREMBITA_GATEWAY`, `TREMBITA_GATEWAY_NO_JOBS` /
  `TREMBITA_GATEWAY_NO_ACTORS`); optional mount of job-queue + actors APIs;
  `.gateway_routes` for custom Axum/WebSocket handlers; integration test
  `http_gateway`.
- **`#[trembita::consumer]` macro** — generates a `JobConsumer` adapter over
  `run_queue_consumer` for typed queue workers.
- **HTTP batch routes** — `POST /jobs/{stream}/batch`, `POST /jobs/{stream}/ack-batch`
  in `trembita-http`.
- **Tests** — `trembita-actor` `queue_throughput`; facade prefetch-after-ack regression;
  gateway and DLQ HTTP routes.

### Changed

- Dashboard actor **msg/s** column; observer mailbox depth, uptime, and
  message-rate sampling (carried from 0.2.1 development).
- **`start_from_config_shared`** — QUIC production start with `TREMBITA_GATEWAY` auto-spawn (pair with `start_from_env_shared`).
- **Docs** — synced for `0.2.2`: four product showcases under [`examples/`](examples/README.md) (replaces removed `crates/trembita/examples/*`); reference KV in [`trembita_core::kv`](crates/trembita-core/src/kv.rs).

### 0.2.1 (internal, 2026-08-29)

### Changed

- **B-11b:** workspace `missing_docs = "deny"` on published crates; CI/hooks no longer allow undocumented public API.

### 0.2.0 (internal, 2026-08-29)

Product layer release: **`TrembitaApp`** facade, four scenario guides, HTTP jobs API,
WebSocket gateway example, workflow builder, and observability polish.

### Added

- **`TrembitaApp` + `TrembitaAppBuilder`** ([getting-started.md](docs/getting-started.md)) —
  product entry over `EmptyStateMachine`: `data_dir`, `job_stream`, `manage` /
  `manage_auto`, `enqueue` / `enqueue_opts`, `run_workflow` / `resume_workflow`,
  `app_config_from_env` (`TREMBITA_*`).
- **`RedbActorStateStore` + voter replication** — `StoreService`, wire routes
  `/raft/v1/actor-store/*`, `ClusterActorStateStore`; auto-wired with
  `TrembitaClusterBuilder::data_dir`.
- **`trembita-http`** — `POST /jobs/{stream}` → `202` + `job_id`; optional
  `GET /jobs/{stream}/{id}` job metadata; `TrembitaApp::jobs_api` behind `http-jobs`
  feature on `trembita`.
- **`JobQueue::job_status`** — in-memory, redb, sharded, and cluster wire lookup
  (`POST /raft/v1/queue/job-status`).
- **Workers / session product API on `TrembitaApp`** — `worker_groups`, `workers`,
  `cast`, `session` / `session_keyed`, `cast_session`.
- **`WorkflowBuilder`** — fluent cross-shard saga plans; `onboarding_workflow`
  example; `scripts/trembita-workflow.sh resume <id>` + `workflow_resume_cli` stub.
- **`examples/websocket_gateway.rs`** — axum WS + sticky `ActorSession`;
  `GATEWAY=1` edge split; optional `GATEWAY_TOKEN` auth; auto session reopen on
  `NoTarget` / TTL expiry.
- **`scripts/trembita-init.sh`** + `templates/trembita-app/` — 3-node docker-compose
  dev template (no Redis).
- **Dashboard** — `/introspect/queues`, `/introspect/sagas`, HTML panels, Prometheus
  gauges for queue depth and saga state.
- **Docs & ops** — [production-runbook.md](docs/ops/production-runbook.md),
  scenario guides ([scenarios/](docs/scenarios/README.md)), product-scenarios ADR;
  Redis de-emphasized in README/status.
- **P3 stabilization** — scenario soak bins (`soak_actor_store`, `soak_saga`,
  `soak_session`) in scheduled CI; [public-api-1.0.md](docs/decisions/public-api-1.0.md),
  [missing-docs-1.0.md](docs/decisions/missing-docs-1.0.md), [jepsen-1.0.md](docs/decisions/jepsen-1.0.md).
- **`trembita-node`** published to crates.io (reference binary; build from repo for
  production).

### Changed

- **Pre-push publish dry-run** — per-crate order includes `trembita-http`; skips
  `trembita` / `trembita-node` when workspace API is ahead of the last crates.io release.

### Fixed

- **Node router** — `Route::QueueJobStatus` wired through `NodeRouter`.
- **Examples** — websocket session key sizing; workflow resume CLI saga id type.

### 0.1.0 (internal, 2026-08-28)

Initial development release. The full workspace is in place and internally
tested; APIs are still evolving toward a 1.0 stabilization.

### Added

- **`trembita` facade** — `TrembitaCluster` + `TrembitaClusterBuilder` assemble a whole
  node (consensus runtime, actor registry/control/messaging/directory, the
  leader-only cluster supervisor, and telemetry) from one call. `start_local`
  drives an in-process/`LocalNetwork` cluster; `start_quic` runs the live
  transport. Re-exports the stable public API so users add one dependency.
- **Consensus (`trembita-core`)** — pure, I/O-free Raft state machine: leader
  election, log replication, membership, and `ReadIndex` linearizable reads.
- **Storage (`trembita-storage`)** — durable Raft log, hard state, and snapshots.
- **Transport (`trembita-net`)** — HTTP/3 over QUIC with mutual TLS between nodes,
  a `PeerDirectory` address book, and an in-memory `LocalNetwork` for tests.
  `dev-certs` feature mints a dev cluster CA + node identities.
- **Actors (`trembita-actor`)** — actor runtime, registry, cluster directory, and
  a leader-driven supervisor that auto-places one worker per node; cross-node
  messaging, spawning, and state migration.
- **Client (`trembita-client`)** — in-process and remote (HTTP/3) clients with
  transparent leader forwarding; typed client wrappers.
- **Macros (`trembita-macros`)** — `StateMachine` derive and the `remote_actor`
  attribute (auto codec generation for cross-node delivery).
- **Redis store (`trembita-store-redis`)** — a Redis-backed `ActorStateStore` for
  stateful actors, with an idempotent-worker example.
- **Dashboard (`trembita-dashboard`)** — health/admin endpoints and a live
  cluster/actor introspection view over an `Observer`.
- **Simulation (`trembita-sim`)** — deterministic harness for testing consensus.
- **`trembita-node`** — reference binary that runs a node from environment config
  (`TREMBITA_NODE_ID`, `TREMBITA_LISTEN`, `TREMBITA_ADMIN`, `TREMBITA_PEERS`, PEM cert vars),
  resolving DNS hostnames for peers.
- **Certificate provisioning** — `examples/certs/generate.sh` (portable
  OpenSSL/LibreSSL) mints a cluster CA, per-node certs, and client certs;
  documented in `docs/certs.md`.
- **Testing** — in-process QUIC/mTLS cluster test, a linearizability checker,
  the deterministic simulator, and an `e2e/` docker-compose cluster that asserts
  leader election and failover re-election over real QUIC/mTLS.
- **Docs** — consolidated decision records under `docs/decisions/`, [status.md](docs/status.md), wire protocol in `docs/protocol.md`.
- **`tracing` + `pretty_assertions`** — `trembita::init_tracing()`, rebalance/role `tracing` events; `trembita_test_support` helpers.
- **Multi-Raft runtime** ([write-sharding-multi-raft](docs/decisions/multi-raft.md)) — `ShardedNodeService`, keyed `ProposeKeyed`/`QueryKeyed`, per-group redb (`data_dir`), rebalance + cross-node group migration RPC.
- **Per-group membership** ([per-group-raft-membership](docs/decisions/cluster-membership.md#per-group-membership-multi-raft)) — `group_replication_factor`, `sync_group_membership`.
- **Multi-Raft modulus routing** ([multi-raft § modulus routing](docs/decisions/multi-raft.md#modulus-routing--keyed-batch)) — learners, `expand_shard_count`, `propose_keyed_batch`, `/introspect/raft-groups`.
- **Stable shards & dynamic catalog** ([multi-raft](docs/decisions/multi-raft.md)) — dynamic catalog (`add_raft_groups`), stable shards (default), `catalog_version`, `switch_to_stable_shards`.
- **Meta-Raft coordinator** ([meta-raft](docs/decisions/multi-raft.md#meta-raft-coordinator)) — dedicated `group-meta.redb` for join/leave, catalog, and saga journal in multi-Raft mode; group 0 is user data only.
- **Cross-shard saga** ([cross-shard-transactions](docs/decisions/multi-raft.md#cross-shard-transactions)) — `run_saga`, `resume_saga`, `StoreSagaJournal`, `MetaRaftSagaJournal`/`CompositeSagaJournal` (alias `Group0SagaJournal`), metrics; optional 2PC (`cross_shard_2pc`); durable 2PC (`durable_cross_shard_2pc`) with per-group Raft log entries, prepare timeout GC, client journal (`StoreTwoPhaseJournal`/`CompositeTwoPhaseJournal`), facade `run_keyed_2pc`/`resume_cross_shard_2pc`, metrics (`trembita_2pc_*`), and `examples/cross_shard_2pc.rs`.
- **Actor routing** ([actor-routing](docs/decisions/actor-routing.md)) — consistent-hash ring, `ActorSession`, per-group drain, `DirectoryPolicy::ReadYourWrites`.
- **Follower + lease reads** ([read-consistency](docs/decisions/client-and-routing.md#read-consistency)) — `ReadIndexConfirm` path, `RaftNode::lease_read` fast path.
- **Liveness vs membership** ([liveness-vs-membership](docs/decisions/cluster-membership.md#liveness-vs-membership)) — `reachable_nodes()`, crash-driven supervisor reconcile.
- **Discovery & ops** — seed-set + DNS discovery; cluster leave RPC; mTLS hot reload; `TrafficPolicy`; `trembita-ops` backup/restore; admin HTTPS; linearizability E2E.
- **Dev JSON wire** — `trembita/json-wire` feature.
- **Durable job queue** ([job-queue](docs/decisions/job-queue.md)) — `JobQueue` port, `RedbJobQueue`, leader `QueueService`, sync voter replication, `ClusterJobQueue`, worker autoscale.
- **Job queue v2** — sharded streams (`job_queue_sharded`), priority/delayed enqueue (`EnqueueOptions`), enqueue dedup keys, membership autoscale hook (`job_queue_membership_autoscale`); examples in `job_queue_cluster`.
- **Job queue production polish** — parallel voter replicate (`JoinSet`), replicate auth (caller must be Raft leader via `LocalTransport` / QUIC peer id), Meta-Raft persisted autoscale policy (`QueueAutoscalePolicyCommand`, `job_queue_autoscale` / `job_queue_membership_autoscale`), periodic `redb` compaction after acks.
- **Job queue docs + examples + E2E** — `/queue/*` routes in `docs/protocol.md`; `job_queue_worker` cluster follower worker + failover; `trembita-e2e-queue-client` + `e2e/queue.sh` (QUIC, 3-node).
- **Durable mailbox spool** — redb outbox/inbox for cross-node `/actor/deliver`; builder `.durable_mailbox(true)`.

### Changed

- **MSRV 1.90** — workspace MSRV probe (`cargo check --workspace --all-features` on 1.85–1.95 toolchains); floor set by `redb 4.2.0` (requires 1.90) and transitive `time 0.3.55` (requires 1.88); CI, `deploy/Dockerfile`, and `clippy.toml` aligned.
- **Leader-gated forwarded scale** — deposed nodes cannot double-place against the real leader.
- **Shared `trembita_net::RemoteError`** — unified remote error variant across actor/cluster APIs.

### Fixed

- Bounded `ask` timeout (30s); at-most-once side-effecting `ask` dedup; reply-encode errors surfaced; actor-stream backpressure on QUIC.

[Unreleased]: https://gitlab.com/lemarco/trembita/-/compare/v0.2.1...HEAD
[0.2.1]: https://gitlab.com/lemarco/trembita/-/compare/v0.2.0...v0.2.1
[0.2.0]: https://gitlab.com/lemarco/trembita/-/tags/v0.2.0
