# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the workspace
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) with all
`crafty-*` crates sharing a synchronized version ([library-and-publishing](docs/decisions/library-and-publishing.md)).

Pre-1.0 (`0.x`): breaking changes may land on minor bumps and are noted here.

## [Unreleased]

### Added

- **Durable event topics** — [`EventTopic`](crates/crafty-actor/src/topic.rs) pub/sub with named
  subscriptions, independent cursors, compaction by `min(cursor)`, retention thresholds, and
  voter replication ([`event-topics`](docs/decisions/event-topics.md)); [`TopicOpts`](crates/crafty/src/topic_opts.rs),
  [`.topics()`](crates/crafty/src/app.rs), [`CraftyApp::publish`](crates/crafty/src/app.rs),
  `#[consumer(..., subscription = "...")]` for subscribers. Not a replacement for transactional
  outbox — see ADR.
- **`ScheduleSource` port** — dynamic recurring-job schedules polled on the queue
  leader ([`schedule-source`](docs/decisions/schedule-source.md)); [`.schedule_source()`](crates/crafty/src/app.rs)
  on `CraftyAppBuilder`; [`.cron()`](crates/crafty/src/app.rs) reimplemented via
  [`StaticScheduleSource`](crates/crafty-actor/src/schedule_source.rs). Source errors and
  bootstrap empty polls never wipe persisted schedules; diff reconcile replicates
  `UpsertSchedule` / `RemoveSchedule`.
- **`ExternalBacklog` port** — leader feeder + settle outbox for tier-C windows over an
  external source of truth ([`external-backlog`](docs/decisions/external-backlog.md));
  [`JobOpts::backlog`](crates/crafty/src/job_opts.rs), honest autoscale depth via
  `effective_queue_depth`.
- **`crafty-backlog-postgres`** — optional published crate with [`PgBacklog`](crates/crafty-backlog-postgres/src/lib.rs)
  (`SKIP LOCKED` claim + idempotent settle).
- **Workload governor** — per-node [`ComputeTokenPool`](crates/crafty-actor/src/compute_token.rs) +
  [`WorkloadGovernor`](crates/crafty-actor/src/workload.rs) consumer tuning from gateway load
  ([`workload-governor`](docs/decisions/workload-governor.md)); [`WorkloadOpts`](crates/crafty/src/workload.rs),
  [`.workload()`](crates/crafty/src/app.rs). Removed static role env vars (`CRAFTY_ROLE`, etc.).
- **`HostRouter`** — virtual-host dispatch for product gateway HTTP ([`crafty-http`](crates/crafty-http/src/host_router.rs));
  strict default with loopback dev fallback.

### Removed

- **`CRAFTY_ROLE`**, **`CRAFTY_GATEWAY_ONLY`**, **`CRAFTY_NO_CONSUMER`** — use homogeneous nodes +
  `.workload()` and deployment choice (register consumers or not) instead.

## [0.5.2] — 2026-09-02

**Product polish & cross-scenario composition (B-14).** Gateway auth for built-in
product APIs, HTTP queue metadata parity, typed JSON consumers, graceful consumer
drain, saga step dedup helpers, and runnable cross-scenario examples.

### Added

- **Gateway bearer auth (B-14a)** — [`GatewayBearerIdentity`](crates/crafty/src/gateway/identity.rs)
  (`GATEWAY_TOKEN` / `Authorization: Bearer …`); [`GatewayOpts::protect_product_apis`](crates/crafty/src/gateway/mod.rs)
  requires identity on built-in `/jobs/*`, `/actors/*`, `/workflows/*`. Optional `AuthFn`
  hook on `crafty-http` API state for custom checks.
- **`crafty init` v2 (B-14b)** — template ships `JobOpts`, `#[consumer]`,
  `IdempotencyOpts::by_dedup_key`, `default_max_attempts(5)`, and gateway auth defaults.
- **E2E gateway jobs (B-14c)** — `crafty/tests/gateway_jobs_http.rs`, `./e2e/gateway_jobs.sh`
  (in-process HTTP batch enqueue through product gateway).
- **`IdempotencyOpts::retain_for` (B-14d)** — alias for marker TTL on done keys
  (high-volume cleanup; default remains forever).
- **Graceful consumer drain (B-14e)** — `run_queue_consumer` finishes the in-flight
  lease batch on stop; [`ShutdownOpts::consumer_drain_timeout`](crates/crafty/src/app.rs)
  bounds shutdown wait.
- **`#[consumer_json]` (B-14f)** — deserializes JSON payloads before the handler;
  raw `&[u8]` remains the default for `#[consumer]`.
- **HTTP queue parity (B-14g)** — enqueue accepts per-job `max_attempts`; job status
  exposes `dedup`, `attempts`, and `is_redelivery`.
- **Idempotency + failover test (B-14h)** — `dedup_key_survives_leader_failover` in
  `crafty/tests/queue.rs`; `./e2e/queue_idempotency.sh` runs idempotency regression.
- **Saga step dedup helper (B-14i)** — [`WorkflowBuilder::step_dedup_key`](crates/crafty/src/workflow.rs),
  [`CraftyApp::enqueue_workflow_step`](crates/crafty/src/app.rs).
- **State placement cheat sheet (B-14j)** — [docs/scenarios/state-placement.md](docs/scenarios/state-placement.md)
  (SM vs queue vs actor store vs saga journal).
- **Queue → actor bridge (B-14k)** — `examples/background-jobs/src/bridge.rs` +
  [`ConsumerOpts::on_app`](crates/crafty/src/consumer.rs) pattern; consumer delegates
  side effects via `CraftyApp::cast`.
- **`ConsumerOpts` builders** — `.instance()`, `.batch()`, `.idle_sleep()` replace
  struct-literal fields for test and app code.

### Changed

- **`examples/realtime/`** — `ShowcaseGatewayIdentity` replaced with
  `GatewayBearerIdentity` + `protect_product_apis(true)`; docs and `trigger-http.sh`
  updated.
- **`QueueJobStatusReply::dedup_key`** — always serialized on the wire (fixes postcard
  decode 503 when the field was omitted).

## [0.5.1] — 2026-09-01

**Job queue delivery semantics & idempotency (B-13).** At-least-once is now
documented as a contract rather than a caveat, consumers can tell a redelivery
from a first attempt, and redelivery is visible in metrics and the dashboard.

### Added

- **Stream-level attempt ceilings (B-13d)** — [`JobOpts::default_max_attempts`](crates/crafty/src/job_opts.rs) /
  [`QueueOpts::default_max_attempts`](crates/crafty/src/queue_opts.rs) apply to enqueues that cannot
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

- **Redelivery observability (B-13i/B-13j)** — `crafty_queue_redeliveries_total{stream}` counter,
  `crafty_queue_job_attempts{stream}` histogram (both recorded once per delivery), and a
  `crafty_queue_redelivered_jobs{stream}` gauge. The same count is exposed per stream in
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

## [0.5.0] — 2026-09-01

### Changed

- **`remote_actor` → `actor` (breaking on `0.x`)** — attribute macro renamed; use `use crafty::actor::{UserActor, actor}` and `#[actor]` / `#[actor(migratable)]`.
- **Worker registration examples** — prefer `workers!(…)` call syntax (same macro; parentheses instead of brackets in docs and showcases).

## [0.4.1] — 2026-09-01

### Changed

- **API surface cleanup (breaking on `0.x`)**
  - Removed root re-export of `CraftyCluster` / `CraftyClusterBuilder` — use `crafty::cluster::…`.
  - Renamed `crafty::advanced` → [`crafty::cluster`](crates/crafty/src/cluster.rs) (no `advanced` alias); implementation in private `cluster_handle`.
  - **`CraftyApp` delegates** — `node_id`, `control`, `registry`, `supervisor`, `job_queue`, `is_leader`, `shutdown` (product path).
  - **`#[doc(hidden)]`** — `CraftyApp::cluster`, `into_cluster`, `CraftyAppBuilder::inner_mut` (tests / custom SM only).

### Added

- **`JobOpts` + `CraftyAppBuilder::jobs`** — declarative queue + handler + HTTP enqueue in one call.
- **`WorkerOpts` + `WorkerScale` + `CraftyAppBuilder::workers`** — declarative actor groups (`Fixed` / `PerNode` / queue `Auto`) via [`workers!`](crates/crafty/src/worker_opts.rs) macro.
- `crafty_test_support::wait_for_crafty_app_leader` — poll leader election on a running `CraftyApp`.

## [0.4.0] — 2026-08-31

**Gateway + ops release.** Sticky gateway sessions (HTTP/WS), optional gateway TLS,
graceful drain, self-update coordinator, and a narrower public re-export surface
(`prelude` / `advanced` / `env`).

### Added

- **Self-update coordinator** — reference [`UpgradeMachine`](crates/crafty-core/src/upgrade.rs),
  leader reconcile + local executor ([`spawn_upgrade_runtime`](crates/crafty/src/upgrade/mod.rs)),
  HTTP [`GET/POST /cluster/upgrade*`](crates/crafty-http/src/upgrade_routes.rs),
  showcase [`examples/self-update/`](examples/self-update/).
  ADR: [upgrade-coordinator](docs/decisions/upgrade-coordinator.md).
- **Gateway identity + sticky sessions** — [`GatewayIdentity`](crates/crafty/src/gateway/identity.rs),
  [`CraftyGatewayState::extract_session`](crates/crafty/src/gateway/mod.rs),
  [`extract_session_parts`](crates/crafty/src/gateway/mod.rs) / [`open_actor_session_parts`](crates/crafty/src/gateway/mod.rs) for WebSocket handlers,
  [`extract_session_from`](crates/crafty/src/gateway/mod.rs) / [`open_actor_session_from`](crates/crafty/src/gateway/mod.rs) for plain HTTP GET,
  [`SessionHandle`](crates/crafty/src/gateway/session.rs) (cast/ask + auto-reopen),
  showcases: HTTP + WS in [`examples/realtime/`](examples/realtime/), auth submit in [`examples/stateful-workers/`](examples/stateful-workers/).
  [`GatewayHandle`](crates/crafty/src/gateway/drain.rs) graceful drain,
  [`GatewayOpts::identity_mapped`](crates/crafty/src/gateway/mod.rs) for custom session keys.
  ADR: [gateway-identity](docs/decisions/gateway-identity.md).
- **`CRAFTY_GATEWAY_DRAIN_TIMEOUT`** — product gateway connection drain (default 30s).
- **Gateway TLS (HTTPS / WSS)** — [`GatewayOpts::tls`](crates/crafty/src/gateway/mod.rs);
  `CRAFTY_GATEWAY_TLS_CERT` + `CRAFTY_GATEWAY_TLS_KEY` (server-only PEM, same semantics as admin TLS).

### Changed

- **`GatewayOpts::routes`** — closure receives [`CraftyGatewayState`] (not `Arc<CraftyApp>`).
  Use [`.routes_with_app`](crates/crafty/src/gateway/mod.rs) for app-only routes.
- **`spawn_gateway`** — returns [`GatewayHandle`]; [`ShutdownOpts::drain_gateway`] (default `true`).
- **`GatewayOpts`** — listen address moved into `GatewayOpts::new(addr)`; `.gateway(opts)` takes
  a single argument (matches `QueueOpts::new`, `CronOpts::new`, …). Public bool fields removed;
  use `.with_jobs_api` / `.with_actors_api` / `.with_workflows_api` only.
- **`CraftyAppBuilder` validation** — boot fails fast when `.cron()` or `.consumer()` reference
  a stream missing from `.queue()`, or when `.workflows([…])` is set without
  `.gateway(…).with_workflows_api(true)`.
- **`.consumers(ConsumerGroup)`** — register multiple tier-C workers in one call (replaces
  looping `.consumer()`).
- **`.workflows([WorkflowOpts::…])`** — vector of workflow configs; optional `WorkflowOpts::named`
  prefix for multi-workflow dispatch.
- **`crafty::prelude` / `crafty::cluster` / `crafty::env`** — product imports via `prelude`;
  cluster/journal/queue internals under `cluster` (named `advanced` in 0.4.0, renamed in 0.4.1); `CRAFTY_*` helpers under `env`.
- **`http-jobs` default feature** — gateway API (`GatewayOpts`, `.gateway()`) always available;
  built-in `/jobs/*`, `/actors/*`, `/workflows/*` routes require `http-jobs` (now default).
- **Root re-exports narrowed** — advanced types moved to the `advanced` module (renamed to [`cluster`](crates/crafty/src/cluster.rs) in 0.4.1);
  [`lib.rs`](crates/crafty/src/lib.rs) rustdoc centers on [`CraftyApp`](crates/crafty/src/app.rs).

## [0.3.0] — 2026-08-31

**Breaking product API release.** Single boot path (always QUIC cluster from env), config
structs instead of long builder chains, gateway built-in routes opt-in by default, and
auto-assigned node ids for joiners.

### Added

- **`CraftyConfigure`** — runtime tuning via `.configure()`: `tick_period`,
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
  `.gateway_routes`, and `CRAFTY_GATEWAY_NO_*` env vars.
- **Auto node id** — seed defaults to `NodeId(1)`; joiners request assignment from the
  leader; persisted at `{data_dir}/node-id`. `CRAFTY_NODE_ID` remains an optional override.
- **Join wire** — `JoinRequest.node_id: Option<NodeId>`; `JoinResponse::Accepted` carries
  assigned `node_id`.
- **Queue replicate auth** — `QueueReplicateRequest.leader_id` for leader-only apply on
  followers.

### Changed

- **Always cluster** — `RunOpts::default()` always boots a QUIC member (seed or joiner);
  topology from `CRAFTY_*` env only. `RunOpts::local()` is `#[doc(hidden)]` for tests.
- **Gateway security defaults** — built-in `/jobs/*`, `/actors/*`, `/workflows/*` are
  **disabled** unless opted in via `GatewayOpts` or `CRAFTY_GATEWAY_JOBS=1` /
  `CRAFTY_GATEWAY_ACTORS=1` / `CRAFTY_GATEWAY_WORKFLOWS=1`.
- **Removed from product builder** — `from_config`, `from_env`, `members`, `.http_routes`,
  `.gateway_jobs_api` / `.gateway_actors_api` / `.gateway_workflows_api`,
  `.recurring_job` (use `.cron`), separate actor registration helpers.
- **Showcases + template** — all four examples and `templates/crafty-app` use the unified
  `CraftyApp::builder()…run(RunOpts::default())` pattern.
- **Docs** — getting-started, scenarios, product-scenarios ADR updated for the new API.

### Removed

- **`RunOpts::local()`** from public docs (still available for integration tests).
- **`members`** from `CraftyConfigure` / product env surface (advanced: `CraftyClusterBuilder`).
- **`CRAFTY_GATEWAY_NO_*`** env vars (replaced by opt-in `CRAFTY_GATEWAY_*=1` flags).


Product throughput and ops release: queue batch/prefetch, dead-letter recovery,
cron scheduling, actor-store TTL/GC, and a dedicated **`CraftyApp` HTTP gateway**.

### Added

- **Queue batch enqueue/ack** — wire routes `POST /raft/v1/queue/enqueue-batch` and
  `ack-batch`; `JobQueue::enqueue_batch_opts_replicated` / `ack_batch_replicated`;
  single-transaction redb paths in `RedbJobQueue`; sharded batch routing;
  `CraftyCluster` / `CraftyApp` `enqueue_batch` / `ack_batch`; defaults
  `DEFAULT_QUEUE_BATCH_MAX = 256`.
- **Leader prefetch cache** — `QueuePrefetchCache` on the queue leader; leases
  served from RAM when possible (`lease_prefetched`); cache fill on enqueue,
  eviction on ack; `CraftyClusterBuilder::job_queue_prefetch` (default 256);
  safe disk fallback after leader failover.
- **Dead letter queue (DLQ)** — jobs moved to dead-letter after max attempts;
  `requeue_dead_letter` on cluster/facade; wire `QueueRequeueDeadLetter`;
  `POST /jobs/{stream}/{id}/requeue` in `crafty-http`.
- **Recurring / cron jobs** — `RecurringJob` + `parse_cron`; leader
  `queue_schedule` ticker enqueues on schedule; `CraftyAppBuilder::recurring_job`.
- **Actor store TTL + GC** — `set_with_ttl` on `RedbActorStateStore`; periodic
  leader GC removes expired keys and replicates deletes; builder wires
  `run_actor_store_gc_ticker` with `data_dir`.
- **`CraftyApp` gateway** — `gateway` module: separate public HTTP listener
  (`.gateway_addr`, `CRAFTY_GATEWAY`, `CRAFTY_GATEWAY_NO_JOBS` /
  `CRAFTY_GATEWAY_NO_ACTORS`); optional mount of tier C jobs + actors APIs;
  `.gateway_routes` for custom Axum/WebSocket handlers; integration test
  `http_gateway`.
- **`#[crafty::consumer]` macro** — generates a `JobConsumer` adapter over
  `run_queue_consumer` for typed queue workers.
- **HTTP batch routes** — `POST /jobs/{stream}/batch`, `POST /jobs/{stream}/ack-batch`
  in `crafty-http`.
- **Tests** — `crafty-actor` `queue_throughput`; facade prefetch-after-ack regression;
  gateway and DLQ HTTP routes.

### Changed

- Dashboard actor **msg/s** column; observer mailbox depth, uptime, and
  message-rate sampling (carried from 0.2.1 development).
- **`start_from_config_shared`** — QUIC production start with `CRAFTY_GATEWAY` auto-spawn (pair with `start_from_env_shared`).
- **Docs** — synced for `0.2.2`: four product showcases under [`examples/`](examples/README.md) (replaces removed `crates/crafty/examples/*`); reference KV in [`crafty_core::kv`](crates/crafty-core/src/kv.rs).

## [0.2.1] — 2026-08-29

### Changed

- **B-11b:** workspace `missing_docs = "deny"` on published crates; CI/hooks no longer allow undocumented public API.

## [0.2.0] — 2026-08-29

Product layer release: **`CraftyApp`** facade, four scenario guides, HTTP jobs API,
WebSocket gateway example, workflow builder, and observability polish — still
pre-1.0 (`0.x`); facade API may change on minor bumps until 1.0 RC.

### Added

- **`CraftyApp` + `CraftyAppBuilder`** ([getting-started.md](docs/getting-started.md)) —
  product entry over `EmptyStateMachine`: `data_dir`, `job_stream`, `manage` /
  `manage_auto`, `enqueue` / `enqueue_opts`, `run_workflow` / `resume_workflow`,
  `app_config_from_env` (`CRAFTY_*`).
- **`RedbActorStateStore` + voter replication** — `StoreService`, wire routes
  `/raft/v1/actor-store/*`, `ClusterActorStateStore`; auto-wired with
  `CraftyClusterBuilder::data_dir`.
- **`crafty-http`** — `POST /jobs/{stream}` → `202` + `job_id`; optional
  `GET /jobs/{stream}/{id}` job metadata; `CraftyApp::jobs_api` behind `http-jobs`
  feature on `crafty`.
- **`JobQueue::job_status`** — in-memory, redb, sharded, and cluster wire lookup
  (`POST /raft/v1/queue/job-status`).
- **Workers / session product API on `CraftyApp`** — `worker_groups`, `workers`,
  `cast`, `session` / `session_keyed`, `cast_session`.
- **`WorkflowBuilder`** — fluent cross-shard saga plans; `onboarding_workflow`
  example; `scripts/crafty-workflow.sh resume <id>` + `workflow_resume_cli` stub.
- **`examples/websocket_gateway.rs`** — axum WS + sticky `ActorSession`;
  `GATEWAY=1` edge split; optional `GATEWAY_TOKEN` auth; auto session reopen on
  `NoTarget` / TTL expiry.
- **`scripts/crafty-init.sh`** + `templates/crafty-app/` — 3-node docker-compose
  dev template (no Redis).
- **Dashboard** — `/introspect/queues`, `/introspect/sagas`, HTML panels, Prometheus
  gauges for queue depth and saga state.
- **Docs & ops** — [production-runbook.md](docs/ops/production-runbook.md),
  scenario guides ([scenarios/](docs/scenarios/README.md)), product-scenarios ADR;
  Redis de-emphasized in README/status.
- **P3 stabilization** — scenario soak bins (`soak_actor_store`, `soak_saga`,
  `soak_session`) in scheduled CI; [public-api-1.0.md](docs/decisions/public-api-1.0.md),
  [missing-docs-1.0.md](docs/decisions/missing-docs-1.0.md), [jepsen-1.0.md](docs/decisions/jepsen-1.0.md).
- **`crafty-node`** published to crates.io (reference binary; build from repo for
  production).

### Changed

- **Pre-push publish dry-run** — per-crate order includes `crafty-http`; skips
  `crafty` / `crafty-node` when workspace API is ahead of the last crates.io release.

### Fixed

- **Node router** — `Route::QueueJobStatus` wired through `NodeRouter`.
- **Examples** — websocket session key sizing; workflow resume CLI saga id type.

## [0.1.0] — 2026-08-28

Initial development release. The full workspace is in place and internally
tested; APIs are still evolving toward a 1.0 stabilization.

### Added

- **`crafty` facade** — `CraftyCluster` + `CraftyClusterBuilder` assemble a whole
  node (consensus runtime, actor registry/control/messaging/directory, the
  leader-only cluster supervisor, and telemetry) from one call. `start_local`
  drives an in-process/`LocalNetwork` cluster; `start_quic` runs the live
  transport. Re-exports the stable public API so users add one dependency.
- **Consensus (`crafty-core`)** — pure, I/O-free Raft state machine: leader
  election, log replication, membership, and `ReadIndex` linearizable reads.
- **Storage (`crafty-storage`)** — durable Raft log, hard state, and snapshots.
- **Transport (`crafty-net`)** — HTTP/3 over QUIC with mutual TLS between nodes,
  a `PeerDirectory` address book, and an in-memory `LocalNetwork` for tests.
  `dev-certs` feature mints a dev cluster CA + node identities.
- **Actors (`crafty-actor`)** — actor runtime, registry, cluster directory, and
  a leader-driven supervisor that auto-places one worker per node; cross-node
  messaging, spawning, and state migration.
- **Client (`crafty-client`)** — in-process and remote (HTTP/3) clients with
  transparent leader forwarding; typed client wrappers.
- **Macros (`crafty-macros`)** — `StateMachine` derive and the `remote_actor`
  attribute (auto codec generation for cross-node delivery).
- **Redis store (`crafty-store-redis`)** — a Redis-backed `ActorStateStore` for
  stateful actors, with an idempotent-worker example.
- **Dashboard (`crafty-dashboard`)** — health/admin endpoints and a live
  cluster/actor introspection view over an `Observer`.
- **Simulation (`crafty-sim`)** — deterministic harness for testing consensus.
- **`crafty-node`** — reference binary that runs a node from environment config
  (`CRAFTY_NODE_ID`, `CRAFTY_LISTEN`, `CRAFTY_ADMIN`, `CRAFTY_PEERS`, PEM cert vars),
  resolving DNS hostnames for peers.
- **Certificate provisioning** — `examples/certs/generate.sh` (portable
  OpenSSL/LibreSSL) mints a cluster CA, per-node certs, and client certs;
  documented in `docs/certs.md`.
- **Testing** — in-process QUIC/mTLS cluster test, a linearizability checker,
  the deterministic simulator, and an `e2e/` docker-compose cluster that asserts
  leader election and failover re-election over real QUIC/mTLS.
- **Docs** — consolidated decision records under `docs/decisions/`, [status.md](docs/status.md), wire protocol in `docs/protocol.md`.
- **`tracing` + `pretty_assertions`** — `crafty::init_tracing()`, rebalance/role `tracing` events; `crafty_test_support` helpers.
- **Multi-Raft runtime** ([write-sharding-multi-raft](docs/decisions/multi-raft.md)) — `ShardedNodeService`, keyed `ProposeKeyed`/`QueryKeyed`, per-group redb (`data_dir`), rebalance + cross-node group migration RPC.
- **Per-group membership** ([per-group-raft-membership](docs/decisions/cluster-membership.md#per-group-membership-multi-raft)) — `group_replication_factor`, `sync_group_membership`.
- **Tier 1 multi-Raft** ([tier1-multi-raft-advances](docs/decisions/multi-raft.md#tier-1-advances-landed)) — learners, `expand_shard_count`, `propose_keyed_batch`, `/introspect/raft-groups`.
- **Tier 2 multi-Raft** ([tier2-multi-raft-architecture](docs/decisions/multi-raft.md)) — dynamic catalog (`add_raft_groups`), stable shards (default), `catalog_version`, `switch_to_stable_shards`.
- **Meta-Raft coordinator** ([meta-raft](docs/decisions/multi-raft.md#meta-raft-coordinator)) — dedicated `group-meta.redb` for join/leave, catalog, and saga journal in multi-Raft mode; group 0 is user data only.
- **Cross-shard saga** ([cross-shard-transactions](docs/decisions/multi-raft.md#cross-shard-transactions)) — `run_saga`, `resume_saga`, `StoreSagaJournal`, `MetaRaftSagaJournal`/`CompositeSagaJournal` (alias `Group0SagaJournal`), metrics; optional 2PC (`cross_shard_2pc`); durable 2PC (`durable_cross_shard_2pc`) with per-group Raft log entries, prepare timeout GC, client journal (`StoreTwoPhaseJournal`/`CompositeTwoPhaseJournal`), facade `run_keyed_2pc`/`resume_cross_shard_2pc`, metrics (`crafty_2pc_*`), and `examples/cross_shard_2pc.rs`.
- **Actor routing Tier 3** ([actor-routing-tier3](docs/decisions/actor-routing-tier3.md)) — consistent-hash ring, `ActorSession`, per-group drain, `DirectoryPolicy::ReadYourWrites`.
- **Follower + lease reads** ([read-consistency](docs/decisions/client-and-routing.md#read-consistency)) — `ReadIndexConfirm` path, `RaftNode::lease_read` fast path.
- **Liveness vs membership** ([liveness-vs-membership](docs/decisions/cluster-membership.md#liveness-vs-membership)) — `reachable_nodes()`, crash-driven supervisor reconcile.
- **Discovery & ops** — seed-set + DNS discovery; cluster leave RPC; mTLS hot reload; `TrafficPolicy`; `crafty-ops` backup/restore; admin HTTPS; linearizability E2E.
- **Dev JSON wire** — `crafty/json-wire` feature.
- **Durable job queue (tier C)** ([job-queue](docs/decisions/job-queue.md)) — `JobQueue` port, `RedbJobQueue`, leader `QueueService`, sync voter replication, `ClusterJobQueue`, worker autoscale.
- **Job queue v2** — sharded streams (`job_queue_sharded`), priority/delayed enqueue (`EnqueueOptions`), enqueue dedup keys, membership autoscale hook (`job_queue_membership_autoscale`); examples in `job_queue_cluster`.
- **Job queue production polish** — parallel voter replicate (`JoinSet`), replicate auth (caller must be Raft leader via `LocalTransport` / QUIC peer id), Meta-Raft persisted autoscale policy (`QueueAutoscalePolicyCommand`, `job_queue_autoscale` / `job_queue_membership_autoscale`), periodic `redb` compaction after acks.
- **Job queue docs + examples + E2E** — `/queue/*` routes in `docs/protocol.md`; `job_queue_worker` cluster follower worker + failover; `crafty-e2e-queue-client` + `e2e/queue.sh` (QUIC, 3-node).
- **Durable mailbox spool** — redb outbox/inbox for cross-node `/actor/deliver`; builder `.durable_mailbox(true)`.

### Changed

- **MSRV 1.90** — workspace MSRV probe (`cargo check --workspace --all-features` on 1.85–1.95 toolchains); floor set by `redb 4.2.0` (requires 1.90) and transitive `time 0.3.55` (requires 1.88); CI, `deploy/Dockerfile`, and `clippy.toml` aligned.
- **Leader-gated forwarded scale** — deposed nodes cannot double-place against the real leader.
- **Shared `crafty_net::RemoteError`** — unified remote error variant across actor/cluster APIs.

### Fixed

- Bounded `ask` timeout (30s); at-most-once side-effecting `ask` dedup; reply-encode errors surfaced; actor-stream backpressure on QUIC.

[Unreleased]: https://gitlab.com/lemarco/craft/-/compare/v0.5.0...HEAD
[0.5.0]: https://gitlab.com/lemarco/craft/-/compare/v0.4.1...v0.5.0
[0.4.1]: https://gitlab.com/lemarco/craft/-/compare/v0.4.0...v0.4.1
[0.4.0]: https://gitlab.com/lemarco/craft/-/compare/v0.3.0...v0.4.0
[0.3.0]: https://gitlab.com/lemarco/craft/-/compare/v0.2.2...v0.3.0
[0.2.2]: https://gitlab.com/lemarco/craft/-/compare/v0.2.1...v0.2.2
[0.2.1]: https://gitlab.com/lemarco/craft/-/compare/v0.2.0...v0.2.1
[0.2.0]: https://gitlab.com/lemarco/craft/-/compare/v0.1.0...v0.2.0
[0.1.0]: https://gitlab.com/lemarco/craft/-/tags/v0.1.0
