# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the workspace
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) with all
`trembita-*` crates sharing a synchronized version ([library-and-publishing](docs/decisions/library-and-publishing.md)).

Under [Semantic Versioning](https://semver.org/spec/v2.0.0.html), `0.x` releases may include breaking changes on minor bumps; each is noted here.

**Crates.io:** [`0.2.2`](https://crates.io/crates/trembita) (2026-09-03). See [0.2.3](#023--2026-09-04) below for the latest release.

## [Unreleased]

### Fixed

- **Gateway fail-closed auth** — product APIs require [`GatewayOpts::identity`](crates/trembita/src/gateway/mod.rs);
  `protect_product_apis(true)` without identity fails at router build; env-enabled gateway APIs require
  `GATEWAY_TOKEN` / `TREMBITA_GATEWAY_TOKEN`.
- **Gateway token bypass** — empty token env returns [`IdentityError::NotConfigured`](crates/trembita/src/gateway/identity.rs)
  instead of accepting arbitrary Bearer/query credentials.
- **Upgrade API auth** — [`UpgradeApi`](crates/trembita-http/src/upgrade_routes.rs) supports [`AuthFn`]; facade wires Bearer
  auth when a gateway token env var is set.
- **Voter replication safety** — queue/topic/store leaders error when other voters exist but none are reachable
  ([`replication_peers`](crates/trembita-runtime/src/leader_replicate.rs)).
- **Store replicate auth** — `StoreReplicateRequest` now carries `leader_id` (aligned with queue/topic).
- **Admin bind default** — reference `trembita-node` admin listens on `127.0.0.1:8080` instead of `0.0.0.0:8080`.

### Added

- **Shared replication helpers** — [`fanout_replicate`](crates/trembita-runtime/src/leader_replicate.rs),
  [`after_failed_attempt`](crates/trembita-runtime/src/retry.rs), [`WorkerId`](crates/trembita-proto/src/worker.rs) and
  [`BoxFuture`](crates/trembita-proto/src/lib.rs) in proto (decouples `trembita-events` from `trembita-jobs`).
- **Gateway HTTP body limit** — 16 MiB [`DefaultBodyLimit`](crates/trembita/src/gateway/mod.rs) on product router.
- **Gateway validation API** — [`validate_gateway_config`](crates/trembita/src/gateway/mod.rs),
  [`GatewayConfigError`](crates/trembita/src/gateway/mod.rs).
- **Gateway rate limiting** — optional [`GatewayOpts::rate_limit_per_sec`](crates/trembita/src/gateway/mod.rs)
  (`429 Too Many Requests` when exceeded).
- **Builder module split** — join/autoscale/error helpers extracted from [`builder/mod.rs`](crates/trembita/src/builder/mod.rs); [`TrembitaClusterBuilder`](crates/trembita/src/builder/cluster/mod.rs) split into `builder/cluster/{config,assemble,products,start,types,topic_leader}.rs`.
- **Runtime module split** — [`runtime.rs`](crates/trembita-runtime/src/runtime/mod.rs) split into `runtime/{types,handle,event_loop,spawn,service,wire}.rs`.
- **Registry module split** — [`registry.rs`](crates/trembita-runtime/src/registry/mod.rs) split into `registry/{actor,errors,reply,pool,lifecycle,refs,inner,observer}.rs`.
- **Queue stream registry** — `QueueService` holds one `Mutex<QueueStreamRegistry>` instead of five separate mutex maps.
- **Shared redb adapter helpers** — [`redb_util`](crates/trembita-storage/src/redb_util.rs) (`now_ms`, `open_database`, `open_mutex_database`); migrated queue/topic/actor-store/mailbox spool, event-outbox cursors, backlog-settle outbox, and queue schedules.

### Fixed

- **Env merge precedence** — [`merge_app_config`](crates/trembita/src/builder.rs) applies env only for unset
  builder fields; code-set [`.members`](crates/trembita/src/app.rs), [`.join_as`](crates/trembita/src/app.rs),
  [`.configure({ node_id })`](crates/trembita/src/configure.rs), etc. win over `TREMBITA_*` on [`.run`](crates/trembita/src/app.rs).
- **Dynamic join role** — joiners now send the role from [`.join_as`](crates/trembita/src/builder.rs) /
  `TREMBITA_JOIN_ROLE` instead of always requesting `JoinRole::Learner`; seed-side
  [`allow_voter_join`](crates/trembita/src/app.rs) / `TREMBITA_ALLOW_VOTER_JOIN` is wired from env.

### Added

- **Product builder parity** — [`TrembitaAppBuilder::allow_join`](crates/trembita/src/app.rs),
  [`allow_leave`](crates/trembita/src/app.rs), [`join`](crates/trembita/src/app.rs) /
  [`join_seeds`](crates/trembita/src/app.rs), [`cert_watch`](crates/trembita/src/app.rs),
  [`voters(n)`](crates/trembita/src/app.rs); `TREMBITA_GATEWAY_INTROSPECT`, `TREMBITA_CERT_WATCH_SECS`,
  `TREMBITA_VOTER_REPLACEMENT*`, and `TREMBITA_ADMIN` opt-in (disabled unless set).
- **Join / TLS diagnostics** — debug pre-vote rejections (`trembita::raft`) and warn on QUIC handshake
  failures (`trembita::net`).
- **External backlog after leader change (CF-026)** — [`ExternalBacklog::reclaim_abandoned_claims`](crates/trembita-jobs/src/external_backlog.rs)
  runs on leadership acquire; [`PgBacklog`](crates/trembita-backlog-postgres/src/lib.rs) resets `claimed → pending`.
- **Leader task observability** — [`run_leader_loop`](crates/trembita-runtime/src/leader_task.rs) logs acquire,
  step-down, and stop at `trembita::leader` (feeder, drainer, supervisor, …).

## [0.2.3] — 2026-09-04

### Fixed

- **`TrembitaApp` env node id** — [`merge_app_config`](crates/trembita/src/builder.rs) now applies
  `AppConfig::node_id`, so `TREMBITA_NODE_ID` and joiner assignment (`NodeId(0)`) work without
  `.configure(TrembitaConfigure { node_id: … })`.

### Added

- **Product cluster membership API** — [`TrembitaAppBuilder::members`](crates/trembita/src/app.rs),
  [`allow_voter_join`](crates/trembita/src/app.rs),
  [`voter_replacement`](crates/trembita/src/app.rs),
  [`voter_replacement_grace_ticks`](crates/trembita/src/app.rs), and
  [`on_leader`](crates/trembita/src/app.rs) forward to the inner cluster builder.

## [0.2.2] — 2026-09-03

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

- **`IntrospectApi` on product gateway** — mount read-only `/introspect/*` beside
  `JobsApi` / `ActorsApi` / `WorkflowsApi` with [`AuthFn`](crates/trembita-http/src/lib.rs)
  ([`introspect-api`](docs/decisions/introspect-api.md)); [`GatewayOpts::with_introspect_api`](crates/trembita/src/gateway/mod.rs),
  [`TrembitaApp::introspect_observer`](crates/trembita/src/app.rs).

### Fixed

- **Event outbox drainer** — settlement errors no longer advance the cursor; failed publishes retry on the next leader tick.

## [0.2.1] — 2026-09-03

### Changed (breaking)

- **`Settlement::Done` / `BacklogSettleOutcome::Done`** — now carry `attempts` (queue attempt counter at ack). [`PgBacklog`](crates/trembita-backlog-postgres/src/lib.rs) applies `Done` only when the row is still `claimed` and `attempts` matches, ignoring stale settle-outbox entries after key reuse.

### Fixed

- **`EnqueueOptions::dedup_key` rustdoc** — documents that the key is held while a job exists and released after ack ([CF-010](docs/backlog.md#cf-010--dedup_key-lifecycle-docs)).

## [0.2.0] — 2026-09-03

**First trembita release on [crates.io](https://crates.io/crates/trembita).** Synchronized workspace `0.2.0` (16 published crates).

### Added

- **Crate layout** — [`trembita-runtime`](crates/trembita-runtime/), [`trembita-jobs`](crates/trembita-jobs/),
  [`trembita-events`](crates/trembita-events/), and [`trembita-actor-store`](crates/trembita-actor-store/).
- **Facade modules** — `trembita::runtime`, `trembita::jobs`, `trembita::events`, `trembita::actor_store`;
  user actors via `trembita::actor::{UserActor, actor}` and `#[actor]` / `#[actor(migratable)]`.
- **Dev tooling** — unpublished [`trembita-tools`](crates/trembita-tools/) bundles node/ops/showcase binaries;
  examples use `trembita_tools::showcase_*`.
- **`EnqueueOptions::max_attempts` as `Option<u32>`** — `None` inherits the stream default,
  `Some(0)` requests unlimited retries; same inheritance for `RecurringJob::max_attempts(0)`.
- **`JobConsumer::handle` with `JobContext`** — generated by `#[consumer]` for typed consumers.
- **`ConsumerOpts`** — `Clone` with optional idempotency config.
- **Queue lease wire format** — `LeasedJob` carries `attempts` and `dedup_key`.
- **Product API surface** — cluster types under `trembita::cluster` (not root re-exports);
  [`TrembitaApp`](crates/trembita/src/app.rs) as the primary entry; cluster internals
  `#[doc(hidden)]` on `TrembitaApp::cluster`, `into_cluster`, `TrembitaAppBuilder::inner_mut`.
- **`GatewayOpts::new(addr)`** — listen address at construction; `.gateway(opts)` takes a single argument;
  `.routes` receives [`TrembitaGatewayState`](crates/trembita/src/gateway/mod.rs) (use
  [`.routes_with_app`](crates/trembita/src/gateway/mod.rs) for app-only routes).
- **Always-on cluster** — `RunOpts::default()` boots a QUIC member (seed or joiner) from `TREMBITA_*` env.
- **Gateway security defaults** — built-in `/jobs/*`, `/actors/*`, `/workflows/*` disabled unless opted in.
- **Root re-exports narrowed** — cluster/journal/queue internals under [`trembita::cluster`](crates/trembita/src/cluster.rs);
  [`lib.rs`](crates/trembita/src/lib.rs) rustdoc centers on [`TrembitaApp`](crates/trembita/src/app.rs).
- **`trembita` facade** — `TrembitaCluster` + `TrembitaClusterBuilder`; `start_local` and `start_quic`.
- **Consensus (`trembita-core`)** — pure Raft FSM: election, replication, membership, `ReadIndex` reads.
- **Storage (`trembita-storage`)** — durable Raft log, hard state, snapshots.
- **Transport (`trembita-net`)** — HTTP/3 / QUIC + mTLS, `PeerDirectory`, `LocalNetwork`; `dev-certs` feature.
- **Runtime split (`trembita-runtime`)** — actor registry, directory, supervisor, cross-node messaging and migration.
- **Client (`trembita-client`)** — in-process and remote HTTP/3 clients with leader forwarding.
- **Macros (`trembita-macros`)** — `StateMachine` derive and `#[actor]` codec generation.
- **Redis store (`trembita-store-redis`)** — optional `ActorStateStore` adapter.
- **Dashboard (`trembita-dashboard`)** — health/admin, live cluster/actor introspection, Prometheus metrics.
- **Simulation (`trembita-sim`)** — deterministic consensus harness.
- **Multi-Raft** — sharded groups, keyed propose/query, rebalance, cross-node migration, modulus routing,
  dynamic catalog, Meta-Raft coordinator, `/introspect/raft-groups` ([multi-raft](docs/decisions/multi-raft.md)).
- **Cross-shard saga + 2PC** — `run_saga`, durable 2PC journal, `run_keyed_2pc`, metrics, examples
  ([cross-shard-transactions](docs/decisions/multi-raft.md#cross-shard-transactions)).
- **Actor routing** — consistent-hash ring, `ActorSession`, `DirectoryPolicy::ReadYourWrites`
  ([actor-routing](docs/decisions/actor-routing.md)).
- **Follower + lease reads**, **liveness vs membership**, **discovery & ops** (join/leave, mTLS reload,
  `TrafficPolicy`, backup/restore, admin TLS, linearizability E2E).
- **Dev JSON wire** — `trembita/json-wire` feature.
- **Durable job queue** — `RedbJobQueue`, leader `QueueService`, voter replication, sharded streams,
  dedup keys, autoscale hooks, DLQ, batch enqueue/ack, prefetch cache, cron schedules, `#[consumer]`,
  HTTP batch routes ([job-queue](docs/decisions/job-queue.md)).
- **Durable mailbox spool** — redb outbox/inbox for cross-node `/actor/deliver`.
- **`TrembitaApp` + `TrembitaAppBuilder`** — product entry over `EmptyStateMachine`; `data_dir`, queues,
  actors, workflows, gateway, `app_config_from_env` ([getting-started.md](docs/getting-started.md)).
- **`RedbActorStateStore` + voter replication** — `/raft/v1/actor-store/*`, auto-wired with `data_dir`.
- **`trembita-http`** — job enqueue/status HTTP API; optional gateway mount for jobs/actors/workflows.
- **`JobQueue::job_status`** — cluster wire lookup (`POST /raft/v1/queue/job-status`).
- **Workers / session on `TrembitaApp`** — cast, ask, sticky `ActorSession`, worker groups.
- **`WorkflowBuilder`** + cross-shard workflow HTTP API.
- **`TrembitaConfigure`**, **`QueueOpts`**, **`CronOpts`**, **`ActorGroupOpts`**, **`GatewayOpts`**
  — declarative product builder registration.
- **`JobOpts` + `.jobs()`**, **`WorkerOpts` + `.workers()`** / [`workers!`](crates/trembita/src/worker_opts.rs).
- **Self-update coordinator** — reference upgrade SM, HTTP `/cluster/upgrade*`, showcase
  ([upgrade-coordinator](docs/decisions/upgrade-coordinator.md)).
- **Gateway identity + sticky sessions** — bearer auth, session extract/reopen, graceful drain, optional TLS
  ([gateway-identity](docs/decisions/gateway-identity.md)).
- **`TREMBITA_GATEWAY_DRAIN_TIMEOUT`**, **gateway TLS env vars**.
- **Job delivery semantics** — `JobContext`, idempotency helpers, redelivery metrics, effectively-once recipe
  ([background-jobs](docs/scenarios/background-jobs.md)).
- **`#[consumer_json]`**, **graceful consumer drain**, **HTTP queue metadata parity**, **saga step dedup**
  helper, **queue → actor bridge** example, **state placement** guide.
- **Gateway bearer auth** — `GatewayBearerIdentity`, `protect_product_apis`, optional `AuthFn`.
- **`trembita init` v2** template, E2E gateway/idempotency scripts.
- **Durable event topics** — [`EventTopic`](crates/trembita-events/src/topic.rs), subscriptions, compaction
  ([event-topics](docs/decisions/event-topics.md)).
- **`ScheduleSource` port** — dynamic recurring jobs ([schedule-source](docs/decisions/schedule-source.md)).
- **`ExternalBacklog` port** + **`trembita-backlog-postgres`** (`PgBacklog`).
- **Workload governor** — compute tokens + gateway-aware consumer tuning
  ([workload-governor](docs/decisions/workload-governor.md)).
- **`HostRouter`** — virtual-host gateway dispatch.
- **Learner join (elastic scale-out)** — default `JoinRole::Learner`; `allow_voter_join` for voter expansion
  ([cluster-elasticity](docs/decisions/cluster-elasticity.md)).
- **Automatic voter replacement** — promote caught-up learners when a voter is lost.
- **External compute load** — `JobOpts::compute_cost`, optional `ExternalLoad` port
  ([external-load](docs/decisions/external-load.md)).
- **Auto node id** — seed `NodeId(1)`, joiner assignment, `{data_dir}/node-id` persistence.
- **Certificate tooling**, **testing pyramid** (sim, QUIC integration, e2e compose), **scenario guides**,
  **production runbook**, **`scripts/trembita-init.sh`** + app template, **soak benchmarks**.
- **Homogeneous cluster nodes** — no role-split env vars; gateway, consumer, and workload tuning via
  the product builder and opt-in `TREMBITA_GATEWAY_*=1` flags.
- **`BacklogFeedOpts::consumer_instances`** — defaults to [`ConsumerCount::Live`](crates/trembita-jobs/src/external_backlog.rs).
- **`QueueLifecycleEvent::Leased` carries `attempts`**; queue metrics expose `redelivered`.
- **Delivery semantics documented** — exactly-once explicitly out of scope ([job-queue ADR](docs/decisions/job-queue.md)).
- **`spawn_gateway` returns [`GatewayHandle`](crates/trembita/src/gateway/drain.rs)** — default gateway drain on shutdown.
- **Batch registration** — `.consumers(ConsumerGroup)`, `.workflows([WorkflowOpts::…])`.
- **`trembita::prelude` / `trembita::cluster` / `trembita::env`** module layout.
- **`http-jobs` default feature** — built-in product HTTP routes when enabled.
- **Showcases + template** — `TrembitaApp::builder()…run(RunOpts::default())` pattern.
- **Dashboard** — queue/saga introspection, msg/s column, redelivery highlighting.
- **Documentation** — contributor guide, doc link checker, scenario ADRs, messaging layer naming.
- **MSRV 1.90**, **release/CI gates** (`gate.sh`, lefthook, `ci-fast-lane.sh`).
- **Workspace `missing_docs = "deny"`** on published crates; pre-push publish dry-run in dependency order.
- **Shared `trembita_net::RemoteError`** across actor/cluster APIs.
- **Leader-gated forwarded scale** — deposed nodes cannot double-place against the real leader.
- **Actor messaging** — bounded `ask` timeout, at-most-once side-effecting `ask` dedup, actor-stream
  backpressure on QUIC.


[Unreleased]: https://gitlab.com/lemarco/trembita/-/compare/v0.2.3...HEAD
[0.2.3]: https://gitlab.com/lemarco/trembita/-/compare/v0.2.2...v0.2.3
[0.2.2]: https://gitlab.com/lemarco/trembita/-/compare/v0.2.1...v0.2.2
[0.2.1]: https://gitlab.com/lemarco/trembita/-/compare/v0.2.0...v0.2.1
[0.2.0]: https://gitlab.com/lemarco/trembita/-/tags/v0.2.0
