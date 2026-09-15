# Changelog (archived pre-1.0)

**Frozen snapshot** — not maintained. For current behavior see [status.md](../status.md) and [decisions/](../decisions/). The root [CHANGELOG.md](../../CHANGELOG.md) stays minimal until **1.0.0**.

All notable changes to this project were documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the workspace
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) with all
`trembita-*` crates sharing a synchronized version ([library-and-publishing](../decisions/library-and-publishing.md)).

Under [Semantic Versioning](https://semver.org/spec/v2.0.0.html), `0.x` releases may include breaking changes on minor bumps; each is noted here.

**Crates.io:** [`0.4.0`](https://crates.io/crates/trembita) (2026-09-07).

## [Unreleased]

Target **0.5.0** — unified HTTP listener; see [unified-listener](../decisions/unified-listener.md) and [env.md](../env.md).

### Added

- **`TopicsApi`** — `POST /topics/{name}/publish`, `GET /topics/{name}`; mounted from [`.topics([…])`](../../crates/trembita/src/app/builder.rs) on the default gateway ([`.without_topics_api()`](../../crates/trembita/src/app/builder.rs) to disable).
- **Product HTTP symmetry** — [`.workflows([…])`](../../crates/trembita/src/app/builder.rs) mounts `/workflows/*` without manual `RouteTable` merge; custom [`.gateway().surfaces()`](../../crates/trembita/src/gateway/opts.rs) composes with registration-driven routes; [`.without_jobs_api()`](../../crates/trembita/src/app/builder.rs) / [`.without_workflows_api()`](../../crates/trembita/src/app/builder.rs) / [`.without_actors_api()`](../../crates/trembita/src/app/builder.rs).
- **`GET /introspect/topics`** — read-only topic lag snapshot on ops/default gateway ([`Observer::topics`](../../crates/trembita-dashboard/src/views.rs)); embedded **Event topics** dashboard panel.
- **`trembita dev http`** — `job` / `topic` / `workflow` product HTTP via `trembita-showcase-client`; `dev trigger … -- job|topic|workflow …` uses the same path.
- **`trembita doctor`** — manifest workflows/topics vs `TREMBITA_LISTEN`; warns on manual `workflows_api` / `topics_api` merges.
- **`AppManifest`** + **[`.manifest()`](../../crates/trembita/src/app/builder.rs)** — one registry for jobs, topics, workers, and workflows; scaffold emits `src/manifest.rs` with `// trembita:*` marker regions.
- **`TrembitaApp::from_env`** / **`RunOpts::from_env`** — one product entry: cluster join/listen/data_dir/job queue from `TREMBITA_*`, default gateway surfaces (ops + jobs/actors/workflows from registration) on `TREMBITA_LISTEN`.
- **Ops zero config** — `/health`, `/ready`, `/metrics`, `/dashboard`, `/introspect/*` on `TREMBITA_LISTEN` via default gateway; [`.without_ops()`](../../crates/trembita/src/app/builder.rs) to disable.
- **`trembita doctor --preflight`** — deploy checks: `TREMBITA_LISTEN` / `DATA_DIR` / `CERT_DIR`, legacy env, compose join pattern, default ops gateway wiring.
- **`trembita dev`** — `dev up|setup|stop|status|trigger|list` for product showcases in the **trembita repo** (see **Changed**: debug-build only in Unreleased).
- **`GatewayOpts::from_env`**, **`GatewayOpts::default_surfaces`**, **`TrembitaApp::default_surfaces`**, **`DefaultGatewayApis`**, **`.gateway_routes()`** — convention over manual `Gateway::new().surface()`; **`Gateway::surface_hosts`** for api/ops host split.
- **[docs/env.md](../env.md)** — product env surface (listen, data_dir, cert_dir, join_seeds, optional `GATEWAY_TOKEN`); boot warns on legacy `TREMBITA_NODE_ID` / `TREMBITA_PEERS` / split HTTP / removed `TREMBITA_GATEWAY_*` toggles.
- **`spawn_cluster_ops_http`** / **`cluster_ops_route_table`** — mount ops routes (`/health`, `/metrics`, `/dashboard`, `/introspect/*`) on a TCP listener for [`TrembitaCluster`](../../crates/trembita/src/cluster_handle/cluster.rs) without [`TrembitaApp`](../../crates/trembita/src/app/mod.rs).
- **`trembita doctor`** — scans sources and `deploy/.env.example` for removed `with_*_api`, `protect_product_apis`, `TREMBITA_ADMIN`, and missing ops/jobs route merges.

### Changed

- **`trembita-cli`** — removed **`trembita add`** (no CLI patching of `manifest.rs` / `app.rs`); register capabilities manually. **`trembita doctor`** is read-only (removed **`--fix`**). **`trembita dev`** is **debug-build only** (not in `cargo build --release` / `cargo install`).
- **MSRV 1.94** (was 1.90) — required by `sqlx` 0.9 and related dependency bumps; CI/`check-msrv.sh` aligned.
- **`trembita-node`** — ops HTTP co-hosted on **`TREMBITA_LISTEN`** (replaces `TREMBITA_ADMIN` / separate admin listener); TLS via `TREMBITA_HTTP_TLS_*`; `TREMBITA_HTTP=-` disables TCP only.
- **`product_http_from_wire`** — TCP product/ops bind is always the same `host:port` as `TREMBITA_LISTEN`; split `TREMBITA_HTTP` on another port is rejected (`TREMBITA_HTTP=-` disables TCP only).
- **Showcases / compose / `cluster-common.sh`** — configure **`TREMBITA_LISTEN` only** (one port per node for wire + HTTP).
- **Showcase Docker Compose** — dynamic join (no `TREMBITA_NODE_ID` / static `TREMBITA_PEERS`); `node-0.pem` join bootstrap + post-assign `node-N.pem` reload.

### Removed

- **`TrembitaClusterBuilder::admin_addr`** / **`admin_tls`** — use [`spawn_cluster_ops_http`](../../crates/trembita/src/gateway/cluster_ops.rs) or app gateway merges.
- **`GatewayOpts::with_jobs_api`**, **`with_actors_api`**, **`with_workflows_api`**, **`with_introspect_api`**, **`protect_product_apis`** — merge [`RouteTable`](../../crates/trembita-http/src/routing/table.rs) in `.surfaces()` with [`AuthMode::Identity`](../../crates/trembita-http/src/routing/auth.rs) where needed ([unified-listener](../decisions/unified-listener.md)).
- **Env:** `TREMBITA_ADMIN`, `TREMBITA_ADMIN_TLS_*`, and `TREMBITA_GATEWAY_*` built-in API toggles — replaced by `TREMBITA_LISTEN` + app registration / explicit route merges.

## [0.4.0] — 2026-09-07

### Added

- **Gateway routing v2** — native [`Gateway`](../../crates/trembita-http/src/gateway/mod.rs) /
  [`RouteTable`](../../crates/trembita-http/src/routing/table.rs) replace Axum; hyper edge for HTTP + WebSocket;
  [`GatewayOpts::surfaces`](../../crates/trembita/src/gateway/opts.rs); [`RouteTable::diff`](../../crates/trembita-http/src/routing/diff.rs)
  for parity tests; see [gateway-routing-v2](../decisions/gateway-routing-v2.md).
- **Route auth at dispatch** — built-in product APIs use [`AuthMode::Identity`](../../crates/trembita-http/src/routing/auth.rs)
  on the route table (no per-handler `authorize()`); `get_session`, `post_identity`, `merge_authed`,
  `put`/`delete`/`patch` sugar, WebSocket auth modes (`websocket_session`, `websocket_identity`).
- **`RequestCtx`** — `uri()`, `query_param()`, `cookie()`, `form()`, URL-decoded query in dispatch.
- **`CorsPolicy::from_env`** and `*.example.com` origin wildcards.
- **`SessionGate`** + [`CookieConfig`](../../crates/trembita-http/src/cookie_config.rs) — `with_cookie_config`, `set_session_cookie`.
- **Framework conventions** ([`framework-conventions`](../decisions/framework-conventions.md)) — standard product app layout
  (`main.rs` / `app.rs` / `config.rs` / `consumers/` / `domain/`) and app-level Cargo features.
- **`trembita-cli`** ([`trembita-cli`](../../crates/trembita-cli/)) — `trembita new`, `trembita add consumer|topic|actor|http-surface|static-site`, `trembita doctor [--fix]`; `add http-surface --session` / `--cors`. *(Removed in Unreleased: `add`, `doctor --fix`; `dev` is debug-build only.)*
- **`trembita-events-postgres`** — [`PgEventOutboxSource`](../../crates/trembita-events-postgres/src/source.rs) for transactional domain outbox drain.
- **[`DepthCache`](../../crates/trembita-jobs/src/depth_cache.rs)** — TTL cache for external backlog depth queries.
- **OTLP tracing bootstrap** ([`init_tracing_with_otlp`](../../crates/trembita-runtime/src/tracing_otlp.rs), feature `otlp`).

### Changed

- **Breaking:** `GatewayOpts::routes` and `build_gateway_router` removed; use `.surfaces()` and [`build_gateway_service`](../../crates/trembita/src/gateway/router.rs).
- **Breaking:** default QUIC/HTTP listen port is **443** (was `7443`); wire UDP and product TCP share the port ([unified-listener](../decisions/unified-listener.md)).
- **Path matching** — trailing-slash normalization on route patterns.

### Fixed

- Topic publish no longer returns `NotLeader` immediately after election (`ClusterFacts` lagged live Raft status).

### Removed

- **`HostRouter`**, **`MultiHostBuilder`**, `host_router.rs`, `trembita::axum` re-exports (see [gateway-routing-v2](../decisions/gateway-routing-v2.md)).

## [0.3.2] — 2026-09-05

> **Superseded in 0.4.0:** This release used Axum `HostRouter` (`host_router.rs`). Those APIs were **removed in 0.4.0** — migrate to [`Gateway`](../../crates/trembita-http/src/gateway/mod.rs) /
> [`RouteTable`](../../crates/trembita-http/src/routing/table.rs) ([gateway-routing-v2](../decisions/gateway-routing-v2.md)).

### Added

- **`StaticSite` gateway helper** ([`trembita-http`](../../crates/trembita-http/README.md)) — serve SPAs from
  three backends (now via [`StaticSite`](../../crates/trembita-http/src/static_site/mod.rs) + [`RouteTable::fallback`](../../crates/trembita-http/src/routing/table.rs) in 0.4.0):
  - [`StaticSource::Embedded`](../../crates/trembita-http/src/static_site/mod.rs) — compile-time bytes via
    `include_dir!` / [`embedded_from_dir`](../../crates/trembita-http/src/static_site/embedded.rs)
  - [`StaticSource::Filesystem`](../../crates/trembita-http/src/static_site/mod.rs) — directory on disk (dev/staging)
  - [`StaticSource::ObjectStore`](../../crates/trembita-http/src/static_site/object_store.rs) — S3-compatible storage
    (feature `static-s3`, proxy or CDN redirect)
- SPA fallback, cache-control presets, precompressed `.gz`/`.br` siblings, env-based config
  (`{PREFIX}_SOURCE`, `{PREFIX}_ROOT`, `{PREFIX}_BUCKET`, …).
- Virtual-host static mounting via `HostRouter::static_site` *(removed in 0.4.0 — use `.surface()` + `StaticSite`)*.

## [0.3.1] — 2026-09-04

### Fixed

- **Pre-vote election livelock (CF-028)** — [`leader_recent`](../../crates/trembita-core/src/node/vote.rs) now uses
  `last_leader_contact` (refreshed only on append/snapshot from a leader), not the election timer reset by
  campaigns; clusters elect promptly after leader loss.
- **Job plane stall with a dead voter (CF-026)** — unknown peers default to unreachable in
  [`AckWindowLiveness`](../../crates/trembita-core/src/failure_detector.rs); liveness maps clear on
  [`become_leader`](../../crates/trembita-core/src/node/role.rs); failed queue replicate rolls back local ops;
  failed lease replicate rolls back; [`ExternalBacklog::release_claim`](../../crates/trembita-jobs/src/external_backlog.rs)
  on enqueue failure so Postgres rows do not stay `claimed`.
- **Graceful shutdown on SIGTERM (CF-029)** — [`wait_for_int_or_term`](../../crates/trembita/src/shutdown_signal.rs)
  waits on SIGINT and SIGTERM; wired into [`TrembitaApp::wait_for_shutdown`](../../crates/trembita/src/app/runtime.rs)
  and `trembita-node`. Custom embedders can use [`RunOpts::with_shutdown_signal`](../../crates/trembita/src/app_opts.rs).
- **External backlog feeder observability (CF-027)** — [`feed_backlog_once`](../../crates/trembita-jobs/src/external_backlog.rs)
  logs `tracing::warn!` at `trembita::leader` on metrics, claim, enqueue, and release failures.

### Added

- **Tests** — election livelock regression tests; reachability / replication unit tests;
  `queue_enqueue_and_lease_with_one_unreachable_voter` integration test.

## [0.3.0] — 2026-09-04

### Fixed

- **Gateway fail-closed auth** — product APIs require [`GatewayOpts::identity`](../../crates/trembita/src/gateway/mod.rs);
  `protect_product_apis(true)` without identity fails at router build; env-enabled gateway APIs require
  `GATEWAY_TOKEN` / `TREMBITA_GATEWAY_TOKEN`.
- **Gateway token bypass** — empty token env returns [`IdentityError::NotConfigured`](../../crates/trembita/src/gateway/identity.rs)
  instead of accepting arbitrary Bearer/query credentials.
- **Upgrade API auth** — [`UpgradeApi`](../../crates/trembita-http/src/upgrade_routes.rs) supports [`AuthFn`]; facade wires Bearer
  auth when a gateway token env var is set.
- **Voter replication safety** — queue/topic/store leaders error when other voters exist but none are reachable
  ([`replication_peers`](../../crates/trembita-runtime/src/leader_replicate.rs)).
- **Store replicate auth** — `StoreReplicateRequest` now carries `leader_id` (aligned with queue/topic).
- **Admin bind default** — reference `trembita-node` admin listens on `127.0.0.1:8080` instead of `0.0.0.0:8080`.

### Changed (breaking)

- **Typed client wire errors** — `ClientResponse::Error(String)` replaced with `ClientResponse::Err(ClientWireError)` in `trembita-proto`.
- **Typed product wire errors** — queue/topic/actor-store reply `error` fields use [`ProductWireError`](../../crates/trembita-proto/src/product.rs) instead of plain strings.

### Changed
- **Shared leader replication** — [`forward_to_leader`](../../crates/trembita-runtime/src/leader_replicate.rs), [`fanout_product_replicate`](../../crates/trembita-runtime/src/leader_replicate.rs), [`replicate_reply_err`](../../crates/trembita-runtime/src/leader_replicate.rs); queue/topic/store services deduplicated.
- **Topic service split** — [`topic_service.rs`](../../crates/trembita-events/src/topic_service/mod.rs) → `topic_service/{replication,handlers,dispatch,cluster_topic}.rs`.
- **Gateway identity hardening** — [`GatewayBearerIdentity`](../../crates/trembita/src/gateway/identity.rs) requires `Authorization: Bearer` + `X-Trembita-User` header (no query `?token=` / `?user=`); [`GatewayTokenIdentity`](../../crates/trembita/src/gateway/identity.rs) accepts Bearer or query token.

### Added

- **Shared replication helpers** — [`fanout_replicate`](../../crates/trembita-runtime/src/leader_replicate.rs),
  [`after_failed_attempt`](../../crates/trembita-runtime/src/retry.rs), [`WorkerId`](../../crates/trembita-proto/src/worker.rs) and
  [`BoxFuture`](../../crates/trembita-proto/src/lib.rs) in proto (decouples `trembita-events` from `trembita-jobs`).
- **Gateway HTTP body limit** — 16 MiB [`DefaultBodyLimit`](../../crates/trembita/src/gateway/mod.rs) on product router.
- **Gateway validation API** — [`validate_gateway_config`](../../crates/trembita/src/gateway/mod.rs),
  [`GatewayConfigError`](../../crates/trembita/src/gateway/mod.rs).
- **Gateway rate limiting** — optional [`GatewayOpts::rate_limit_per_sec`](../../crates/trembita/src/gateway/mod.rs)
  (`429 Too Many Requests` when exceeded).
- **Queue module split** — [`queue.rs`](../../crates/trembita-jobs/src/queue/mod.rs) split into `queue/{types,port,in_memory,consumer,time}.rs`.
- **Raft node split** — [`node.rs`](../../crates/trembita-core/src/node/mod.rs) split into `node/{types,bootstrap,accessors,events,vote,append,replicate,read,…}.rs` (ordinary submodules; cross-file helpers use `pub(in crate::node)`).
- **Queue handlers split** — [`handlers.rs`](../../crates/trembita-jobs/src/queue_service/handlers/mod.rs) split into `queue_service/handlers/{enqueue,lease,ack,query,dead_letter}.rs`.
- **Runtime event loop split** — [`event_loop.rs`](../../crates/trembita-runtime/src/runtime/event_loop/mod.rs) split into `runtime/event_loop/{core,settle,envelope,two_phase,membership,catalog}.rs`.
- **App module split** — [`app.rs`](../../crates/trembita/src/app/mod.rs) split into `app/{types,shutdown,builder,runtime,workflow}.rs`.
- **Cluster handle module split** — [`cluster_handle.rs`](../../crates/trembita/src/cluster_handle/mod.rs) split into `cluster_handle/{facts,telemetry,cluster,errors}.rs`.
- **Gateway module split** — [`gateway/mod.rs`](../../crates/trembita/src/gateway/mod.rs) split into `gateway/{config,opts,state,router,spawn}.rs`.
- **Builder module split** — join/autoscale/error helpers extracted from [`builder/mod.rs`](../../crates/trembita/src/builder/mod.rs); [`TrembitaClusterBuilder`](../../crates/trembita/src/builder/cluster/mod.rs) split into `builder/cluster/{config,assemble,products,start,types,topic_leader}.rs`.
- **Runtime module split** — [`runtime.rs`](../../crates/trembita-runtime/src/runtime/mod.rs) split into `runtime/{types,handle,event_loop,spawn,service,wire}.rs`.
- **Registry module split** — [`registry.rs`](../../crates/trembita-runtime/src/registry/mod.rs) split into `registry/{actor,errors,reply,pool,lifecycle,refs,inner,observer}.rs`.
- **Queue stream registry** — `QueueService` holds one `Mutex<QueueStreamRegistry>` instead of five separate mutex maps.
- **Shared redb adapter helpers** — [`redb_util`](../../crates/trembita-storage/src/redb_util.rs) (`now_ms`, `open_database`, `open_mutex_database`); migrated queue/topic/actor-store/mailbox spool, event-outbox cursors, backlog-settle outbox, and queue schedules.

### Fixed

- **Env merge precedence** — [`merge_app_config`](../../crates/trembita/src/builder/cluster/mod.rs) applies env only for unset
  builder fields; code-set [`.members`](../../crates/trembita/src/app/mod.rs), [`.join_as`](../../crates/trembita/src/app/mod.rs),
  [`.configure({ node_id })`](../../crates/trembita/src/configure.rs), etc. win over `TREMBITA_*` on [`.run`](../../crates/trembita/src/app/mod.rs).
- **Dynamic join role** — joiners now send the role from [`.join_as`](../../crates/trembita/src/builder/cluster/mod.rs) /
  `TREMBITA_JOIN_ROLE` instead of always requesting `JoinRole::Learner`; seed-side
  [`allow_voter_join`](../../crates/trembita/src/app/mod.rs) / `TREMBITA_ALLOW_VOTER_JOIN` is wired from env.

### Added

- **Product builder parity** — [`TrembitaAppBuilder::allow_join`](../../crates/trembita/src/app/mod.rs),
  [`allow_leave`](../../crates/trembita/src/app/mod.rs), [`join`](../../crates/trembita/src/app/mod.rs) /
  [`join_seeds`](../../crates/trembita/src/app/mod.rs), [`cert_watch`](../../crates/trembita/src/app/mod.rs),
  [`voters(n)`](../../crates/trembita/src/app/mod.rs); `TREMBITA_GATEWAY_INTROSPECT`, `TREMBITA_CERT_WATCH_SECS`,
  `TREMBITA_VOTER_REPLACEMENT*`, and `TREMBITA_ADMIN` opt-in (disabled unless set).
- **Join / TLS diagnostics** — debug pre-vote rejections (`trembita::raft`) and warn on QUIC handshake
  failures (`trembita::net`).
- **External backlog after leader change (CF-026)** — [`ExternalBacklog::reclaim_abandoned_claims`](../../crates/trembita-jobs/src/external_backlog.rs)
  runs on leadership acquire; [`PgBacklog`](../../crates/trembita-backlog-postgres/src/lib.rs) resets `claimed → pending`.
- **Leader task observability** — [`run_leader_loop`](../../crates/trembita-runtime/src/leader_task.rs) logs acquire,
  step-down, and stop at `trembita::leader` (feeder, drainer, supervisor, …).

## [0.2.3] — 2026-09-04

### Fixed

- **`TrembitaApp` env node id** — [`merge_app_config`](../../crates/trembita/src/builder/cluster/mod.rs) now applies
  `AppConfig::node_id`, so `TREMBITA_NODE_ID` and joiner assignment (`NodeId(0)`) work without
  `.configure(TrembitaConfigure { node_id: … })`.

### Added

- **Product cluster membership API** — [`TrembitaAppBuilder::members`](../../crates/trembita/src/app/mod.rs),
  [`allow_voter_join`](../../crates/trembita/src/app/mod.rs),
  [`voter_replacement`](../../crates/trembita/src/app/mod.rs),
  [`voter_replacement_grace_ticks`](../../crates/trembita/src/app/mod.rs), and
  [`on_leader`](../../crates/trembita/src/app/mod.rs) forward to the inner cluster builder.

## [0.2.2] — 2026-09-03

### Added

- **Leader task primitive** — [`LeaderSession`](../../crates/trembita-runtime/src/leader_task.rs),
  [`run_leader_loop`](../../crates/trembita-runtime/src/leader_task.rs), and
  [`TrembitaClusterBuilder::on_leader`](../../crates/trembita/src/builder/cluster/mod.rs) for periodic
  leader-only work with `first_in_term` ([`leader-task`](../decisions/leader-task.md));
  internal feeder, drainer, autoscaler, supervisor, schedule, GC, and topic loops migrated.

- **`EventOutboxSource` port** — leader drainer from application transactional outbox into
  [`EventTopic`](../../crates/trembita-events/src/topic.rs)
  ([`event-outbox`](../decisions/event-outbox.md)); [`TopicOpts::outbox`](../../crates/trembita/src/topic_opts.rs),
  [`.event_outbox_source()`](../../crates/trembita/src/app/mod.rs), cursor checkpoint at
  `{data_dir}/event-outbox-cursors.redb`.

- **`IntrospectApi` on product gateway** — mount read-only `/introspect/*` beside
  `JobsApi` / `ActorsApi` / `WorkflowsApi` with [`AuthFn`](../../crates/trembita-http/src/lib.rs)
  ([`introspect-api`](../decisions/introspect-api.md)); [`GatewayOpts::with_introspect_api`](../../crates/trembita/src/gateway/mod.rs),
  [`TrembitaApp::introspect_observer`](../../crates/trembita/src/app/mod.rs).

### Fixed

- **Event outbox drainer** — settlement errors no longer advance the cursor; failed publishes retry on the next leader tick.

## [0.2.1] — 2026-09-03

### Changed (breaking)

- **`Settlement::Done` / `BacklogSettleOutcome::Done`** — now carry `attempts` (queue attempt counter at ack). [`PgBacklog`](../../crates/trembita-backlog-postgres/src/lib.rs) applies `Done` only when the row is still `claimed` and `attempts` matches, ignoring stale settle-outbox entries after key reuse.

### Fixed

- **`EnqueueOptions::dedup_key` rustdoc** — documents that the key is held while a job exists and released after ack ([CF-010](../backlog.md#cf-010--dedup_key-lifecycle-docs)).

## [0.2.0] — 2026-09-03

**First trembita release on [crates.io](https://crates.io/crates/trembita).** Synchronized workspace `0.2.0` (16 published crates).

### Added

- **Crate layout** — [`trembita-runtime`](../../crates/trembita-runtime/), [`trembita-jobs`](../../crates/trembita-jobs/),
  [`trembita-events`](../../crates/trembita-events/), and [`trembita-actor-store`](../../crates/trembita-actor-store/).
- **Facade modules** — `trembita::runtime`, `trembita::jobs`, `trembita::events`, `trembita::actor_store`;
  user actors via `trembita::actor::{UserActor, actor}` and `#[actor]` / `#[actor(migratable)]`.
- **Dev tooling** — unpublished [`trembita-tools`](../../crates/trembita-tools/) bundles node/ops/showcase binaries;
  examples use `trembita_tools::showcase_*`.
- **`EnqueueOptions::max_attempts` as `Option<u32>`** — `None` inherits the stream default,
  `Some(0)` requests unlimited retries; same inheritance for `RecurringJob::max_attempts(0)`.
- **`JobConsumer::handle` with `JobContext`** — generated by `#[consumer]` for typed consumers.
- **`ConsumerOpts`** — `Clone` with optional idempotency config.
- **Queue lease wire format** — `LeasedJob` carries `attempts` and `dedup_key`.
- **Product API surface** — cluster types under `trembita::cluster` (not root re-exports);
  [`TrembitaApp`](../../crates/trembita/src/app/mod.rs) as the primary entry; cluster internals
  `#[doc(hidden)]` on `TrembitaApp::cluster`, `into_cluster`, `TrembitaAppBuilder::inner_mut`.
- **`GatewayOpts::new(addr)`** — listen address at construction; `.gateway(opts)` takes a single argument;
  `.routes` receives [`TrembitaGatewayState`](../../crates/trembita/src/gateway/mod.rs) (use
  [`.routes_with_app`](../../crates/trembita/src/gateway/mod.rs) for app-only routes).
- **Always-on cluster** — `RunOpts::default()` boots a QUIC member (seed or joiner) from `TREMBITA_*` env.
- **Gateway security defaults** — built-in `/jobs/*`, `/actors/*`, `/workflows/*` disabled unless opted in.
- **Root re-exports narrowed** — cluster/journal/queue internals under [`trembita::cluster`](../../crates/trembita/src/cluster.rs);
  [`lib.rs`](../../crates/trembita/src/lib.rs) rustdoc centers on [`TrembitaApp`](../../crates/trembita/src/app/mod.rs).
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
  dynamic catalog, Meta-Raft coordinator, `/introspect/raft-groups` ([multi-raft](../decisions/multi-raft.md)).
- **Cross-shard saga + 2PC** — `run_saga`, durable 2PC journal, `run_keyed_2pc`, metrics, examples
  ([cross-shard-transactions](../decisions/multi-raft.md#cross-shard-transactions)).
- **Actor routing** — consistent-hash ring, `ActorSession`, `DirectoryPolicy::ReadYourWrites`
  ([actor-routing](../decisions/actor-routing.md)).
- **Follower + lease reads**, **liveness vs membership**, **discovery & ops** (join/leave, mTLS reload,
  `TrafficPolicy`, backup/restore, admin TLS, linearizability E2E).
- **Dev JSON wire** — `trembita/json-wire` feature.
- **Durable job queue** — `RedbJobQueue`, leader `QueueService`, voter replication, sharded streams,
  dedup keys, autoscale hooks, DLQ, batch enqueue/ack, prefetch cache, cron schedules, `#[consumer]`,
  HTTP batch routes ([job-queue](../decisions/job-queue.md)).
- **Durable mailbox spool** — redb outbox/inbox for cross-node `/actor/deliver`.
- **`TrembitaApp` + `TrembitaAppBuilder`** — product entry over `EmptyStateMachine`; `data_dir`, queues,
  actors, workflows, gateway, `app_config_from_env` ([getting-started.md](../getting-started.md)).
- **`RedbActorStateStore` + voter replication** — `/raft/v1/actor-store/*`, auto-wired with `data_dir`.
- **`trembita-http`** — job enqueue/status HTTP API; optional gateway mount for jobs/actors/workflows.
- **`JobQueue::job_status`** — cluster wire lookup (`POST /raft/v1/queue/job-status`).
- **Workers / session on `TrembitaApp`** — cast, ask, sticky `ActorSession`, worker groups.
- **`WorkflowBuilder`** + cross-shard workflow HTTP API.
- **`TrembitaConfigure`**, **`QueueOpts`**, **`CronOpts`**, **`ActorGroupOpts`**, **`GatewayOpts`**
  — declarative product builder registration.
- **`JobOpts` + `.jobs()`**, **`WorkerOpts` + `.workers()`** / [`workers!`](../../crates/trembita/src/worker_opts.rs).
- **Self-update coordinator** — reference upgrade SM, HTTP `/cluster/upgrade*`, showcase
  ([upgrade-coordinator](../decisions/upgrade-coordinator.md)).
- **Gateway identity + sticky sessions** — bearer auth, session extract/reopen, graceful drain, optional TLS
  ([gateway-identity](../decisions/gateway-identity.md)).
- **`TREMBITA_GATEWAY_DRAIN_TIMEOUT`**, **gateway TLS env vars**.
- **Job delivery semantics** — `JobContext`, idempotency helpers, redelivery metrics, effectively-once recipe
  ([background-jobs](../scenarios/background-jobs.md)).
- **`#[consumer_json]`**, **graceful consumer drain**, **HTTP queue metadata parity**, **saga step dedup**
  helper, **queue → actor bridge** example, **state placement** guide.
- **Gateway bearer auth** — `GatewayBearerIdentity`, `protect_product_apis`, optional `AuthFn`.
- **`trembita init` v2** template, E2E gateway/idempotency scripts.
- **Durable event topics** — [`EventTopic`](../../crates/trembita-events/src/topic.rs), subscriptions, compaction
  ([event-topics](../decisions/event-topics.md)).
- **`ScheduleSource` port** — dynamic recurring jobs ([schedule-source](../decisions/schedule-source.md)).
- **`ExternalBacklog` port** + **`trembita-backlog-postgres`** (`PgBacklog`).
- **Workload governor** — compute tokens + gateway-aware consumer tuning
  ([workload-governor](../decisions/workload-governor.md)).
- **`HostRouter`** — virtual-host gateway dispatch.
- **Learner join (elastic scale-out)** — default `JoinRole::Learner`; `allow_voter_join` for voter expansion
  ([cluster-elasticity](../decisions/cluster-elasticity.md)).
- **Automatic voter replacement** — promote caught-up learners when a voter is lost.
- **External compute load** — `JobOpts::compute_cost`, optional `ExternalLoad` port
  ([external-load](../decisions/external-load.md)).
- **Auto node id** — seed `NodeId(1)`, joiner assignment, `{data_dir}/node-id` persistence.
- **Certificate tooling**, **testing pyramid** (sim, QUIC integration, e2e compose), **scenario guides**,
  **production runbook**, **`scripts/trembita-init.sh`** + app template, **soak benchmarks**.
- **Homogeneous cluster nodes** — no role-split env vars; gateway, consumer, and workload tuning via
  the product builder and opt-in `TREMBITA_GATEWAY_*=1` flags.
- **`BacklogFeedOpts::consumer_instances`** — defaults to [`ConsumerCount::Live`](../../crates/trembita-jobs/src/external_backlog.rs).
- **`QueueLifecycleEvent::Leased` carries `attempts`**; queue metrics expose `redelivered`.
- **Delivery semantics documented** — exactly-once explicitly out of scope ([job-queue ADR](../decisions/job-queue.md)).
- **`spawn_gateway` returns [`GatewayHandle`](../../crates/trembita/src/gateway/drain.rs)** — default gateway drain on shutdown.
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


[Unreleased]: https://gitlab.com/lemarco/trembita/-/compare/v0.4.0...HEAD
[0.4.0]: https://gitlab.com/lemarco/trembita/-/compare/v0.3.2...v0.4.0
[0.3.2]: https://gitlab.com/lemarco/trembita/-/compare/v0.3.1...v0.3.2
[0.3.1]: https://gitlab.com/lemarco/trembita/-/compare/v0.3.0...v0.3.1
[0.3.0]: https://gitlab.com/lemarco/trembita/-/compare/v0.2.3...v0.3.0
[0.2.3]: https://gitlab.com/lemarco/trembita/-/compare/v0.2.2...v0.2.3
[0.2.2]: https://gitlab.com/lemarco/trembita/-/compare/v0.2.1...v0.2.2
[0.2.1]: https://gitlab.com/lemarco/trembita/-/compare/v0.2.0...v0.2.1
[0.2.0]: https://gitlab.com/lemarco/trembita/-/tags/v0.2.0
