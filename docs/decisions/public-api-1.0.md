# Public API scope

**Status:** Accepted  
**Date:** 2026-08-28  
**Backlog:** B-11a

## Scope

The **`trembita` facade** is the semver surface for product teams. Internal crates
(`trembita-core`, `trembita-net`, …) are published for advanced users; semver guarantees apply primarily to types re-exported from `trembita`.

## Product surface (`TrembitaApp`)

| Area | Types | Notes |
|------|-------|-------|
| App | `TrembitaApp`, `TrembitaAppBuilder`, `AppManifest`, `TrembitaConfigure`, `JobOpts`, `QueueOpts`, `CronOpts`, `CapManifest`, `GatewayOpts`, `RunOpts` | Primary entry; register **capabilities** + jobs/topics/workflows in [`.manifest`](../../crates/trembita/src/app/manifest.rs); cluster join via `TREMBITA_*` + [`from_env`](../../crates/trembita/src/app/runtime.rs) / [`from_config`](../../crates/trembita/src/app/runtime.rs) |
| Env (embedders) | [`trembita::env::AppConfig`](../../crates/trembita/src/env.rs), `EnvOverrides`, `app_config_from_env` | Parse once in `main`, then `TrembitaApp::from_config(cfg)`. [`trembita-node`](../../crates/trembita-tools/src/bin/node.rs): [`NodeConfig::into_app_config`](../../crates/trembita-tools/src/node/config.rs) copies [`EnvOverrides`](../../crates/trembita-assembly/src/env_config.rs) (e.g. `peers` for `TREMBITA_PEERS`) so builder merge applies static membership. |
| App runtime (product) | `node_id`, `enqueue`, `shutdown_graceful`, capability call sites (`.via(&app)`) | Day-to-day product control on `TrembitaApp` |
| App runtime (advanced) | `cast`, `ask`, `registry`, `supervisor`, `TrembitaApp::actors_api`, `WorkerOpts`, `ActorGroupOpts` | Legacy/custom **`UserActor`** + optional `/actors/*` ([`WorkerOpts::http_cast`](../../crates/trembita/src/worker_opts.rs)); default gateway excludes actors API ([product-terminology](product-terminology.md)) |
| Identity | `NodeId`, `Security`, `PeerDirectory`, cert reload helpers | Multi-node wiring |
| Jobs | `JobQueue`, `EnqueueOptions`, `run_queue_consumer`, `ClusterJobQueue` | Via `TrembitaApp::enqueue` |
| Workflows | `WorkflowBuilder`, `run_workflow` / `resume_workflow` on app | Saga journal durable |
| Cap store | `trembita::capstore` (`CapStore`, `RedbCapStore`), `OpCtx::store` / `require_store`, `store_get` / `store_set` | Auto via `data_dir`; legacy `actor_store` re-export |
| Cap DX | `CapManifest`, `OpCtx`, `CapDeps`, `CapIngress`, `cap_*` gateway helpers | [`capability-dx`](capability-dx.md) |
| Sessions | `ActorSession`, `TrembitaApp::session_keyed` | Sticky routing |
| Observability | `init_tracing`, `Metrics`, `TrembitaEvent` | Ops routes on unified `TREMBITA_LISTEN` (or explicit gateway merge) |

## Cluster & client API (`trembita::cluster`)

| Area | Types | Notes |
|------|-------|-------|
| Cluster | `trembita::cluster::{TrembitaCluster, StartError, …}` | Runtime handle, queues, journals — not the product builder |
| Client | `RemoteClient`, `run_saga`, `run_keyed_saga`, `KeyedClient` | Re-exported `trembita::client` |
| Multi-Raft | `propose_keyed`, `add_raft_groups`, `RaftGroupsView` | Runtime on [`TrembitaCluster`](../../crates/trembita-assembly/src/cluster_handle/cluster.rs); product boot via env + [`TrembitaConfigure`](../../crates/trembita/src/configure.rs) |
| Saga / 2PC journals | `MetaRaftSagaJournal`, `CompositeSagaJournal`, `StoreTwoPhaseJournal` | Ops / recovery |
| HTTP product | `Gateway`, `RouteTable`, `GatewayOpts` (`http-jobs` feature) | Gateway layer — see [facade](facade.md) |
| Optional adapters | `PgBacklog` (`external-backlog`), `PgEventOutboxSource` (`domain-outbox`), `RedisStore` (`redis-store`) | Feature-gated re-exports from facade |

## Not on the facade root

| Item | Use instead |
|------|-------------|
| Custom SM cluster assembly | **Removed from public API** — product uses [`TrembitaApp`](../../crates/trembita/src/app/mod.rs) + empty default SM + `TREMBITA_*`. Custom state machines: unpublished [`trembita-showcase`](../../crates/trembita-showcase/src/lib.rs) (+ bins `showcase-migrate-demo`, `showcase-self-update`), or in-crate [`integration`](../../crates/trembita/src/integration/mod.rs) tests via [`trembita-assembly`](../../crates/trembita-assembly/src/lib.rs) [`TrembitaClusterBuilder`](../../crates/trembita-assembly/src/builder/mod.rs). Benchmarks/soaks use [`trembita_showcase::cluster::cluster_builder`](../../crates/trembita-showcase/src/cluster.rs). |
| `TrembitaCluster::builder` / exported `TrembitaClusterBuilder` | **Removed** — not on the facade |
| `TrembitaApp::cluster` / `into_cluster` | Not public — use `TrembitaApp` product APIs or `trembita::cluster` runtime handles |
| `TrembitaAppBuilder::inner_mut` | `#[doc(hidden)]` — in-crate tests only |
| Cluster join / static members / voter replacement | `TREMBITA_*` env on [`TrembitaApp::from_env`](../../crates/trembita/src/app/runtime.rs) — not builder setters |

## Out of semver scope

| Item | Policy |
|------|--------|
| `trembita-node` binary flags / env | May add vars; documented in `docs/certs.md` |
| `pub(crate)` facade internals | Not public API |
| `#[doc(hidden)]` re-exports | Do not use |
| Sim / test crates | Unstable |
| SemVer `0.x` minors | Breaking changes allowed until 1.0; public **change history** in [CHANGELOG.md](../../CHANGELOG.md) starts at 1.0 ([library-and-publishing](library-and-publishing.md)) |

## Facade re-export audit (`crates/trembita/src/lib.rs`)

**Intentionally public:** `actor_store`, `client`, `core`, `net`, `storage`, `proto`,
`dashboard`, `macros`, `jobs`, `events`, `runtime`, cluster/app/workflow types listed above.

**Feature-gated re-exports** ([facade](facade.md)): `http-jobs` → HTTP gateway types;
`redis-store` → `store_redis`; `external-backlog` → `backlog_postgres`; `domain-outbox` → `events_postgres`.

**Not re-exported (use sub-crates deliberately):** `trembita-sim`, `trembita-ops`, `trembita-cli`.

## Maintenance checklist

- [ ] [CHANGELOG.md](../../CHANGELOG.md) — Keep a Changelog entries from **1.0.0**
- [x] `missing_docs = deny` on published crates (see [missing-docs-1.0.md](missing-docs-1.0.md)) — shipped 2026-08-29
- [ ] `./scripts/docs-missing-audit.sh --workspace` → 0 warnings
- [ ] Scenario soak harness green in scheduled CI (B-10)

## Related

- [library-and-publishing.md](library-and-publishing.md)
- [facade.md](facade.md)
- [product-scenarios.md](product-scenarios.md)
- [CHANGELOG.md](../../CHANGELOG.md)
