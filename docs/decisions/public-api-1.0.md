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
| App | `TrembitaApp`, `TrembitaAppBuilder`, `TrembitaConfigure`, `JobOpts`, `WorkerOpts`, `WorkerScale`, `QueueOpts`, `CronOpts`, `ActorGroupOpts`, `GatewayOpts`, `RunOpts` | Primary entry; env via `TREMBITA_*` at boot |
| App runtime | `node_id`, `control`, `registry`, `supervisor`, `enqueue`, `cast`, `ask`, `shutdown_graceful` | Product control plane on `TrembitaApp` |
| Identity | `NodeId`, `Security`, `PeerDirectory`, cert reload helpers | Multi-node wiring |
| Jobs | `JobQueue`, `EnqueueOptions`, `run_queue_consumer`, `ClusterJobQueue` | Via `TrembitaApp::enqueue` |
| Workflows | `WorkflowBuilder`, `run_workflow` / `resume_workflow` on app | Saga journal durable |
| Actor store | `RedbActorStateStore`, `ClusterActorStateStore`, `store_get` / `store_set` | Auto via `data_dir` |
| Sessions | `ActorSession`, `TrembitaApp::session_keyed` | Sticky routing |
| Observability | `init_tracing`, `Metrics`, `TrembitaEvent` | Ops routes on unified `TREMBITA_LISTEN` (or explicit gateway merge) |

## Cluster & client API (`trembita::cluster`)

| Area | Types | Notes |
|------|-------|-------|
| Cluster | `trembita::cluster::{TrembitaCluster, TrembitaClusterBuilder, StartError}` | Custom SM, integration tests; not re-exported at crate root |
| Client | `RemoteClient`, `run_saga`, `run_keyed_saga`, `KeyedClient` | Re-exported `trembita::client` |
| Multi-Raft | `propose_keyed`, `add_raft_groups`, `RaftGroupsView` | Builder flags |
| Saga / 2PC journals | `MetaRaftSagaJournal`, `CompositeSagaJournal`, `StoreTwoPhaseJournal` | Ops / recovery |
| HTTP product | `Gateway`, `RouteTable`, `GatewayOpts` (`http-jobs` feature) | Gateway layer — see [facade](facade.md) |
| Optional adapters | `PgBacklog` (`external-backlog`), `PgEventOutboxSource` (`domain-outbox`), `RedisStore` (`redis-store`) | Feature-gated re-exports from facade |

## Not on the facade root

| Item | Use instead |
|------|-------------|
| Low-level cluster handle | `trembita::cluster::{TrembitaCluster, TrembitaClusterBuilder, …}` |
| `TrembitaApp::cluster` / `into_cluster` | Not public — use `TrembitaApp` product APIs or `trembita::cluster` |
| `TrembitaAppBuilder::inner_mut` | `#[doc(hidden)]` — tests only |

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
