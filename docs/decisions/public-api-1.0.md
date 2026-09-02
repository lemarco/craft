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
| Observability | `init_tracing`, `Metrics`, `TrembitaEvent` | Admin port separate |

## Cluster & client API (`trembita::cluster`)

| Area | Types | Notes |
|------|-------|-------|
| Cluster | `trembita::cluster::{TrembitaCluster, TrembitaClusterBuilder, StartError}` | Custom SM, integration tests; not re-exported at crate root |
| Client | `RemoteClient`, `run_saga`, `run_keyed_saga`, `KeyedClient` | Re-exported `trembita::client` |
| Multi-Raft | `propose_keyed`, `add_raft_groups`, `RaftGroupsView` | Builder flags |
| Saga / 2PC journals | `MetaRaftSagaJournal`, `CompositeSagaJournal`, `StoreTwoPhaseJournal` | Ops / recovery |
| HTTP product | `trembita-http` crate, `TrembitaApp::jobs_api` (`http-jobs` feature) | Gateway layer |

**Removed / hidden (0.4.1):** `trembita::advanced` module (renamed to `trembita::cluster`, no alias); root `use trembita::TrembitaCluster`; `TrembitaApp::cluster` / `into_cluster` / `TrembitaAppBuilder::inner_mut` (`#[doc(hidden)]`, tests only).

## Out of semver scope

| Item | Policy |
|------|--------|
| `trembita-node` binary flags / env | May add vars; documented in `docs/certs.md` |
| `pub(crate)` facade internals | Not public API |
| `#[doc(hidden)]` re-exports | Do not use |
| Sim / test crates | Unstable |
| SemVer `0.x` minors | Breaking changes allowed on minor bumps with CHANGELOG entry ([library-and-publishing](library-and-publishing.md)) |

## Facade re-export audit (`crates/trembita/src/lib.rs`)

**Intentionally public:** `actor`, `client`, `core`, `net`, `storage`, `proto`,
`dashboard`, `macros`, cluster/app/workflow/saga/two_phase types listed above.

**Not re-exported (use sub-crates deliberately):** `trembita-sim`, `trembita-ops`,
`trembita-store-redis`, `trembita-http` (separate dependency for product HTTP).

## Maintenance checklist

- [x] CHANGELOG documents breaking API surface cleanup (`advanced` → `cluster`, `TrembitaApp` delegates)
- [x] `missing_docs = deny` on published crates (see [missing-docs-1.0.md](missing-docs-1.0.md)) — shipped 2026-08-29
- [ ] `./scripts/docs-missing-audit.sh --workspace` → 0 warnings
- [ ] Scenario soak harness green in scheduled CI (B-10)

## Related

- [library-and-publishing.md](library-and-publishing.md)
- [product-scenarios.md](product-scenarios.md)
- [CHANGELOG.md](../../CHANGELOG.md)
