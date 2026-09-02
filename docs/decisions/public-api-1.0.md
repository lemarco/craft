# Public API scope

**Status:** Accepted  
**Date:** 2026-08-28  
**Backlog:** B-11a

## Scope

The **`crafty` facade** is the semver surface for product teams. Internal crates
(`crafty-core`, `crafty-net`, …) are published for advanced users; semver guarantees apply primarily to types re-exported from `crafty`.

## Product surface (`CraftyApp`)

| Area | Types | Notes |
|------|-------|-------|
| App | `CraftyApp`, `CraftyAppBuilder`, `CraftyConfigure`, `JobOpts`, `WorkerOpts`, `WorkerScale`, `QueueOpts`, `CronOpts`, `ActorGroupOpts`, `GatewayOpts`, `RunOpts` | Primary entry; env via `CRAFTY_*` at boot |
| App runtime | `node_id`, `control`, `registry`, `supervisor`, `enqueue`, `cast`, `ask`, `shutdown_graceful` | Product control plane on `CraftyApp` |
| Identity | `NodeId`, `Security`, `PeerDirectory`, cert reload helpers | Multi-node wiring |
| Jobs | `JobQueue`, `EnqueueOptions`, `run_queue_consumer`, `ClusterJobQueue` | Via `CraftyApp::enqueue` |
| Workflows | `WorkflowBuilder`, `run_workflow` / `resume_workflow` on app | Saga journal durable |
| Actor store | `RedbActorStateStore`, `ClusterActorStateStore`, `store_get` / `store_set` | Auto via `data_dir` |
| Sessions | `ActorSession`, `CraftyApp::session_keyed` | Sticky routing |
| Observability | `init_tracing`, `Metrics`, `CraftyEvent` | Admin port separate |

## Cluster & client API (`crafty::cluster`)

| Area | Types | Notes |
|------|-------|-------|
| Cluster | `crafty::cluster::{CraftyCluster, CraftyClusterBuilder, StartError}` | Custom SM, integration tests; not re-exported at crate root |
| Client | `RemoteClient`, `run_saga`, `run_keyed_saga`, `KeyedClient` | Re-exported `crafty::client` |
| Multi-Raft | `propose_keyed`, `add_raft_groups`, `RaftGroupsView` | Builder flags |
| Saga / 2PC journals | `MetaRaftSagaJournal`, `CompositeSagaJournal`, `StoreTwoPhaseJournal` | Ops / recovery |
| HTTP product | `crafty-http` crate, `CraftyApp::jobs_api` (`http-jobs` feature) | Gateway layer |

**Removed / hidden (0.4.1):** `crafty::advanced` module (renamed to `crafty::cluster`, no alias); root `use crafty::CraftyCluster`; `CraftyApp::cluster` / `into_cluster` / `CraftyAppBuilder::inner_mut` (`#[doc(hidden)]`, tests only).

## Out of semver scope

| Item | Policy |
|------|--------|
| `crafty-node` binary flags / env | May add vars; documented in `docs/certs.md` |
| `pub(crate)` facade internals | Not public API |
| `#[doc(hidden)]` re-exports | Do not use |
| Sim / test crates | Unstable |
| SemVer `0.x` minors | Breaking changes allowed on minor bumps with CHANGELOG entry ([library-and-publishing](library-and-publishing.md)) |

## Facade re-export audit (`crates/crafty/src/lib.rs`)

**Intentionally public:** `actor`, `client`, `core`, `net`, `storage`, `proto`,
`dashboard`, `macros`, cluster/app/workflow/saga/two_phase types listed above.

**Not re-exported (use sub-crates deliberately):** `crafty-sim`, `crafty-ops`,
`crafty-store-redis`, `crafty-http` (separate dependency for product HTTP).

## Maintenance checklist

- [x] CHANGELOG documents breaking API surface cleanup (`advanced` → `cluster`, `CraftyApp` delegates)
- [x] `missing_docs = deny` on published crates (see [missing-docs-1.0.md](missing-docs-1.0.md)) — shipped 2026-08-29
- [ ] `./scripts/docs-missing-audit.sh --workspace` → 0 warnings
- [ ] Scenario soak harness green in scheduled CI (B-10)

## Related

- [library-and-publishing.md](library-and-publishing.md)
- [product-scenarios.md](product-scenarios.md)
- [CHANGELOG.md](../../CHANGELOG.md)
