# Public API freeze (1.0)

**Status:** Accepted (pre-freeze audit)  
**Date:** 2026-08-28  
**Backlog:** B-11a

## Scope

The **`crafty` facade** is the semver surface for product teams. Internal crates
(`crafty-core`, `crafty-net`, …) are published for advanced users but the 1.0
stability promise applies primarily to types re-exported from `crafty`.

## Tier 1 — product entry (stable at 1.0)

| Area | Types | Notes |
|------|-------|-------|
| App | `CraftyApp`, `CraftyAppBuilder`, `CraftyConfigure`, `QueueOpts`, `CronOpts`, `ActorGroupOpts`, `GatewayOpts`, `RunOpts` | Preferred product path; env via `CRAFTY_*` at boot |
| Cluster | `CraftyCluster`, `CraftyClusterBuilder`, `StartError` | Full control / examples |
| Identity | `NodeId`, `Security`, `PeerDirectory`, cert reload helpers | Multi-node wiring |
| Jobs (tier C) | `JobQueue`, `EnqueueOptions`, `run_queue_consumer`, `ClusterJobQueue` | Via `CraftyApp::enqueue` |
| Workflows | `WorkflowBuilder`, `run_workflow` / `resume_workflow` on app | Saga journal durable |
| Actor store | `RedbActorStateStore`, `ClusterActorStateStore`, `store_get` / `store_set` | Auto via `data_dir` |
| Sessions | `ActorSession`, `CraftyApp::session_keyed` | Sticky routing |
| Observability | `init_tracing`, `Metrics`, `CraftyEvent` | Admin port separate |

## Tier 2 — advanced (stable semantics, may move modules)

| Area | Types | Notes |
|------|-------|-------|
| Client | `RemoteClient`, `run_saga`, `run_keyed_saga`, `KeyedClient` | Re-exported `crafty::client` |
| Multi-Raft | `propose_keyed`, `add_raft_groups`, `RaftGroupsView` | Builder flags |
| Saga / 2PC journals | `MetaRaftSagaJournal`, `CompositeSagaJournal`, `StoreTwoPhaseJournal` | Ops / recovery |
| HTTP product | `crafty-http` crate, `CraftyApp::jobs_api` (`http-jobs` feature) | Gateway layer |

## Tier 3 — explicit non-guarantees until 1.0

| Item | Policy |
|------|--------|
| `crafty-node` binary flags / env | May add vars; documented in `docs/certs.md` |
| `pub(crate)` facade internals | Not public API |
| `#[doc(hidden)]` re-exports | Do not use |
| Sim / test crates | Unstable |
| Pre-1.0 minors | Breaking changes allowed on `0.x` with CHANGELOG entry |

## Facade re-export audit (`crates/crafty/src/lib.rs`)

**Intentionally public:** `actor`, `client`, `core`, `net`, `storage`, `proto`,
`dashboard`, `macros`, cluster/app/workflow/saga/two_phase types listed above.

**Not re-exported (use sub-crates deliberately):** `crafty-sim`, `crafty-ops`,
`crafty-store-redis`, `crafty-http` (separate dependency for product HTTP).

## 1.0 checklist

- [ ] CHANGELOG documents any breaking diff from this audit
- [x] `missing_docs = deny` on published crates (see [missing-docs-1.0.md](missing-docs-1.0.md)) — shipped 2026-08-29
- [ ] `./scripts/docs-missing-audit.sh --workspace` → 0 warnings
- [ ] Scenario soak harness green in scheduled CI (B-10)

## Related

- [library-and-publishing.md](library-and-publishing.md)
- [product-scenarios.md](product-scenarios.md)
- [CHANGELOG.md](../../CHANGELOG.md)
