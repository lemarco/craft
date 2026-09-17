# Product terminology — capabilities vs runtime actors

**Status:** Accepted  
**Date:** 2026-09-17  
**Related:** [capability-greenfield-wire](capability-greenfield-wire.md), [public-api-1.0](public-api-1.0.md), [cross-node-actors](cross-node-actors.md)

## Rule for maintained docs

When writing for **product teams** (`getting-started`, scenarios, env, README, crates.io rustdoc):

| Say | Do not imply |
|-----|----------------|
| **Capabilities** — `CapManifest`, `#[cap_handler]`, `.via(&app)`, gateway `cap_*` | Implement **`UserActor`** for CRUD-style product ops |
| **Cap store** — `trembita::capstore`, `OpCtx::require_store` | “Actor workflow store” as the primary product noun (legacy name) |
| **Jobs / topics / workflows** as first-class registration | `/actors/{group}/cast` as the default HTTP product path |
| **Sessions** — sticky routing for realtime (`SessionHandle`, `open_worker_session_*`, `cap` on wire) | Raw mailbox `cast`/`ask` in app tutorials |

**Runtime actors** (`UserActor`, directory, migration, cross-node deliver) remain in **`trembita-runtime`**. The facade uses them internally (**`CapHost`**, supervisors). Product code does not register custom `UserActor` groups unless documented as **advanced** ([getting-started §5](../getting-started.md#5-product-workers)).

**Scale vocabulary:** **B-28** — auto cap **group scale** (`PerNode` vs `Fixed`) from manifest shape; **B-31** — **stateless / keyed / session / queued op** product scale map + doctor; **B-32** — **coordination scale** (sharded job queue, product multi-Raft). **B-29** — cluster **session cookie** (not in-memory token sets). **B-30** — **`/ready`** vs **`/health`** for LB pools. Index: [status § Product scale wave](../status.md#product-scale-wave-b-28b32) (B-28…B-54). Detail: [capability-dx § Product scale](capability-dx.md#product-scale-model-b-31), [capabilities § Coordination scale](../scenarios/capabilities.md#coordination-scale-b-32).

## Defaults (0.6+)

- [`TrembitaConfigure::default()`](../../crates/trembita/src/configure.rs) sets **`without_actors_api: true`** — no `/actors/*` on the default gateway.
- [`WorkerOpts::http_cast`](../../crates/trembita/src/worker_opts.rs) defaults **`false`**.
- Scaffolds call [`.without_actors_api()`](../../crates/trembita/src/app/builder.rs) on the builder (idempotent).

Re-enable legacy HTTP actors only for migration tooling: `.workers(…).http_cast(true)` + [`.with_actors_api()`](../../crates/trembita/src/app/builder.rs) (or set `without_actors_api: false` in [`.configure`](../../crates/trembita/src/app/builder.rs)).

## Where actor ADRs still apply

[cross-node-actors](cross-node-actors.md), [actor-routing](actor-routing.md), [actor-state-store](actor-state-store.md) describe **platform/runtime** behavior and advanced paths — not the greenfield product checklist.

## Doc inventory (maintained)

Product-facing paths to keep aligned: [getting-started.md](../getting-started.md), [env.md](../env.md), [status.md](../status.md), [scenarios/](../scenarios/README.md), root + [crates/trembita/README.md](../../crates/trembita/README.md), [deployment-model](deployment-model.md), [observability](observability.md), [protocol.md](../protocol.md) (wire vs product HTTP).
