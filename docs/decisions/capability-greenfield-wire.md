# Greenfield wire — capabilities only at the edge

**Status:** Accepted  
**Date:** 2026-09-16  
**Supersedes (product policy):** ad-hoc “use `/actors/cast` for product work”  
**Related:** [capability-dx](capability-dx.md), [wire-protocol](wire-protocol.md), [gateway-routing-v2](gateway-routing-v2.md)

## Context

Trembita targets **new product apps** with no obligation to support legacy HTTP clients that
post opaque bytes to `/actors/{group}/cast`. Those apps still need:

- A **visible HTTP tree** on the gateway (custom routes + `cap_*` adapters).
- **Typed ops** in-process and across the cluster.
- **Per-op durability** (store keys, idempotency) instead of “migrate the whole actor RAM” as the
  default story.

Internal cluster messaging already uses **postcard** on the QUIC wire ([wire-protocol](wire-protocol.md)).
That is **not** a second product API — it is machine-to-machine framing between nodes of the **same**
binary. Product authors should not hand-author that layer.

## Decision

### Product ingress (greenfield)

| Surface | Wire | Rule |
|---------|------|------|
| **Gateway** | JSON (or explicit handler DTO → `CapRequest`) | All product triggers go through **`http/`** + [`cap_fire` / `cap_invoke` / `cap_enqueue`](../scenarios/capabilities.md) or custom routes that call `.via(&app)` |
| **In-process** | Rust types + [`Route`](../scenarios/capabilities.md) | `Req.via(&app).route(…)` / `.fire()` / `.enqueue()` |
| **Inter-node** | `CapWire { op, payload }` + postcard `Req` | Automatic for registered ops; authors do not define a parallel “cluster REST” |

**Do not document** raw `/actors/{group}/cast` as a product path for new apps. The route may remain
on the default gateway for tooling and migration demos until removed in a future release.

### Decode

- **Cluster:** `CapWire` → postcard → `Req` (automatic for registered ops).
- **Gateway:** JSON → `Req` (`cap_*`) or custom HTTP handler → struct → `.via(&app)`.

No per-op `.parse()` hook in the product model — custom shapes stay at the gateway layer.

### Migration (greenfield semantics)

- **Default:** each op is **independently safe across nodes** — progress in [`ActorStateStore`](actor-state-store.md), Raft SM, or external DB; handlers idempotent where retries happen.
- **Not the default product story:** snapshot whole in-memory actor state (`UserActor::MIGRATABLE`) except migration showcases and advanced runtime ([cross-node-actors](cross-node-actors.md)).

Sticky [`ActorSession`](../../crates/trembita-runtime/src/session.rs) is an **optimization** for locality, not the system of record.

### Where `UserActor` remains

| Use | Status |
|-----|--------|
| Internal `CapHost` | Always (hidden) |
| Realtime/long-lived loops, migration lab, `spawn_remote` / custom control plane | Advanced / explicit `trembita::runtime` |
| New CRUD-style product ops | **Capabilities only** |

## Consequences

- Docs, scaffold, and examples lead with **`capabilities/` + gateway**, not `actors/` + cast.
- [`getting-started.md`](../getting-started.md) should treat § Workers as advanced or replace with capabilities (follow-up).
- New scaffolds call [`.without_actors_api()`](../crates/trembita/src/app/builder.rs); re-enable `/actors/*` with [`WorkerOpts::http_cast(true)`](../crates/trembita/src/worker_opts.rs) in `manifest.rs`.

## Non-goals

- Removing postcard on the QUIC wire (see [wire-protocol](wire-protocol.md)).
- Forcing JSON between cluster nodes.
- Deleting `UserActor` from the runtime crate.

## Follow-up (implementation)

| Id | Task | Status |
|----|------|--------|
| B-22 | Greenfield wire (gateway ingress, scripts, Event route) | ✅ |
| B-23 | Showcase: capability idempotency; advanced RAM migrate-demo documented | ✅ |
