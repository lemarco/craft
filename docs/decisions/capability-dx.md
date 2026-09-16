# Capability DX — distributed ops without actor boilerplate

**Status:** Accepted  
**Date:** 2026-09-16  
**Epic:** [B-21](../backlog.md#b-21--capability-dx-product-api)

## Context

Product teams want **one mental model** for cluster work: typed operations, location-transparent
execution, good performance (postcard wire, single binary), without choosing up front between
“sync service” and “async job”. Today that split is visible in APIs:

- Stateful / RPC-ish paths → `UserActor`, `WorkerOpts`, manual `cast`/`ask` bytes
- Backlog paths → `#[consumer]`, separate streams and handlers
- Bridge patterns (queue → actor) are manual ([B-14k](../backlog.md))

**Capability** is the product name for a **registered operation** (`Op`) with explicit **routes**
(how to invoke), shared **group** state when needed, and **domain logic** kept in plain functions.

Runtime primitives (mailbox, queue, topic, Raft SM) stay unchanged; capability is a **facade +
registry + adapters**.

## Decision

### Terms

| Term | Meaning |
|------|---------|
| **Group** | Named pool on the cluster (`"orders"`) — routing, optional shared `State`, one internal host |
| **Op** | One operation: request struct, reply type, handler fn, metadata (routes, key, queue stream) |
| **Route** | Invocation mode for an op (inline, queued, …) — chosen at **call site**, not baked into the op definition |
| **OpCtx** | Per-invocation context: `&TrembitaApp`, actor store, deps, tracing (no `UserActor` in app code) |

App code **must not** implement runtime worker traits or hand-encode mailbox payloads for
product ops. Advanced / cluster authors may still use `trembita::runtime` directly.

### App layout (replaces `actors/` as the default path)

```
src/
├── domain/           # no trembita imports
├── capabilities/
│   └── orders/
│       ├── mod.rs    # group marker + state type
│       ├── fulfill.rs
│       └── status.rs
├── manifest.rs       # CapManifest + existing jobs/topics as needed
└── app.rs
```

Scaffold: `capabilities/` instead of `actors/`; `consumers/` remains for **legacy or
queue-only** streams until ops fully subsume them.

### Authoring — one handler per op

```rust
// capabilities/orders/fulfill.rs
use super::OrdersState;
use crate::domain;
use trembita::{cap_handler, CapError, OpCtx};

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Receipt(pub String);

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Fulfill {
    pub id: OrderId,
}

#[cap_handler(group = "orders")]
pub fn run(msg: Fulfill, state: &mut OrdersState) -> Result<Receipt, CapError> {
    // needs `ctx.app()`? use the 3-arg form `(Fulfill, OpCtx<'_>, &mut OrdersState)` instead
    todo!()
}
```

Registration (routes live on the handler; no duplicate `CapOp::new`):

```rust
CapManifest::new().group(trembita::cap_register_chain!(
    CapGroup::<OrdersState>::for_cap::<Fulfill>().instances(1),
    run_register,
))
```

(`Reply` from `Result<…>`; `op` from request struct name; `key = "field"` → `CapOp::key_cap` in `{handler}_register`. DTO-only: [`#[cap_request]`](../../crates/trembita-macros/src/lib.rs).)

Registration (manifest) — **data, no attribute DSL on the handler**:

```rust
// manifest.rs
use trembita::capability::{CapGroup, CapManifest, Route};

pub fn build() -> AppManifest {
    let caps = CapManifest::new().group(
        CapGroup::new("orders")
            .state::<OrdersState>()
            .op::<Fulfill, Receipt>(capabilities::orders::fulfill::run)
            .routes([Route::Inline, Route::Queued])
            .key(|m: &Fulfill| m.id.0.to_string())
            .queue_stream("orders"), // stream name when Route::Queued
    );

    AppManifest::new()
        .capabilities(caps)
        .jobs([/* optional: streams not tied to a cap group */])
}
```

### Calling — route at the call site

Generated (or trait-based) builder on the **request type**:

```rust
use capabilities::orders::fulfill::Fulfill;

// wait for reply on the cluster (ask_keyed when key is set)
Fulfill { id }.via(&app).route(Route::Inline).await?;

// durable backlog + retries
Fulfill { id }.via(&app).route(Route::Queued).await?; // -> EnqueueOutcome / job id

// fire-and-forget mailbox
Fulfill { id }.via(&app).route(Route::InlineFire).await?;
```

**Route is chosen at the call site** (`.route(Route::Queued)`, `fire_cap`, session APIs). Registration
does not whitelist routes unless an op uses [`.routes`](../../crates/trembita/src/capability/op.rs) for
advanced restriction. Missing queue/topic wiring fails when that mode is invoked, not at handler define time.

### Routes (product contract)

| Route | User semantics | Adapter (runtime) |
|-------|----------------|-------------------|
| `Inline` | Await reply; short work | `ask` / `ask_keyed` on group host |
| `InlineFire` | No reply; best-effort | `cast` / `cast_keyed` |
| `Queued` | At-least-once job | `enqueue` on op’s `queue_stream` |
| `QueuedWait` | Enqueue then await stored result | enqueue + result correlation (redb or SM) |
| `Scheduled` | Delay / cron payload | delayed enqueue / schedule API |
| `Session` | Sticky session id | `ActorSession` + inline deliver |
| `Event` | Same handler from subscription or publish | `.event_ingress(topic, sub)`; ingress → `CapWire` → `ask`; egress → `.publish_event()` |

**Not unified semantics:** `Inline` ≠ linearizable Raft; `Queued` = at-least-once. Docs and
`Route` docs state this explicitly.

### Internal implementation (not exported to apps)

Each **group** with `State` or inline routes gets an internal **`CapHost`** (runtime): one
mailbox worker that holds `State`, decodes `CapEnvelope { op_name, payload }`, dispatches to
registered `run` fns. App authors never see `UserActor` / `#[actor]` for this path.

Queued routes register the **same** `run` with the job consumer for the op’s stream (shared
decode + handler table).

### Public API surface (`trembita::capability`)

| Type | Role |
|------|------|
| `CapManifest`, `CapGroup`, `OpRegistration` | manifest wiring |
| `Route`, `OpCtx`, `CapError` | call + handler context |
| `Via`, `CallBuilder` | `msg.via(&app).route(...)` |
| `Deps` | app-injected domain ports (builder hook) |

**Prelude (after MVP):** export `CapManifest`, `Route`, `OpCtx`, `CapError` — not worker traits.

**Product vs advanced (shipped):** New apps use `CapManifest` + gateway `cap_*`; `WorkerOpts` /
`UserActor` remains for migration labs and custom mailboxes ([getting-started §5](../getting-started.md#5-product-workers) Advanced). Realtime and workflows use capabilities in scaffolds/showcases. `trembita::runtime` stays for cluster/advanced use
([facade](facade.md)).

### HTTP (wave 2)

**Paths stay in gateway** (`app.rs` / `http/`) so the full route tree is visible. Manifest does
not attach URLs to ops.

Register capability delivery beside other product routes:

```rust
ProductRoutes::new()
    .post_identity("/orders/submit", cap_fire::<ProcessOrder>(state))
    .post("/math/add", cap_invoke::<Add>(state, Route::Inline))
    .post("/math/add-async", cap_enqueue::<Add>(state))
    .build()
```

Helpers (`cap_fire`, `cap_invoke`, `cap_enqueue`) decode JSON ↔ `CapRequest`, optionally open a
sticky actor session when gateway identity is configured (`Req::GROUP`), and map routes to HTTP
(`Inline` → JSON reply, fire/queued → `202`).

Custom HTTP shapes (extra response fields, different DTO) remain plain handlers that call
`.via(&app)` or wrap these adapters.

Greenfield product policy: no documented reliance on `/actors/{group}/cast` or ad-hoc cast bytes —
see [capability-greenfield-wire](capability-greenfield-wire.md). The actors HTTP surface may remain
enabled for tooling until B-22 removes it from default gateway presets.

### Non-goals

- Transparent “one function, same guarantees everywhere” — routes differ by design
- Replacing Raft SM for authoritative domain data
- gRPC / tarpc — keep HTTP/postcard + in-process
- Mandatory proc-macro DSL (`cap! { ... }`) in MVP — builder registration first

## MVP acceptance (B-21 wave 1)

1. `CapManifest` applied via `AppManifest::capabilities`
2. One showcase group, two ops (`Inline` + `Queued` on one op, `Inline`-only on another)
3. `Fulfill.via(&app).route(Route::Inline|Queued)` integration test (LocalNetwork)
4. `domain/` unchanged hexagon rule
5. [stateful-workers](../../examples/stateful-workers/) migrated to capability layout (one group)
6. ADR + scenario page linked from [scenarios/README.md](../scenarios/README.md)

## Consequences

- Product docs talk about **ops and routes**, not mailboxes
- Duplicate handler logic between consumer and actor paths goes away for registered ops
- Slight new runtime type (`CapHost` + registry); mailbox semantics preserved
- Migration period: workers + capabilities can coexist in one manifest

## Alternatives considered

| Option | Verdict |
|--------|---------|
| `trait Capability` with associated `Msg` enum | Rejected — god enums, hard evolution |
| `#[capability]` attribute on struct | Rejected — replaced by [`#[cap_request]`](../../crates/trembita-macros/src/lib.rs) + manifest builder |
| All ops require `UserActor` in app | Rejected — hide runtime worker trait |
| Single route per op only | Rejected — user wants route at call site |

## Related

- [capability-greenfield-wire](capability-greenfield-wire.md) — ingress, parser default, per-op migration
- [product-scenarios](product-scenarios.md) — messaging layers unchanged under the hood
- [framework-conventions](framework-conventions.md) — update scaffold to `capabilities/`
- [state-placement](../scenarios/state-placement.md) — where group `State` vs store vs SM
- [job-queue](job-queue.md) — queued route uses existing queue contract
