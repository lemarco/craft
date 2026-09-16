# Capabilities — typed ops on the cluster

**Status:** Shipped (B-21) — [capability-dx ADR](../decisions/capability-dx.md) (Accepted).

## When to use

- You want **one handler** callable **inline** (await reply) or **queued** (retries, DLQ) without duplicating logic
- Stateful work pinned by key (`OrderId`, tenant, …) without writing mailbox boilerplate
- Product code stays in **`capabilities/` + `domain/`**; not runtime worker traits

**Prefer** [`CapManifest`](../../crates/trembita/src/capability/manifest.rs) for product ops. Legacy [`WorkerOpts`](../../crates/trembita/src/worker_opts.rs) / `UserActor` in app code remains for migration demos and advanced runtime use.

## Quick sketch

```rust
// capabilities/orders.rs — wire op `process_order` from struct name `ProcessOrder`
#[cap_handler(group = "orders", key = "order_id")]
async fn process_order(msg: ProcessOrder, state: &mut OrdersState) -> Result<Ack, CapError> { … }

// manifest.rs — registration
CapManifest::new().group(cap_register_chain!(
    CapGroup::<OrdersState>::for_cap::<ProcessOrder>()
        .instances(1)
        .default_queue_for::<ProcessOrder>()
        .default_event_ingress_for::<ProcessOrder>(),
    process_order_register,
));

// call site
ProcessOrder { id }.via(&app).route(Route::Inline).await?;
ProcessOrder { id }.via(&app).enqueue().await?;
ProcessOrder { id }.via(&app).queued_wait().await?;
ProcessOrder { id }.via(&app).publish_event().await?; // Route::Event egress
```

## HTTP (gateway-first)

Declare paths in `http/product.rs` — not on the manifest:

```rust
ProductRoutes::new()
    .post_identity("/orders/submit", cap_fire::<ProcessOrder>(state))
    .post("/orders/query", cap_invoke::<ProcessOrder>(state, Route::Inline))
    .build()
```

Greenfield apps do **not** rely on `/actors/.../cast` — see [capability-greenfield-wire](../decisions/capability-greenfield-wire.md).

## Routes (semantics)

| Route | Use |
|-------|-----|
| `Inline` | Short RPC-style await |
| `InlineFire` | Cast, no reply — use `.fire()` |
| `Queued` | Backlog + at-least-once — use `.enqueue()` |
| `QueuedWait` | Enqueue + await stored reply — use `.queued_wait()` |
| `Scheduled` | Delayed enqueue — `.run_at_ms(ms).schedule()` |
| `Session` | Sticky session — `.session_key(k).route(Route::Session)` |
| `Event` | Topic — `.event_ingress(topic, sub)` + `.publish_event()`; subscriber runs same handler |

Authoritative domain data still lives in Raft SM or external DB — see [state-placement](state-placement.md).

## Related

- [capability-parity](capability-parity.md) — scenario matrix (same power, no `UserActor` in app)
- [stateful-workers](stateful-workers.md) — showcase (capabilities + migration demo)
- [background-jobs](background-jobs.md) — queue semantics for queued routes
- [framework-conventions](../decisions/framework-conventions.md) — `capabilities/` layout
