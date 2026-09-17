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
        .default_queue_for::<ProcessOrder>()
        .default_event_ingress_for::<ProcessOrder>(),
    process_order_register,
));

// call site
ProcessOrder { id }.via(&app).route(Route::Inline).await?;
ProcessOrder { id }.via(&app).enqueue().await?; // dedup from `cap_key()` when `key = "…"` on handler
ProcessOrder { id }.via(&app).dedup_key("client-token").enqueue().await?;
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

Authoritative domain data still lives in Raft SM or external DB — see [state-placement](state-placement.md). Platform limits (R1–R4, query vs ask, sagas): [structural-limits](structural-limits.md).

## Group scale (B-28)

Omit [`.instances(n)`](../../crates/trembita/src/capability/group.rs) unless you need an explicit fixed pool:

| Your group | Default hosts |
|------------|----------------|
| Marker-only `State` (zero-sized), inline/queued ops | **One per live node** — add VPS → more cap handlers without manifest changes |
| Non–zero-sized `State` (shared RAM in the host) | **One cluster-wide** — use `.instances(n)` or `.per_node()` to override |
| Any op with `Route::Session` | **One cluster-wide** — realtime apps usually add `.per_node()` ([realtime](../../examples/realtime/)) |

ADR: [capability-dx § Group scale (B-28)](../decisions/capability-dx.md#group-scale-b-28).

### Automated regression (B-28)

| Area | Location |
|------|----------|
| `resolved_scale` defaults (marker / RAM / session) | `trembita/src/capability/group.rs` (`#[cfg(test)]`, overlaps B-31 runtime defaults table) |
| 3-node PerNode pool + shared-RAM single host + session Fixed(1) | `trembita/src/integration/cap_scale.rs` |

```bash
./scripts/test-fast.sh -p trembita --lib founder_scale_b31_runtime_defaults_table
./scripts/test-fast.sh -p trembita --lib auto_scale_marker_state_spawns_one_host_per_node
./scripts/test-fast.sh -p trembita --lib auto_scale_shared_ram_state_spawns_single_cluster_host
./scripts/test-fast.sh -p trembita --lib auto_scale_session_route_defaults_to_single_host
```

## Founder scale (B-31)

When you add VPS nodes with the same binary, scale follows **how the op is invoked**, not a single “autoscale” knob:

| You call | Cluster behavior |
|----------|------------------|
| `Route::Inline` / `InlineFire` on a marker-only group | More nodes → more cap hosts (default **PerNode**) |
| `#[cap_handler(key = "…")]` | One logical owner per key (shard / sticky compute) |
| `Route::Session` | Sticky to session host; pools usually **`.per_node()`** |
| `Route::Queued` | Consumers drain the stream cluster-wide; **handlers** follow group scale |

**Foot-gun:** `.instances(1)` + queued wiring without keyed handlers — queue depth grows on all nodes but only one host runs handlers. **`trembita doctor`** reports an `[error]`; remove `.instances(1)` or add keys / shared-state intent.

Full table: [capability-dx § Founder scale model](../decisions/capability-dx.md#founder-scale-model-b-31).

### Automated regression (B-31)

| Area | Location |
|------|----------|
| Doctor foot-guns (table + scaffold) | `trembita-cli/src/scaffold/doctor.rs` (`founder_scale_b31_*`) |
| Doctor on real scaffold tree | `trembita-cli/tests/cap_scale_doctor.rs` |
| Runtime scale defaults vs founder map | `trembita/src/capability/group.rs` (`founder_scale_b31_runtime_defaults_table`) |
| 3-node host placement | `trembita/src/integration/cap_scale.rs` (B-28) |

```bash
./scripts/test-fast.sh -p trembita-cli --lib founder_scale_b31
./scripts/test-fast.sh -p trembita-cli --test cap_scale_doctor
./scripts/test-fast.sh -p trembita --lib founder_scale_b31
```

## Coordination scale (B-32)

When job enqueue or keyed coordination (cap store, topics, multi-group Raft) outgrow one Meta-Raft group, wire scale through the **product** surface — [`QueueOpts` / `JobOpts`](../../crates/trembita/src/queue_opts.rs), [`TrembitaConfigure`](../../crates/trembita/src/configure.rs), and `TREMBITA_JOB_QUEUE_*` / `TREMBITA_RAFT_*` env — not only assembly `TrembitaClusterBuilder`.

| Need | Product | Env |
|------|---------|-----|
| Fixed queue shards | `.sharded(n)` on queue / job opts | `TREMBITA_JOB_QUEUE` + `TREMBITA_JOB_QUEUE_SHARDS` |
| Adaptive queue shards | `.auto_shard()` | `TREMBITA_JOB_QUEUE_AUTO_SHARD=1` |
| Multi-Raft coordination | `.with_coordination_raft_groups(n)` | `TREMBITA_RAFT_GROUPS`, optional `TREMBITA_RAFT_SHARD_COUNT` |
| Expand catalog live | [`TrembitaApp::add_raft_groups`](../../crates/trembita/src/app/runtime.rs) | — |

Full map: [capability-dx § Coordination scale](../decisions/capability-dx.md#coordination-scale-b-32).

### Automated regression (B-32)

| Area | Location |
|------|----------|
| Configure + queue/job scale tables | `trembita/src/{configure,queue_opts,job_opts}.rs` (`b32_*_scenarios_table`) |
| Env parse (shards vs auto-shard, Raft groups) | `trembita-assembly/src/env_config.rs` (`b32_*_scenarios_table`) |
| Product boot (sharded / auto-shard / multi-Raft / `from_config`) | `trembita/tests/product_coordination_scale.rs` |

```bash
./scripts/test-fast.sh -p trembita --lib b32_
./scripts/test-fast.sh -p trembita --test product_coordination_scale --all-features
./scripts/test-fast.sh -p trembita-assembly --lib b32_
```

## Queued idempotency

At-least-once still applies; use **three layers** together ([idempotency-contract](../decisions/idempotency-contract.md)):

1. **Enqueue** — `CallBuilder::dedup_key`, or `CapRequest::cap_key()` from `#[cap_handler(key = "field")]`, or HTTP `?dedup=` on [`cap_enqueue`](../../crates/trembita/src/gateway/cap_handlers.rs).
2. **Bridge** — when `TREMBITA_DATA_DIR` enables [`TrembitaApp::actor_state_store`](../../crates/trembita/src/app/runtime.rs) (cap store), the cap queue consumer runs [`IdempotencyOpts::by_dedup_key`](../../crates/trembita/src/consumer.rs) around delivery (`cap:{stream}:` prefix).
3. **Handler** — domain markers in store for partial failure before ack (see [background-jobs](background-jobs.md#effectively-once-recipe)).

## Related

- [capability-parity](capability-parity.md) — scenario matrix (same power, no `UserActor` in app)
- [stateful-workers](stateful-workers.md) — showcase (capabilities + migration demo)
- [background-jobs](background-jobs.md) — queue semantics for queued routes
- [framework-conventions](../decisions/framework-conventions.md) — `capabilities/` layout
