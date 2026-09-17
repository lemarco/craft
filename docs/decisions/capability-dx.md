# Capability DX — distributed ops without actor boilerplate

**Status:** Accepted  
**Date:** 2026-09-16  
**Backlog:** B-21 … B-27, **B-28**, **B-31**, **B-32**, **B-33** (shipped in this ADR). **B-29** / **B-30** — [gateway-cluster-auth](gateway-cluster-auth.md), [ops/ingress-lb.md](../ops/ingress-lb.md). Wave index: [status § B-28–B-32](../status.md#product-scale-wave-b-28b32).

## Context

Product teams want **one mental model** for cluster work: typed operations, location-transparent
execution, good performance (postcard wire, single binary), without choosing up front between
“sync service” and “async job”. **Shipped (0.6):** that model is **capabilities** — routes at call site, shared groups, gateway `cap_*`. Legacy split (app `UserActor`, manual cast bytes, queue→actor bridges) remains **advanced** only ([capability-parity](../scenarios/capability-parity.md), [product-terminology](product-terminology.md)).

**Capability** is the product name for a **registered operation** (`Op`) with explicit **routes**
(how to invoke), shared **group** state when needed, and **domain logic** kept in plain functions.

Runtime primitives (mailbox, queue, topic, Raft SM) stay unchanged; capability is a **facade +
registry + adapters**.

## Decision

### Terms

| Term | Meaning |
|------|---------|
| **Group** | Named pool on the cluster (`"orders"`) — routing, optional shared `State`, one internal host |
| **Op** | One operation: request struct, reply type, handler fn, metadata (optional key, queue stream on group) |
| **Route** | Invocation mode for an op (inline, queued, …) — chosen at **call site**, not baked into the op definition |
| **OpCtx** | [`app`](../../crates/trembita/src/capability/ctx.rs) / [`require_app`](../../crates/trembita/src/capability/consensus.rs), [`store`](../../crates/trembita/src/capability/ctx.rs) / [`require_store`](../../crates/trembita/src/capability/ctx.rs), [`deps`](../../crates/trembita/src/capability/ctx.rs), [`ingress`](../../crates/trembita/src/capability/ctx.rs); linearizable Raft + sagas: [`consensus`](../../crates/trembita/src/capability/consensus.rs) (`query_keyed_linearizable`, `propose_keyed`, …) — [structural-limits § read paths](../scenarios/structural-limits.md#read-paths--not-interchangeable) |

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
pub async fn run(msg: Fulfill, state: &mut OrdersState) -> Result<Receipt, CapError> {
    // needs `ctx.app()`? use `(Fulfill, OpCtx<'_>, &mut OrdersState)` instead
    todo!()
}
```

Registration (`{handler}_register` from the attribute; wire `OP` is never duplicated):

```rust
CapManifest::new().group(trembita::cap_register_chain!(
    CapGroup::<OrdersState>::for_cap::<Fulfill>()
        .default_queue_for::<Fulfill>(), // `{group}.{op}` on macro-generated `CapRequest`
    run_register,
))
```

Product handlers are **`async fn`** (registers via [`CapOp::for_request_async`](../../crates/trembita/src/capability/op.rs)). `Reply` from `Result<…>`;
`OP` = snake_case of the request struct (`ProcessOrder` → `process_order`); `key = "field"` → `CapOp::key_cap`.
DTO-only requests: [`#[cap_request]`](../../crates/trembita-macros/src/lib.rs).

Use `(Req, OpCtx<'_>, &mut State)` when the handler needs `ctx.app()`. Two-parameter `(Req, &mut State)` is fine — the macro adds `OpCtx` at the registration boundary.
Sync `fn` is **not** supported on [`#[cap_handler]`](../../crates/trembita-macros/src/lib.rs) — legacy/tests only via manual [`CapOp::for_request`](../../crates/trembita/src/capability/op.rs).

**Advanced:** manual [`CapOp::new`](../../crates/trembita/src/capability/op.rs) / `.op::<Req, Reply>(fn)` on [`CapGroup`](../../crates/trembita/src/capability/group.rs) remains for integration tests and custom wiring — not the product authoring path.

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

### Group scale (B-28)

Host placement is chosen when the manifest is applied ([`resolved_scale`](../../crates/trembita/src/capability/group.rs)):

| Situation | Default |
|-----------|---------|
| Marker-only `State` (zero-sized), no `Route::Session` on any op | **`PerNode`** — stateless inline/queued ops scale with cluster nodes |
| Non–zero-sized `State` (shared RAM in the host) | **`Fixed(1)`** — one authoritative host unless you `.per_node()` or `.instances(n)` |
| Any op registers `Route::Session` | **`Fixed(1)`** — use `.per_node()` for realtime pools (see [realtime showcase](../../examples/realtime/)) |

Explicit overrides: [`.instances(n)`](../../crates/trembita/src/capability/group.rs) (fixed pool), [`.per_node()`](../../crates/trembita/src/capability/group.rs). Queued work still flows through job consumers; handler hosts follow the group scale above.

**CI regression:** [capabilities § Automated regression (B-28)](../scenarios/capabilities.md#automated-regression-b-28).

**Multi-node cap hosts (B-33):** when `resolved_scale` is `Fixed(n>1)` or `PerNode`, the runtime registers a **local spawn config** per group name and uses **remote `CapHost` spawn** on peer nodes so directory pools are not stuck on the boot node. Regression: [`product_coordination_scale`](../../crates/trembita/tests/product_coordination_scale.rs), [`cap_scale`](../../crates/trembita/src/integration/cap_scale.rs) unit tests.

**Boot scale report (B-33):** after `wait_until_ready`, the product logs a structured **`product_scale`** line (capability groups + `resolved_scale`, queue shard mode, coordination Raft groups) and exposes the same JSON at **`GET /introspect/product-scale`** on the unified ops listener. See [production-runbook § Product scale introspection](../ops/production-runbook.md#product-scale-introspection-b-33).

### Product scale model (B-31)

Transparent split for **«add VPS + same binary»** — what actually scales when the cluster grows:

| Op shape | What scales with more nodes | Author rule |
|----------|----------------------------|--------------|
| **Stateless** inline / fire (`Route::Inline`, `InlineFire`) on marker-only `State` | **Capability hosts** — default [`PerNode`](../../crates/trembita/src/capability/group.rs) | Do not `.instances(1)` unless you mean a single global owner |
| **Keyed** inline / fire (`#[cap_handler(key = "field")]`) | **Shard / single owner per key** — directory routes to the host for that key | Use for per-entity RAM or single-writer semantics |
| **Session** (`Route::Session`) | **Sticky** session → one cap host for the session lifetime; LB may stick WebSocket | Realtime pools: `.per_node()` + session routes ([realtime-sessions](../scenarios/realtime-sessions.md)) |
| **Queued** (`Route::Queued`, `default_queue_for`) | **Job consumers** scale with cluster / autoscale; **handler runs on cap hosts** per group scale | Queued + `.instances(1)` without keyed ops pins all handler work on one node — `trembita doctor` **errors** |

**Two layers for queued work:** the **job stream** (backlog depth, consumer count) is independent from **cap host placement** (where `run` executes). Misconfiguring `.instances(1)` on a stateless queued group looks like “queue scales” but handlers do not.

Tooling: [`trembita doctor`](../../crates/trembita-cli/src/scaffold/doctor.rs) flags Fixed(1)+queued without keys, stateless `.instances(1)`, and session routes without `.per_node()`. See [capabilities § Product scale](../scenarios/capabilities.md#product-scale-b-31).

**CI regression:** [capabilities § Automated regression (B-31)](../scenarios/capabilities.md#automated-regression-b-31).

### Coordination scale (B-32)

When **enqueue throughput** or **keyed coordination** (cap store, topics, multi-group Raft) outgrow a single Meta-Raft group, use assembly-scale features through the **product** surface — not only `TrembitaClusterBuilder`:

| Need | Product API | Env (env-only queue boot) |
|------|-------------|---------------------------|
| Fixed queue shards | [`QueueOpts::sharded`](../../crates/trembita/src/queue_opts.rs) / [`JobOpts::sharded`](../../crates/trembita/src/job_opts.rs) | `TREMBITA_JOB_QUEUE` + `TREMBITA_JOB_QUEUE_SHARDS` |
| Adaptive queue shards | [`.auto_shard()`](../../crates/trembita/src/queue_opts.rs) | `TREMBITA_JOB_QUEUE_AUTO_SHARD=1` |
| Multi-Raft on coordination SM | [`TrembitaConfigure::with_coordination_raft_groups`](../../crates/trembita/src/configure.rs), optional [`.with_coordination_shard_count`](../../crates/trembita/src/configure.rs) | `TREMBITA_RAFT_GROUPS`, `TREMBITA_RAFT_SHARD_COUNT` |
| Expand catalog at runtime | [`TrembitaApp::add_raft_groups`](../../crates/trembita/src/app/runtime.rs) | — |

This path uses **`EmptyStateMachine`** (default `TrembitaApp`) — not custom application state machines. See [multi-raft](multi-raft.md), [job-queue § sharded](job-queue.md).

**CI regression:** [`product_coordination_scale.rs`](../../crates/trembita/tests/product_coordination_scale.rs), env parse tables in [`env_config.rs`](../../crates/trembita-assembly/src/env_config.rs), unit tables in [`configure.rs`](../../crates/trembita/src/configure.rs) / [`queue_opts.rs`](../../crates/trembita/src/queue_opts.rs) / [`job_opts.rs`](../../crates/trembita/src/job_opts.rs). Scenario index: [capabilities § B-32](../scenarios/capabilities.md#automated-regression-b-32).

### Coordination growth presets (B-37)

Presets bundle [B-32](#coordination-scale-b-32) defaults so teams pick a **named growth path** instead of tuning Raft groups and queue auto-shard separately. Apply on [`TrembitaConfigure`](../../crates/trembita/src/configure.rs) **before** [`.manifest()`](../../crates/trembita/src/app/builder.rs); the builder stores the preset for standard manifest queues ([`builder.rs`](../../crates/trembita/src/app/builder.rs)).

| Profile | API | Env |
|---------|-----|-----|
| Bundled B-32 defaults | [`TrembitaConfigure::with_coordination_growth_preset`](../../crates/trembita/src/configure.rs) | `TREMBITA_COORDINATION_PROFILE` |
| Per-stream tuning | [`.with_coordination_growth_preset`](../../crates/trembita/src/queue_opts.rs) on [`QueueOpts`](../../crates/trembita/src/queue_opts.rs) / [`JobOpts`](../../crates/trembita/src/job_opts.rs) | — |
| Resolved spec (tests + env assembly) | [`CoordinationGrowthPreset::spec`](../../crates/trembita-assembly/src/coordination_profile.rs) | parsed in [`env_config.rs`](../../crates/trembita-assembly/src/env_config.rs) |

Leader auto-shard **depth / ceiling** live in [`AutoShardPolicy::jobs_backlog_growth`](../../crates/trembita-jobs/src/queue_auto_shard.rs) and [`full_growth`](../../crates/trembita-jobs/src/queue_auto_shard.rs). Explicit `TREMBITA_RAFT_*` and `TREMBITA_JOB_QUEUE_AUTO_SHARD` override the profile.

**When to enable:** [getting-started § B-37](../getting-started.md#when-to-enable-coordination-growth-b-37). **Ops check:** [production-runbook § B-37](../ops/production-runbook.md#coordination-growth-preset-b-37).

#### Automated regression (B-37)

Scenario index + exact test names: [capabilities § B-37](../scenarios/capabilities.md#automated-regression-b-37).

```bash
./scripts/test-fast.sh -p trembita-assembly --lib b37_
./scripts/test-fast.sh -p trembita-jobs --lib b37_
./scripts/test-fast.sh -p trembita --lib b37_
./scripts/test-fast.sh -p trembita --test coordination_growth_preset b37_
```

### Scale & scaffold DX (B-38)

Builds on [Product scale model (B-31)](#product-scale-model-b-31): app authors get **narrative scale help**, **actionable lint**, and **profile scaffolds** without learning every `--template` id upfront.

| Tool | Purpose |
|------|---------|
| `trembita doctor --explain-scale` | [`run_explain_scale`](../../crates/trembita-cli/src/scaffold/doctor.rs) — product scale copy + capability foot-guns; **does not** fail on missing scaffold paths |
| `trembita doctor` | Full layout + manifest lint; B-38 adds optional **`suggestion`** on findings (scale + [`check_capability_store`](../../crates/trembita-cli/src/scaffold/doctor.rs)) |
| `trembita new --profile …` | [`AppTemplate::parse_profile`](../../crates/trembita-cli/src/scaffold/template.rs) — alias for `--template` |
| Jobs **`task.rs.tpl`** | Queued handler pattern: `default_queue_for`, `require_store`, marker get/set ([structural-limits § R4](../scenarios/structural-limits.md#r4--handler-ram-without-cap-store)) |

**Profile map:** `jobs` / `realtime` / `api` / `workflows` / `topics` — see [capabilities § B-38](../scenarios/capabilities.md#scale-scaffold-dx-b-38). **`api`** omits the jobs feature and sample consumer; **`realtime`** scaffolds `.per_node()` session caps.

**Ops:** run `doctor --explain-scale` when tuning scale; `doctor --preflight` before deploy ([production-runbook § B-38](../ops/production-runbook.md#scale-scaffold-dx-b-38)).

#### Automated regression (B-38)

Full scenario index: [capabilities § Automated regression (B-38)](../scenarios/capabilities.md#automated-regression-b-38).

```bash
./scripts/test-fast.sh -p trembita-cli --lib b38_
./scripts/test-fast.sh -p trembita-cli --test scaffold b38_
./scripts/test-fast.sh -p trembita-cli --test cap_scale_doctor b38_
```

### Local 3-node cluster (B-39)

Teams validate **multi-node gateway + shared session secret** locally before VPS deploy. Three **release-built showcase** processes join via seed **`1@127.0.0.1:<base>`**; [`up_with_shared_gateway_env`](../../crates/trembita-cli/src/dev/cluster.rs) injects the same [`LOCAL_CLUSTER_GATEWAY_SESSION_SECRET`](../../crates/trembita-cli/src/dev/local_cluster.rs) on each node.

| Tool | Purpose |
|------|---------|
| [`scripts/local-cluster.sh`](../../scripts/local-cluster.sh) | Bash phases: setup, up, session-smoke, lb-up/down, stop |
| `trembita dev cluster-up [--setup] [--lb]` | Debug CLI wrapper ([`dev_cluster_up`](../../crates/trembita-cli/src/dev/mod.rs)) — **not** in release `trembita-cli` install |
| `trembita dev cluster-lb-up` | Renders [`nginx.conf.template`](../../dev/local-3node/nginx.conf.template) → `nginx.generated.conf`, `docker compose up` |

Default **`realtime`** — cookie login across nodes ([B-29](../decisions/gateway-cluster-auth.md) / [B-40](../decisions/gateway-cluster-auth.md#b-40--logic--storage-split)). Optional nginx on **`:18290`** mirrors ingress **`GET /ready`** checks ([B-30](../ops/ingress-lb.md), [B-35](../decisions/cluster-elasticity.md#join-readiness-pipeline-b-35)).

| vs | B-39 (local 3-node) | B-42 (local elastic) | B-34 (elastic E2E) |
|----|------------------------|----------------------|---------------------|
| Nodes | 3 showcases on localhost | Staged **4th joiner** (`--nodes 4`) | 4th joiner + dedicated E2E binary |
| Packaging | `local-cluster.sh` / `dev cluster-*` | `elastic-up`, `elastic-smoke`, 4-upstream nginx | `e2e/elastic_lb.sh`, Docker elastic compose |
| Primary proof | Session smoke + optional nginx | Same as B-34 on laptop: `/ready` spread, session, **`/e2e/whoami`** | PerNode cap + LB round-robin under CI |

**CI:** [capabilities § B-39](../scenarios/capabilities.md#local-3-node-cluster-b-39) · [local-3node README](../../dev/local-3node/README.md).

#### Automated regression (B-39)

```bash
./scripts/test-fast.sh -p trembita-cli --lib b39_
./scripts/test-fast.sh -p trembita-cli --test dev b39_
```

### Local elastic parity (B-42)

[`cluster-up --nodes 4`](../../crates/trembita-cli/src/main.rs) runs seed nodes **1–3**, waits on **`GET /ready`**, then starts the **4th joiner** ([`up_with_shared_gateway_env`](../../crates/trembita-cli/src/dev/cluster.rs)). Optional nginx includes a **4th upstream** when node **4** responds on `/ready`. Default **`realtime`** showcase registers shared [`e2e_pool` / `/e2e/whoami`](../../crates/trembita-tools/src/e2e_elastic/) for PerNode cap smoke ([`cap-smoke`](../../scripts/local-cluster.sh)).

```bash
./scripts/local-cluster.sh setup
./scripts/local-cluster.sh elastic-up
./scripts/local-cluster.sh lb-up
./scripts/local-cluster.sh elastic-smoke
```

Regression:

```bash
./scripts/test-fast.sh -p trembita-cli --lib b39_
./scripts/test-fast.sh -p trembita-cli --lib b42_
./scripts/test-fast.sh -p trembita-cli --test dev b42_
./scripts/test-fast.sh -p trembita-tools --lib b42_
./scripts/test-fast.sh -p trembita-tools --lib b34_
```

Full **`b42_*`** inventory (spawn waves, smoke thresholds, nginx/LB resolve, join seed, scripts): [capabilities § B-42 regression](../scenarios/capabilities.md#automated-regression-b-42).

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

### Queued enqueue dedup + bridge idempotency (shipped)

| Layer | API |
|-------|-----|
| Enqueue | [`CallBuilder::dedup_key`](../../crates/trembita/src/capability/call.rs), default from [`CapRequest::cap_key`](../../crates/trembita/src/capability/call.rs) (`#[cap_handler(key = "field")]`), HTTP `?dedup=` on [`cap_enqueue`](../../crates/trembita/src/gateway/cap_handlers.rs) |
| Bridge | [`capability/queue`](../../crates/trembita/src/capability/queue.rs) wraps delivery with [`IdempotencyOpts::by_dedup_key`](../../crates/trembita/src/consumer.rs) when cluster [`actor_state_store`](../../crates/trembita/src/app/runtime.rs) is configured |
| Handler | Application markers — see [idempotency-contract](idempotency-contract.md) |

Integration: [`capability_enqueue_dedup_key_collapses`](../../crates/trembita/tests/capability.rs).

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
| `Route`, `OpCtx`, `CapError` | call + handler context (`OpCtx::cap_store` → [`capstore::CapStore`](../../crates/trembita/src/capstore.rs)) |
| `Via`, `CallBuilder` | `msg.via(&app).route(...)` |
| `CapDeps` | [`.cap_deps`](../../crates/trembita/src/app/builder.rs) on builder → [`OpCtx::deps`](../../crates/trembita/src/capability/ctx.rs) |
| `CapIngress` | HTTP / in-process [`CallBuilder::ingress`](../../crates/trembita/src/capability/call.rs) → [`OpCtx::ingress`](../../crates/trembita/src/capability/ctx.rs) |

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
see [capability-greenfield-wire](capability-greenfield-wire.md). Default product gateway **excludes** `/actors/*`; re-enable with [`.http_cast(true)`](../../crates/trembita/src/worker_opts.rs) / [`.with_actors_api()`](../../crates/trembita/src/app/builder.rs) for advanced labs only.

### Non-goals

- Transparent “one function, same guarantees everywhere” — routes differ by design
- Replacing Raft SM for authoritative domain data
- gRPC / tarpc — keep HTTP/postcard + in-process
- Mandatory proc-macro DSL (`cap! { ... }`) — rejected; [`#[cap_handler]`](../../crates/trembita-macros/src/lib.rs) + manifest chain is the product path

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
| `cap_group!` / DSL macros for manifest wiring | Rejected — normal `CapGroup::…` method chain + [`cap_register_chain!`](../../crates/trembita/src/lib.rs) only |

## Related

- [capability-greenfield-wire](capability-greenfield-wire.md) — ingress, parser default, per-op migration
- [product-scenarios](product-scenarios.md) — messaging layers unchanged under the hood
- [framework-conventions](framework-conventions.md) — update scaffold to `capabilities/`
- [state-placement](../scenarios/state-placement.md) — where group `State` vs store vs SM
- [job-queue](job-queue.md) — queued route uses existing queue contract
