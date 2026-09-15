# Introspect API as a mountable gateway router

**Status:** Accepted (implemented)  
**Date:** 2026-09-03  
**Epic:** [B-19](../backlog.md#b-19--introspect-api-gateway-router)

## Context

Cluster introspection JSON already exists ([observability §4](observability.md)):

| Route | View type | Source |
|-------|-----------|--------|
| `GET /introspect/cluster` | [`ClusterView`](../../crates/trembita-dashboard/src/views.rs) | [`Observer::cluster`](../../crates/trembita-dashboard/src/views.rs) |
| `GET /introspect/actors` | `Vec<ActorView>` | `Observer::actors` |
| `GET /introspect/actors/{id}` | `Option<ActorView>` | `Observer::actor` |
| `GET /introspect/node/{id}` | `Option<NodeView>` | `Observer::node` |
| `GET /introspect/queues` | [`QueuesView`](../../crates/trembita-dashboard/src/views.rs) | `Observer::queues` |
| `GET /introspect/sagas` | `Vec<SagaRecordView>` | `Observer::sagas` |
| `GET /introspect/topics` | [`TopicsView`](../../crates/trembita-dashboard/src/views.rs) | `Observer::topics` |
| `GET /introspect/raft-groups` | [`RaftGroupsView`](../../crates/trembita-dashboard/src/views.rs) | `Observer::raft_groups` |

[`TrembitaObserver`](../../crates/trembita/src/observer.rs) implements the port. [`TrembitaApp::ops_api()`](../../crates/trembita/src/app/runtime.rs) and [`AdminServer`](../../crates/trembita-dashboard/src/server.rs) serve the same JSON on the **ops HTTP bind** ([wire-protocol § ops HTTP](wire-protocol.md#ops-http-tcp-on-trembita_listen)) together with `/health`, `/ready`, `/metrics`, and the embedded dashboard.

Product HTTP merges optional [`RouteTable`](../../crates/trembita-http/src/routing/table.rs) entries with [`AuthFn`](../../crates/trembita-http/src/lib.rs) ([`JobsApi`](../../crates/trembita-http/src/lib.rs), [`ActorsApi`](../../crates/trembita-http/src/lib.rs), [`WorkflowsApi`](../../crates/trembita-http/src/lib.rs), [`IntrospectApi`](../../crates/trembita-http/src/lib.rs)) via [`GatewayOpts::surfaces`](../../crates/trembita/src/gateway/opts.rs) and [`build_gateway_service`](../../crates/trembita/src/gateway/mod.rs). [`TrembitaApp::from_env()`](../../crates/trembita/src/app/runtime.rs) mounts ops + registration-driven product APIs on **`TREMBITA_LISTEN`** by default.

Teams whose **operator UI is the product** (multi-page admin apps, session auth in Postgres, RBAC) mount the same snapshots **beside custom routes** on the product hostname, typically behind [`AuthMode::Identity`](../../crates/trembita-http/src/routing/auth.rs).

Job **operations** for operator screens (`list_jobs`, `requeue_dead_letter_batch`) mount with [`.jobs([…]).http_enqueue(true)`](../../crates/trembita/src/job_opts.rs) as `GET /jobs/{stream}` and `POST /jobs/{stream}/requeue-batch`. **Read-only introspection** uses the same paths on the unified bind (`GET /introspect/*`) or an explicit [`IntrospectApi`](../../crates/trembita-http/src/lib.rs) merge.

This mirrors the split already accepted for schedules and backlogs: trembita holds data and semantics; the **HTTP surface** for operator UIs stays in the app ([schedule-source](schedule-source.md), [external-backlog](external-backlog.md) — “HTTP schedule admin on trembita” rejected).

## Decision

Add a fourth mountable product router — **`IntrospectApi`** — in `trembita-http`, wired from the facade like the other three.

### 1. `IntrospectApi` (`trembita-http`)

Same shape as existing product APIs:

```rust
pub struct IntrospectApi {
    observer: Arc<dyn Observer>,
}

pub struct IntrospectApiState {
    pub(crate) observer: Arc<dyn Observer>,
    pub(crate) auth: Option<AuthFn>,
}

impl IntrospectApi {
    pub fn new(observer: Arc<dyn Observer>) -> Self { /* ... */ }
    pub fn router(&self) -> Router<Arc<IntrospectApiState>> { /* ... */ }
    pub fn into_state(self) -> IntrospectApiState { /* ... */ }
    pub fn into_state_with_auth(self, auth: Option<AuthFn>) -> IntrospectApiState { /* ... */ }
}
```

Routes (read-only GET, **same paths and JSON** as admin):

| Route | Handler |
|-------|---------|
| `GET /introspect/cluster` | `observer.cluster().await` |
| `GET /introspect/actors` | `observer.actors().await` |
| `GET /introspect/actors/{id}` | `observer.actor(&id).await` → `404` when `None` |
| `GET /introspect/node/{id}` | `observer.node(id).await` → `404` when `None` |
| `GET /introspect/queues` | `observer.queues().await` |
| `GET /introspect/sagas` | `observer.sagas().await` |
| `GET /introspect/topics` | `observer.topics().await` |
| `GET /introspect/raft-groups` | `observer.raft_groups().await` |

Each handler calls the shared `authorize()` helper when `state.auth` is set (same pattern as [`routes.rs`](../../crates/trembita-http/src/routes.rs)).

**Types:** re-export [`Observer`](../../crates/trembita-dashboard/src/views.rs) and view structs from `trembita-http` (and the `trembita` facade). `trembita-http` adds a dependency on `trembita-dashboard` for those types only — no second HTTP stack in the router.

**Error type:** `IntrospectApiError` (unauthorized, bad path param) with `IntoResponse`; map `JobsApiError::Unauthorized` from `AuthFn` like actors/workflows.

### 2. Facade wiring

```rust
// TrembitaApp — builds Arc<dyn Observer> from TrembitaObserver (same as ops_api)
pub fn introspect_api(&self) -> trembita_http::IntrospectApi { /* ... */ }

// Explicit merge (brownfield) — identity on sensitive tables
GatewayOpts::new(addr)
    .identity(MySessionIdentity)
    .surfaces(|state| {
        let introspect = state.app.introspect_api().route_table_with_auth(Some(my_auth));
        let jobs = TrembitaApp::jobs_api(Arc::clone(&state.app))
            .route_table()
            .with_auth_mode(AuthMode::Identity);
        my_admin_ui_routes(state).merge(jobs).merge(introspect)
    })
```

[`TrembitaApp::default_surfaces`](../../crates/trembita/src/app/gateway.rs) includes ops `/introspect/*` when using [`from_env()`](../../crates/trembita/src/app/runtime.rs). Removed in 0.5: `GatewayOpts::with_*_api` / `protect_product_apis` ([unified-listener](unified-listener.md)).

Manual mount (custom route table without full gateway):

```rust
let observer: Arc<dyn Observer> = app.introspect_observer(); // or TrembitaObserver handle
let api = IntrospectApi::new(observer);
let routes = api.route_table_with_auth(Some(my_auth));
```

### 3. Ops bind (unified listener)

On product apps, `/introspect/*` lives on the **same TCP bind as product HTTP** (`TREMBITA_LISTEN`) via default gateway surfaces:

- Prometheus scrape co-location (`/metrics`)
- Embedded dashboard (`GET /dashboard`)
- Ops probes (`/health`, `/ready`) — typically on an internal hostname in production
- E2E and runbook curls against the unified HTTP port

Host-split deployments can mount ops-only tables on `ops.internal` and product + introspect on `api.example.com` ([unified-listener](unified-listener.md)).

### 4. Auth model

- Same [`AuthFn`](../../crates/trembita-http/src/lib.rs) / [`GatewayIdentity`](../../crates/trembita/src/gateway/identity.rs) as other product APIs via [`RouteTable::with_auth_mode(AuthMode::Identity)`](../../crates/trembita-http/src/routing/auth.rs).
- Session cookies, JWT, RBAC — app-defined via `GatewayIdentity` or a custom `AuthFn` on manual mount ([gateway-identity](gateway-identity.md)).
- Custom routes from `GatewayOpts::routes()` remain **unprotected** by default (unchanged).

### 5. Testing

| Layer | Test |
|-------|------|
| Unit | `introspect_routes.rs` — 404 on missing actor/node, auth rejects without token |
| Integration | `trembita/tests/gateway_introspect_http.rs` — merge router, assert JSON shape matches admin |
| Facade | `AuthMode::Identity` → `401` on `/introspect/cluster` without identity |

Update [testing-coverage.md](../testing-coverage.md) when tests land.

## Consequences

**Positive**

- Operator UIs mount trembita snapshots beside their own handlers — no duplicate REST glue.
- Same JSON contract as admin/dashboard; frontend code can share fetch paths.
- Pairs naturally with `JobsApi` for full admin coverage (depth gauges + per-job list/requeue).
- Reuses existing [`Observer`](../../crates/trembita-dashboard/src/views.rs) port — no new data layer.

**Negative**

- `trembita-http` depends on `trembita-dashboard` (view types). Acceptable: facade already pulls both; alternative would be extracting views to a fifth crate — deferred unless dependency graph becomes painful.
- Host-based routing can expose identical paths on different virtual hosts — pick one **authoritative** introspection URL for your operator UI (usually the identity-protected product hostname).
- Cross-node actor aggregation semantics unchanged — still whatever `TrembitaObserver` returns on the queried node; not a new cluster-wide fan-out API.

## Out of scope

| Item | Reason |
|------|--------|
| Moving `/metrics`, `/dashboard` off the ops bind | Ops/embedded UI stay on the ops hostname by convention |
| `GET /ready`, `GET /health` on a public product hostname | K8s probes should target the ops hostname; workflows API has its own `/health` |
| Mutating introspection | Read-only by design ([observability §4](observability.md)) |
| Moving `Observer` out of `trembita-dashboard` | Follow-up only if crate split is needed |

## Alternatives considered

| Option | Verdict |
|--------|---------|
| Proxy admin port from app | Rejected — second listener, mTLS/network policy friction, bypasses gateway auth |
| Hand-written handlers in each app | Rejected — exact gap this ADR closes |
| Put introspect routes on admin only | Status quo — rejected for product-admin teams |
| Separate `trembita-observe` crate for view types | Deferred — two-crate split before proven need |
| Include `list_jobs` in IntrospectApi | Rejected — already in `JobsApi`; document pairing |

## Related

- [observability.md §4](observability.md#4-introspection-api-observer-like)
- [gateway-identity.md](gateway-identity.md)
- [product-scenarios.md](product-scenarios.md)
- [wire-protocol.md § ops HTTP](wire-protocol.md#ops-http-tcp-on-trembita_listen)
- [schedule-source.md](schedule-source.md) — operator UI stays in the app
- [external-backlog.md](external-backlog.md) — same product boundary
