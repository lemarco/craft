# Introspect API — mountable `RouteTable` on the gateway

**Status:** Accepted (implemented)  
**Date:** 2026-09-03  
**Backlog:** B-19 (shipped)

## Context

Cluster introspection JSON is defined in [observability §4](observability.md):

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

[`TrembitaObserver`](../../crates/trembita-assembly/src/observer.rs) implements the port. On the **unified HTTP bind** ([wire-protocol § ops HTTP](wire-protocol.md#ops-http-tcp-on-trembita_listen), typically **`TREMBITA_LISTEN`**), [`TrembitaApp::default_surfaces`](../../crates/trembita/src/app/gateway.rs) serves the same JSON alongside `/health`, `/ready`, `/metrics`, and the embedded dashboard. [`AdminServer`](../../crates/trembita-dashboard/src/server.rs) is for tests and tooling — not a separate production listener.

Product HTTP merges optional [`RouteTable`](../../crates/trembita-http/src/routing/table.rs) entries ([`JobsApi`](../../crates/trembita-http/src/lib.rs), [`WorkflowsApi`](../../crates/trembita-http/src/lib.rs), [`IntrospectApi`](../../crates/trembita-http/src/lib.rs), …) via [`GatewayOpts::surfaces`](../../crates/trembita/src/gateway/opts.rs). **[`ActorsApi`](../../crates/trembita-http/src/lib.rs)** (`/actors/*`) is **advanced** — default product boot omits it ([`without_actors_api`](../../crates/trembita/src/configure.rs), [product-terminology](product-terminology.md)). [`TrembitaApp::from_env()`](../../crates/trembita/src/app/runtime.rs) mounts default ops + registration-driven product APIs on **`TREMBITA_LISTEN`**.

Teams whose **operator UI is the product** mount the same snapshots beside custom routes on the product hostname, typically behind [`AuthMode::Identity`](../../crates/trembita-http/src/routing/auth.rs).

Job **operations** for operator screens (`list_jobs`, `requeue_dead_letter_batch`) mount with [`.jobs([…]).http_enqueue(true)`](../../crates/trembita/src/job_opts.rs) as `GET /jobs/{stream}` and `POST /jobs/{stream}/requeue-batch`. **Read-only introspection** uses `GET /introspect/*` on the unified bind or an explicit [`IntrospectApi`](../../crates/trembita-http/src/lib.rs) merge.

This mirrors the split already accepted for schedules and backlogs: trembita holds data and semantics; the **HTTP surface** for operator UIs stays in the app ([schedule-source](schedule-source.md), [external-backlog](external-backlog.md)).

## Decision

**`IntrospectApi`** in `trembita-http` exposes introspection as a [`RouteTable`](../../crates/trembita-http/src/routing/table.rs), wired from the facade like the other product APIs.

### 1. `IntrospectApi` (`trembita-http`)

```rust
pub struct IntrospectApi {
    observer: Arc<dyn Observer>,
}

pub struct IntrospectApiState {
    pub(crate) observer: Arc<dyn Observer>,
}

impl IntrospectApi {
    pub fn new(observer: Arc<dyn Observer>) -> Self { /* ... */ }
    pub fn route_table(&self) -> RouteTable { /* ... */ }
    pub fn route_table_with_auth(&self, auth: Option<AuthFn>) -> RouteTable { /* ... */ }
    pub fn into_state(self) -> IntrospectApiState { /* ... */ }
}
```

When `route_table_with_auth` is given `Some(auth)`, the table uses [`AuthMode::Identity`](../../crates/trembita-http/src/routing/auth.rs); the hook is wired on the gateway via [`auth_fn_to_identity`](../../crates/trembita-http/src/lib.rs), not stored in handler state.

Routes (read-only GET, same paths and JSON as ops `/introspect/*`):

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

**Types:** re-export [`Observer`](../../crates/trembita-dashboard/src/views.rs) and view structs from `trembita-http` (and the `trembita` facade). `trembita-http` depends on `trembita-dashboard` for those types only.

**Errors:** `IntrospectApiError` (unauthorized, bad path param) with HTTP mapping; same auth rejection shape as other product APIs.

### 2. Facade wiring

```rust
pub fn introspect_api(&self) -> trembita_http::IntrospectApi { /* TrembitaObserver */ }

GatewayOpts::from_env()?
    .identity(MySessionIdentity)
    .surfaces(|state| {
        let introspect = state.app.introspect_api().route_table_with_auth(Some(my_auth));
        let jobs = TrembitaApp::jobs_api(Arc::clone(&state.app))
            .route_table()
            .with_auth_mode(AuthMode::Identity);
        my_admin_ui_routes(state).merge(jobs).merge(introspect)
    })
```

[`TrembitaApp::default_surfaces`](../../crates/trembita/src/app/gateway.rs) includes ops `/introspect/*` when using [`from_env()`](../../crates/trembita/src/app/runtime.rs). Brownfield apps merge explicit tables ([unified-listener](unified-listener.md)).

Manual mount:

```rust
let api = IntrospectApi::new(observer);
let routes = api.route_table_with_auth(Some(my_auth));
```

### 3. Unified listener

`/introspect/*` shares **`TREMBITA_LISTEN`** with product routes by default (metrics, dashboard, health/ready on the same bind unless you split hosts). Host-split deployments can mount ops-only tables on `ops.internal` and product + introspect on `api.example.com` ([unified-listener](unified-listener.md)).

### 4. Auth model

Same [`AuthFn`](../../crates/trembita-http/src/lib.rs) / [`GatewayIdentity`](../../crates/trembita/src/gateway/identity.rs) as other product APIs via [`RouteTable::with_auth_mode`](../../crates/trembita-http/src/routing/auth.rs). Session cookies, JWT, RBAC — app-defined ([gateway-identity](gateway-identity.md)). Routes in a custom `RouteTable` without `AuthMode::Identity` stay unprotected unless the app adds checks.

### 5. Testing

| Layer | Test |
|-------|------|
| Unit | `introspect_routes.rs` — 404 on missing actor/node, auth rejects without token |
| Integration | `trembita/tests/gateway_introspect_http.rs` — merged table, JSON shape |
| Facade | `AuthMode::Identity` → `401` on `/introspect/cluster` without identity |

Inventory: [testing-coverage.md](../testing-coverage.md).

## Consequences

**Positive**

- Operator UIs mount trembita snapshots beside their own handlers — no duplicate REST glue.
- Same JSON contract as ops/dashboard; frontend code can share fetch paths.
- Pairs naturally with `JobsApi` for full operator coverage (depth gauges + per-job list/requeue).
- Reuses existing [`Observer`](../../crates/trembita-dashboard/src/views.rs) port — no new data layer.

**Negative**

- `trembita-http` depends on `trembita-dashboard` (view types). Extracting views to a separate crate remains possible if the dependency graph becomes painful.
- Host-based routing can expose identical paths on different virtual hosts — pick one **authoritative** introspection URL for your operator UI (usually the identity-protected product hostname).
- Cross-node actor aggregation semantics unchanged — still whatever `TrembitaObserver` returns on the queried node; not a new cluster-wide fan-out API.

## Out of scope

| Item | Reason |
|------|--------|
| Mutating introspection | Read-only by design ([observability §4](observability.md)) |
| `list_jobs` on IntrospectApi | Already on `JobsApi`; document pairing |
| Moving `Observer` out of `trembita-dashboard` | Only if crate split is needed |

## Alternatives considered

| Option | Verdict |
|--------|---------|
| Proxy a second HTTP listener from the app | Rejected — mTLS/network policy friction, bypasses gateway auth |
| Hand-written handlers in each app | Rejected — duplicated REST glue |
| Separate crate for view types only | Rejected until dependency pain is proven |

## Related

- [observability.md §4](observability.md#4-introspection-api-observer-like)
- [gateway-identity.md](gateway-identity.md)
- [product-scenarios.md](product-scenarios.md)
- [wire-protocol.md § ops HTTP](wire-protocol.md#ops-http-tcp-on-trembita_listen)
- [schedule-source.md](schedule-source.md)
- [external-backlog.md](external-backlog.md)
