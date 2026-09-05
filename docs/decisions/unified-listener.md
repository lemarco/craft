# Unified listener — one port, two sockets, explicit routes

**Status:** Accepted  
**Date:** 2026-09-05  
**Target release:** 0.5.0

## Context

Trembita ran **three HTTP-related listeners** per node:

| Listener | Default | Stack |
|----------|---------|-------|
| Wire | `:7443/udp` | QUIC + HTTP/3 + mTLS + postcard |
| Admin | `:8080/tcp` | HTTP/1.1 — health, metrics, dashboard, introspect |
| Gateway | `:8090/tcp` | HTTP/1.1 — product routes + hidden built-in APIs |

Built-in routes (`/jobs/*`, `/introspect/*`, …) were mounted via `.with_*_api(true)` flags
and merged invisibly in `collect_builtin_routes()`. Apps could not see the full route table in
`src/http/` or `app.rs`.

[framework-conventions](framework-conventions.md) prescribes HTTP ingress via explicit
[`RouteTable`](../../crates/trembita-http/src/routing/table.rs) in `src/http/`, but the framework
violated its own rule for operational routes.

## Decision

### One port number, two sockets (industry standard)

```
:443/udp  →  QUIC + HTTP/3 + mTLS     wire (/raft/v1/*, actors, queue)
:443/tcp  →  TLS + HTTP/1.1 (+ h2)    product + ops (all RouteTable routes)
```

Default listen port moves from `7443` to **`443`** for both transports. Wire and HTTP share the
port number; UDP and TCP are separate sockets.

Env:

| Variable | Role |
|----------|------|
| `TREMBITA_LISTEN` | QUIC wire bind (default `0.0.0.0:443`) |
| `TREMBITA_HTTP` | Product + ops HTTP bind (default = same as `TREMBITA_LISTEN`) |
| `TREMBITA_HTTP_TLS_*` | Server TLS for the TCP listener |

**Removed:** `TREMBITA_ADMIN`, `TREMBITA_ADMIN_TLS_*`, `TREMBITA_GATEWAY_*` API flags
(`TREMBITA_GATEWAY_JOBS`, …), separate admin listener.

### All HTTP routes are explicit app code

Scaffold generates:

```
src/http/
├── mod.rs           # re-exports + merge helpers
├── ops.rs           # /health, /ready, /metrics, /introspect/*, /dashboard
├── jobs.rs          # /jobs/* (when jobs feature enabled)
└── product.rs       # custom business routes (via trembita add http-surface)
```

`app.rs` wires routes in `.surfaces()` — no hidden merge:

```rust
.surfaces(|state| {
    Gateway::new(cfg.is_production())
        .surface(|s| s.hosts(["api.example.com"]).routes(
            http::product::route_table()
                .merge(http::jobs::route_table(&state))
        ))
        .surface(|s| s.hosts(["_ops.localhost"]).routes(
            http::ops::route_table(&state)
        ))
})
```

CLI:

```bash
trembita add ops-routes
trembita add jobs-routes
trembita add http-surface api --hosts api.example.com
```

Framework provides **`OpsApi`**, **`JobsApi`**, **`IntrospectApi`**, etc. as route-table
builders — apps choose what to merge and where.

### Removed APIs

- `GatewayOpts::with_jobs_api`, `with_actors_api`, `with_workflows_api`, `with_introspect_api`
- `GatewayOpts::protect_product_apis`
- `collect_builtin_routes()` in `router.rs`
- `TrembitaConfigure::admin_addr` and admin listener spawn in `assemble.rs`
- Separate `AdminServer` production path (ops routes live in `OpsApi` → `RouteTable`)

`AdminServer` in `trembita-dashboard` remains for dashboard-crate unit tests only.

## Rejected

- Deprecation period for old admin/gateway split — hard cut in 0.5.0
- Single socket for wire + HTTP — different transports and security profiles
- Keeping hidden `.with_*_api()` flags alongside explicit routes

## Consequences

**Positive**

- One firewall rule per direction (`udp/443`, `tcp/443`)
- Full route inventory visible in `src/http/` and `app.rs`
- Framework conventions enforced for ops routes too
- Host-based separation: product surface vs ops surface

**Negative**

- Breaking: all apps using `TREMBITA_ADMIN`, `.with_jobs_api(true)`, examples, docker-compose
- Apps must merge ops/jobs routes explicitly (scaffold + CLI handle this)
- SSE `/dashboard/events` requires `RouteTable::sse()` support in gateway dispatch

## Migration (0.5.0)

1. Replace `TREMBITA_ADMIN` + `TREMBITA_GATEWAY` with `TREMBITA_HTTP=0.0.0.0:443`
2. Replace `.with_jobs_api(true)` with `http::jobs::route_table(&state)` in surfaces
3. Add `trembita add ops-routes` or copy from scaffold template
4. Point probes at ops host: `GET /health`, `GET /ready`, `GET /metrics`

## Related

- [gateway-routing-v2](gateway-routing-v2.md) — native RouteTable model
- [framework-conventions](framework-conventions.md) — `src/http/` layout
- [wire-protocol](wire-protocol.md) — QUIC wire (updated default port)
- [introspect-api](introspect-api.md) — superseded ops-on-gateway section
