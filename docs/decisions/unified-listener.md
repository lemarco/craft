# Unified listener — one port, two sockets, explicit routes

**Status:** Accepted  
**Date:** 2026-09-05

## Context

Product nodes expose **one published port number** for cluster wire and for HTTP. Split admin/gateway TCP listeners and hidden built-in route merges made the full route table invisible in app code — conflicting with [framework-conventions](framework-conventions.md) (`src/http/` + explicit [`RouteTable`](../../crates/trembita-http/src/routing/table.rs) merges).

Product **capabilities** (jobs, topics, workers) live in scaffold `src/manifest.rs`; HTTP wiring stays in `app.rs` / `src/http/`.

## Decision

### One port number, two sockets

```
:443/udp  →  QUIC + HTTP/3 + mTLS     wire (/raft/v1/*, actors, queue)
:443/tcp  →  TLS + HTTP/1.1 (+ h2)    product + ops (all RouteTable routes)
```

Wire and HTTP share the port **number**; UDP and TCP are separate sockets. Default: **`TREMBITA_LISTEN=0.0.0.0:443`**.

| Variable | Role |
|----------|------|
| `TREMBITA_LISTEN` | QUIC (UDP) + product/ops HTTP (TCP) on the same `host:port` |
| `TREMBITA_HTTP` / `TREMBITA_GATEWAY` | **Internal:** `-` disables TCP only; otherwise omit |
| `TREMBITA_HTTP_TLS_*` | Server TLS for the TCP listener |

Do **not** use separate admin/gateway env ports or `TREMBITA_GATEWAY_*` API toggles — see [env.md](../env.md).

### Explicit HTTP routes

Scaffold layout:

```
src/http/
├── mod.rs           # re-exports + merge helpers
├── ops.rs           # /health, /ready, /metrics, /introspect/*, /dashboard
├── jobs.rs          # /jobs/* (when jobs feature enabled)
└── product.rs       # custom business routes
```

`app.rs` wires [`.gateway_routes()` / `.surfaces()`](../../crates/trembita/src/app/builder.rs):

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

Framework builders: **`OpsApi`**, **`JobsApi`**, **`IntrospectApi`**, etc. Apps choose what to merge and where.

### `TrembitaApp::from_env` defaults

[`TrembitaApp::from_env()`](../../crates/trembita/src/app/runtime.rs) mounts **ops + registration-driven product APIs** on `TREMBITA_LISTEN` via [`default_surfaces`](../../crates/trembita/src/app/gateway.rs) unless you opt out ([`.without_ops()`](../../crates/trembita/src/app/builder.rs), [`.without_jobs_api()`](../../crates/trembita/src/app/builder.rs), …). Custom host splits still use explicit `src/http/` merges.

### Cluster-only ops HTTP

[`spawn_cluster_ops_http`](../../crates/trembita/src/gateway/cluster_ops.rs) / [`cluster_ops_route_table`](../../crates/trembita/src/gateway/cluster_ops.rs) for [`TrembitaCluster`](../../crates/trembita/src/cluster.rs) and [`trembita-node`](../../crates/trembita-tools/src/bin/node.rs) without a product gateway.

`AdminServer` in `trembita-dashboard` is for crate unit tests only; production ops use `OpsApi` → `RouteTable`.

## Rejected

- Single socket for wire + HTTP — different transports and security profiles
- Hidden `.with_*_api()` flags alongside explicit routes

## Consequences

**Positive:** One firewall rule per direction; full route inventory in app source; host-based product vs ops surfaces.

**Negative:** Host-split and fully custom gateways require explicit route-table merges; SSE `/dashboard/events` needs `RouteTable::sse()` in dispatch.

## Deploy checklist

1. Set **`TREMBITA_LISTEN`** (wire + TCP share this port)
2. Prefer **`TrembitaApp::from_env()`** + `.jobs()` / `.topics()` / `.workers()` when defaults fit
3. Otherwise merge `ops_api()` and product API tables in `.surfaces()` / `.gateway_routes()`; use `RouteTable::with_auth_mode(AuthMode::Identity)` for protected APIs
4. Run `trembita doctor --preflight`; scrape `/health` and `/metrics` on the unified bind

**Docker / VPS firewall:** publish one port number for UDP + TCP; split product vs ops by **hostname** on the same TCP bind when needed ([production-runbook](../ops/production-runbook.md)).

## Related

- [gateway-routing-v2](gateway-routing-v2.md) — native RouteTable model
- [framework-conventions](framework-conventions.md) — `src/http/` layout
- [wire-protocol](wire-protocol.md) — QUIC wire
- [introspect-api](introspect-api.md) — introspection routes
