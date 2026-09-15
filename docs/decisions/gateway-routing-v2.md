# Gateway routing v2 — native HTTP model

**Status:** Accepted  
**Date:** 2026-09-05

## Context

Product HTTP is a first-class part of [framework-conventions](framework-conventions.md): homogeneous nodes, [`TrembitaApp`](../../crates/trembita/src/app/mod.rs), host-based surfaces, [gateway-identity](gateway-identity.md), connection drain.

Large gateways are mostly **declarative data** (`RouteTable`), not bespoke handler wiring per route. Host surfaces, session gates, and CORS belong on the **surface**, not copy-pasted in every app.

## Decision

**Native gateway HTTP model** — product apps use trembita-owned routing types only (no third-party router crate in app `Cargo.toml`).

### Public API (`trembita::gateway::http`)

| Type | Role |
|------|------|
| [`RouteTable`](../../crates/trembita-http/src/routing/table.rs) | Declarative routes — method, path pattern, handler, auth mode |
| [`Handler`](../../crates/trembita-http/src/routing/handler.rs) | Async request handler over [`RequestCtx`](../../crates/trembita-http/src/routing/ctx.rs) |
| [`RequestCtx`](../../crates/trembita-http/src/routing/ctx.rs) | Method, path params, query, headers, body, optional session/identity |
| [`Response`](../../crates/trembita-http/src/routing/ctx.rs) | Status, headers, body — including `Set-Cookie` |
| [`Gateway`](../../crates/trembita-http/src/gateway/mod.rs) | Builder: surfaces (hostnames), gates, dev fallback |
| [`Surface`](../../crates/trembita-http/src/gateway/mod.rs) | One product surface — hosts + CORS + session + routes |
| [`AuthMode`](../../crates/trembita-http/src/routing/auth.rs) | `Open`, `Session(SessionGate)`, `Identity` |
| [`CorsPolicy`](../../crates/trembita-http/src/gateway/cors.rs) | Origins, methods, headers, credentials, max-age |

### Wiring

```rust
GatewayOpts::from_env()?.surfaces(|state| {
    Gateway::new(cfg.is_production())
        .surface(|s| {
            s.hosts(["api.example.com"])
                .cors(CorsPolicy::allow_origins(["https://app.example.com"]))
                .routes(my_api::ROUTE_TABLE)
        })
        .dev_fallback(RouteTable::new().get("/health", health))
})
```

Use [`GatewayOpts::surfaces`](../../crates/trembita/src/gateway/opts.rs) — not a closure that returns a foreign `Router`.

### Transport

- **hyper** edge for plain HTTP, TLS, and WebSocket upgrades ([`drain.rs`](../../crates/trembita/src/gateway/drain.rs)).
- **tower** middleware: connection tracking, rate limit, body limit, CORS, session gates.
- [`TrembitaGatewayState::open_actor_session_parts`] for sticky WebSocket sessions.

### Built-in product APIs

[`JobsApi`](../../crates/trembita-http/src/lib.rs), [`ActorsApi`](../../crates/trembita-http/src/lib.rs),
[`WorkflowsApi`](../../crates/trembita-http/src/lib.rs), [`IntrospectApi`](../../crates/trembita-http/src/lib.rs),
[`UpgradeApi`](../../crates/trembita-http/src/upgrade_routes.rs), [`StaticSite`](../../crates/trembita-http/src/static_site/mod.rs)
expose [`RouteTable`](../../crates/trembita-http/src/routing/table.rs) entries for explicit merges ([unified-listener](unified-listener.md)).

## Rejected

- Re-exporting a third-party HTTP router into product apps — routing is a trembita product surface
- Prescribing business auth — unchanged from [gateway-identity](gateway-identity.md); apps supply [`SessionGate`](../../crates/trembita-http/src/routing/auth.rs) / [`GatewayIdentity`](../../crates/trembita/src/gateway/identity.rs)

## Consequences

**Positive:** Single dependency story; host/session/CORS as declarations; route tables are diffable test data.

**Negative:** Trembita owns path matching, body limits, and JSON extraction that a generic router would provide.

## Related

- [gateway-identity](gateway-identity.md) — identity/session layers
- [framework-conventions](framework-conventions.md) — product app layout (`http/` module)
- [introspect-api](introspect-api.md) — built-in routes as `RouteTable` entries
