# Gateway routing v2 — native HTTP model (big bang)

**Status:** Accepted  
**Date:** 2026-09-05  
**Target release:** 0.4.0

## Context

Trembita is opinionated at the runtime level ([framework-conventions](framework-conventions.md)):
homogeneous nodes, [`TrembitaApp`](../../crates/trembita/src/app/mod.rs), host-based product
surfaces, gateway identity, connection drain.

Product HTTP is still expressed through **Axum** — `Router`, extractors, `IntoResponse`,
`GatewayOpts::routes(|state| Router)`. Apps must pin axum versions, learn middleware ordering,
and reimplement the same host dispatch, session gates, and CORS layers.

[gateway-identity](gateway-identity.md) deliberately separated user auth from sticky routing but
**rejected a custom HTTP stack** (“hyper/axum at the edge is sufficient”). That held while axum
was an implementation detail trembita owned end-to-end. Re-exporting axum into product apps
(CR-100) showed the boundary was wrong: **routing is a product concern**, not a third-party
framework concern.

Large production gateway ports (100+ routes) proved that most routes are **data**
(`RouteTable`), not bespoke handler code. Host surfaces, session gates, and CORS belong on the
surface, not copy-pasted per BC adapter.

## Decision

**Trembita 0.4.0 replaces Axum with a native gateway HTTP model.** One release, no transitional
API, no `trembita::axum` re-export.

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

### Wiring change

```rust
// 0.3.x — removed
GatewayOpts::new(addr).routes(|state| Router::new().route(...))

// 0.4.0
GatewayOpts::new(addr).surfaces(|g| {
    g.surface(&["api.example.com"], |s| {
        s.cors(CorsPolicy::...).routes(my_api::ROUTE_TABLE)
    })
    .dev_fallback(RouteTable::new().get("/health", health))
})
```

### Transport

- **Single hyper edge** for plain HTTP and TLS (extend the existing TLS path in
  [`drain.rs`](../../crates/trembita/src/gateway/drain.rs)).
- **tower** middleware chain: connection tracking, rate limit, body limit, CORS, session gates.
- **WebSocket** via hyper upgrades; [`TrembitaGatewayState::open_actor_session_parts`] unchanged.

### Built-in product APIs

[`JobsApi`](../../crates/trembita-http/src/lib.rs), [`ActorsApi`](../../crates/trembita-http/src/lib.rs),
[`WorkflowsApi`](../../crates/trembita-http/src/lib.rs), [`IntrospectApi`](../../crates/trembita-http/src/lib.rs),
[`UpgradeApi`](../../crates/trembita-http/src/upgrade_routes.rs), [`StaticSite`](../../crates/trembita-http/src/static_site/mod.rs)
expose [`RouteTable`](../../crates/trembita-http/src/routing/table.rs) entries, not Axum routers.

## Rejected

- **Transitional axum adapter crate** — doubles maintenance; product apps would split across two
  models during migration.
- **Gradual deprecation of `Router`** — trembita 0.3.x remains the last axum release; 0.4.0 is
  a hard cut.
- **Prescribing business auth** — unchanged from [gateway-identity](gateway-identity.md);
  [`SessionGate`](../../crates/trembita-http/src/routing/auth.rs) validates via app-supplied hook.

## Consequences

**Positive**

- Product apps depend only on `trembita` — no axum / axum-extra / tower-http in app `Cargo.toml`.
- Host surfaces, session, CORS, drain are first-class declarations, not middleware boilerplate.
- Route tables are diffable data — parity testing compares tables + responses.
- Version bumps no longer coupled to axum releases.

**Negative**

- **Breaking:** every `GatewayOpts::routes` call site, all examples, all integration tests.
- **~5–6 person-weeks** before 0.4.0 tag.
- Trembita maintains path matching, body limits, JSON extraction — scope axum previously covered.

## Migration gate (0.4.0 release checklist)

- [x] `cargo tree` — zero axum in trembita workspace
- [x] `GatewayOpts::routes` removed (use `GatewayOpts::surfaces`)
- [x] All trembita integration tests pass
- [x] Migration guide [gateway-0.4](../migration/gateway-0.4.md)
- [ ] Gateway parity suite: 125 identical, 1 expected (D-09), 0 unexplained
- [x] Examples updated
- [x] [gateway-identity](gateway-identity.md) transport section updated

## Related

- [gateway-identity](gateway-identity.md) — identity/session layers (unchanged semantics)
- [framework-conventions](framework-conventions.md) — product app layout (`http/` module)
- [introspect-api](introspect-api.md) — built-in routes become `RouteTable` entries
