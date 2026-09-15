# Migrating product HTTP to trembita 0.4.0

**From:** Axum `Router` + `GatewayOpts::routes`  
**To:** native [`Gateway`](../../crates/trembita-http/src/gateway/mod.rs) + [`RouteTable`](../../crates/trembita-http/src/routing/table.rs)

See [gateway-routing-v2](../decisions/gateway-routing-v2.md) for rationale.

## Dependency changes

Remove from your app `Cargo.toml`:

- `axum`, `axum-extra`, `tower-http` (if only used for gateway)

Keep `trembita` with feature `http-jobs` (or your app's `gateway` feature).

## Wiring

```rust
// 0.3.x — removed
.gateway(GatewayOpts::new(addr).routes(|state| {
    Router::new().route("/health", get(...)).with_state(state)
}))

// 0.4.0
.gateway(GatewayOpts::new(addr).surfaces(|state| {
    Gateway::new(cfg.is_production())
        .surface(|s| {
            s.hosts(["api.example.com"])
                .cors(CorsPolicy::allow_origins(["https://app.example.com"]))
                .session(SessionGate::validate("sess", validate_session))
                .routes(api::ROUTE_TABLE)
        })
        .dev_fallback(RouteTable::new().get("/health", health))
}))
```

## Handlers

| Axum | 0.4.0 |
|------|-------|
| `State(state)` + extractors | `RequestCtx` — `ctx.json()`, `ctx.params()`, `ctx.headers()` |
| `IntoResponse` | `Result<Response, HttpError>` |
| `Router::merge(api.router())` | `table.merge(api.route_table())` or built-in APIs via `GatewayOpts` flags |

## Auth

| Pattern | API |
|---------|-----|
| Gateway bearer / custom identity | `GatewayOpts::identity(...)` + `.get_identity()` / `.post_identity()` |
| Cookie session on a surface | `Surface::session(SessionGate::...)` + `.get_session()` / `.post_session()` |
| Protect whole module | `RouteTable::new().merge_authed(AuthMode::Session, submodule::ROUTE_TABLE)` |
| Built-in `/jobs/*`, `/introspect/*` | **0.5.0:** explicit route-table merges + `AuthMode::Identity` — [unified-listener-0.5.md](unified-listener-0.5.md) |

## WebSocket

```rust
RouteTable::new().websocket("/ws", move |req| {
    Box::pin(async move {
        match state.open_actor_session_parts(...).await {
            Ok(handle) => accept_websocket(req, |stream| async move { ... }),
            Err(e) => routing_to_http_response(e.into_http_response()),
        }
    })
})
```

## Parity testing

Compare route tables between old and new stacks:

```rust
let diff = actual.diff(&expected);
assert!(diff.is_empty(), "{diff:?}");
```

[`RouteTable::diff`](../../crates/trembita-http/src/routing/diff.rs) reports missing, extra, and auth-mode mismatches.

## Renames

| 0.3.x | 0.4.0 |
|-------|-------|
| `build_gateway_router` | removed — use `build_gateway_service` |
| `HostRouter` / `MultiHostBuilder` | `Gateway` + `Surface` |
| `trembita::axum::*` re-exports | removed — use `trembita_http` types |
