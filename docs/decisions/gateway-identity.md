# Gateway identity and sticky sessions

**Status:** Accepted  
**Date:** 2026-08-31

## Context

Product apps expose HTTP / WebSocket on a **stateless gateway** while sticky
[`ActorSession`](../../crates/trembita-actor/src/session.rs) traffic routes to
pinned workers ([realtime-sessions](../scenarios/realtime-sessions.md)).

Teams need:

- Their own auth (JWT, cookie→Postgres, API keys) — not built into trembita
- A stable **session key** for consistent-hash worker pick (same as `session_str`)
- Less boilerplate than hand-rolling reconnect + WS lifecycle

## Decision

### Layers (do not mix)

| Layer | Owner | Example |
|-------|-------|---------|
| User auth | Application | JWT, cookie session in Postgres |
| Session key | App mapping or [`SessionKey`] | `user_id`, `room_id` |
| Sticky routing | Trembita | `SessionHandle`, `ActorSession` |

### Public API (`trembita::gateway`)

| Type | Role |
|------|------|
| [`GatewayIdentity`] | User struct + `extract()` |
| [`SessionKey`] | Default map identity → session key |
| [`GatewayOpts::identity_mapped`] | Custom session key when identity ≠ key |
| [`TrembitaGatewayState::extract_session`] | Auth + session key (HTTP) |
| [`TrembitaGatewayState::extract_session_parts`] | Same for WebSocket upgrade (`Method`, `Uri`, `HeaderMap`) |
| [`TrembitaGatewayState::open_actor_session_parts`] | Auth + [`SessionHandle`] on WebSocket upgrade |
| [`SessionHandle`] | cast / ask with auto-reopen |
| [`GatewayHandle`] | Graceful drain on shutdown |

Auth is **never** prescribed (no JWT crate, no cookie store in trembita).

### Transport

- Gateway remains **Axum** on a separate TCP listener (product edge).
- [`GatewayRequest::from_http`] bridges axum requests.
- [`IdentityError`] implements [`IntoResponse`] (401/403/500).

### Shutdown

- [`spawn_gateway`] returns [`GatewayHandle`].
- [`ShutdownOpts::drain_gateway`] (default `true`) waits for active connections
  up to `TREMBITA_GATEWAY_DRAIN_TIMEOUT` / [`GatewayOpts::drain_timeout`] (default 30s).

## Rejected

- Mandatory URL presets (`/ws?user=`) as the only API — presets are examples only
- Custom HTTP stack — hyper/axum at the edge is sufficient
- Built-in JWT / OAuth — stays in user code via [`GatewayIdentity`]

## Consequences

**Positive:** Auth freedom; session key aligned with `session_str`; shorter realtime handlers; graceful gateway drain.

**Negative:** Breaking change — `.routes(|state| …)` instead of `.routes(|app| …)`; users must implement [`GatewayIdentity`] for protected routes.

## Related

- [realtime-sessions](../scenarios/realtime-sessions.md)
- [product-scenarios](product-scenarios.md)
- [security](security.md) — browser TLS stays user-owned

[`GatewayIdentity`]: ../../crates/trembita/src/gateway/identity.rs
[`SessionKey`]: ../../crates/trembita/src/gateway/identity.rs
[`GatewayRequest::from_http`]: ../../crates/trembita/src/gateway/identity.rs
[`IdentityError`]: ../../crates/trembita/src/gateway/identity.rs
[`IntoResponse`]: https://docs.rs/axum/latest/axum/response/trait.IntoResponse.html
[`spawn_gateway`]: ../../crates/trembita/src/gateway/mod.rs
[`GatewayHandle`]: ../../crates/trembita/src/gateway/drain.rs
[`ShutdownOpts::drain_gateway`]: ../../crates/trembita/src/app.rs
[`GatewayOpts::drain_timeout`]: ../../crates/trembita/src/gateway/mod.rs
