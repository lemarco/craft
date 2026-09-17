# Gateway identity and sticky sessions

**Status:** Accepted  
**Date:** 2026-08-31

## Context

Product apps expose HTTP / WebSocket on a **stateless gateway** while sticky
[`ActorSession`](../../crates/trembita-runtime/src/session.rs) traffic routes to
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
| [`TrembitaGatewayState::open_worker_session_parts`](../../crates/trembita/src/gateway/state.rs) | Auth + [`SessionHandle`] on WebSocket upgrade |
| [`SessionHandle`] | cast / ask with auto-reopen |
| [`GatewayHandle`] | Graceful drain on shutdown |

Auth is **never** prescribed (no JWT crate, no cookie store in trembita).

### Transport

- Gateway edge is **hyper** (HTTP/1 + WebSocket upgrade) on a separate TCP listener.
- [`GatewayRequest::from_http`] bridges `http` crate requests.
- [`IdentityError::into_http_response`] maps to 401/403/500 gateway responses.
- Product routes are declared with [`RouteTable`](../../crates/trembita-http/src/routing/table.rs) via [`GatewayOpts::surfaces`](../../crates/trembita/src/gateway/opts.rs).

### Shutdown

- [`spawn_gateway`] returns [`GatewayHandle`].
- [`ShutdownOpts::drain_gateway`] (default `true`) waits for active connections
  up to `TREMBITA_HTTP_DRAIN_TIMEOUT` (alias `TREMBITA_GATEWAY_DRAIN_TIMEOUT`) /
  [`GatewayOpts::drain_timeout`] (default 30s).

## Rejected

- Mandatory URL presets (`/ws?user=`) as the only API — presets are examples only
- Native hyper routing at the edge — trembita owns path matching and dispatch
- Built-in JWT / OAuth — stays in user code via [`GatewayIdentity`]

## Consequences

**Positive:** Auth freedom; session key aligned with `session_str`; shorter realtime handlers; graceful gateway drain.

**Negative:** Protected routes require a [`GatewayIdentity`] implementation and route-level [`AuthMode::Identity`](../../crates/trembita-http/src/routing/auth.rs) ([gateway-routing-v2](gateway-routing-v2.md)).

## Related

- [gateway-cluster-auth](gateway-cluster-auth.md) — cluster-verifiable session cookies (B-29); wave index [status § B-28–B-32](../status.md#product-scale-wave-b-28b32)
- [realtime-sessions](../scenarios/realtime-sessions.md)
- [product-scenarios](product-scenarios.md)
- [security](security.md) — browser TLS stays user-owned

[`GatewayIdentity`]: ../../crates/trembita/src/gateway/identity.rs
[`SessionKey`]: ../../crates/trembita/src/gateway/identity.rs
[`GatewayRequest::from_http`]: ../../crates/trembita/src/gateway/identity.rs
[`IdentityError`]: ../../crates/trembita/src/gateway/identity.rs
[`IdentityError::into_http_response`]: ../../crates/trembita/src/gateway/identity.rs
[`spawn_gateway`]: ../../crates/trembita/src/gateway/mod.rs
[`GatewayHandle`]: ../../crates/trembita/src/gateway/drain.rs
[`ShutdownOpts::drain_gateway`]: ../../crates/trembita/src/app/mod.rs
[`GatewayOpts::drain_timeout`]: ../../crates/trembita/src/gateway/mod.rs
