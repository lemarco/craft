# Gateway cluster session auth (B-29)

**Status:** Accepted  
**Date:** 2026-09-17  
**Backlog:** B-29 (shipped)

## Context

Product gateways run **on every node** behind a load balancer ([realtime-sessions](../scenarios/realtime-sessions.md)). Login flows that keep valid session tokens in a **process-local `HashSet`** break as soon as the next HTTP request hits another VPS.

Teams still own **business auth** (OAuth, Postgres sessions, API keys). Trembita should supply **composable cluster primitives**, not one OAuth stack.

## Decision

### Three layers (compose, do not merge)

| Layer | Mechanism | Cluster-safe |
|-------|-----------|----------------|
| Edge identity | [`GatewayIdentity`](../../crates/trembita/src/gateway/identity.rs) on protected routes | Stateless verify (JWT, bearer, mTLS claim) |
| Browser session cookie | [`ClusterSessionSecret`](../../crates/trembita/src/gateway/cluster_session.rs) + [`SessionGate`](../../crates/trembita-http/src/routing/auth.rs) | **Yes** — shared `TREMBITA_GATEWAY_SESSION_SECRET` |
| Optional server registry | [`register_capstore_session`](../../crates/trembita/src/gateway/cluster_session.rs) + [`CapStateStore`](../../crates/trembita-capstore/src/store.rs) | **Yes** when store is cluster-visible (redb / Redis / Postgres adapters) |
| Sticky compute | [`Route::Session`](../../crates/trembita/src/capability/route.rs) + [`SessionHandle`](../../crates/trembita/src/gateway/session.rs) / WS | Session **key** from verified cookie, not raw token |

OAuth/OIDC stays **app-owned**: implement [`GatewayIdentity`] against your IdP; set [`GatewayAuthProfile::ExternalIdp`](../../crates/trembita/src/gateway/auth_profile.rs) when cookie login is not the primary path.

### Env surface

| Variable | Role |
|----------|------|
| `TREMBITA_GATEWAY_SESSION_SECRET` | Same bytes on all nodes; signs session cookies |
| `TREMBITA_GATEWAY_AUTH_PROFILE` | `cookie-only` (default) or `external-idp` |
| `GATEWAY_TOKEN` / bearer | Unchanged — edge identity for login/WS ([env.md](../env.md)) |
| `{PREFIX}_COOKIE_*` | Cookie name/flags via [`CookieConfig`](../../crates/trembita-http/src/cookie_config.rs) |

### Realtime showcase

[`examples/realtime/`](../../examples/realtime/) uses signed cookies ([`ClusterGatewaySession`](../../examples/realtime/src/gateway_session.rs)) instead of an in-memory store.

## Rejected

- Built-in OAuth/OIDC provider in `trembita` — apps bring IdP + [`GatewayIdentity`]
- Mandatory Redis session store — cap-store adapters are optional ([status.md](../status.md))

## Consequences

**Positive:** Cookie-protected routes and sticky caps work on any gateway node; explicit opt-in for external IdP.

**Negative:** Operators must distribute `TREMBITA_GATEWAY_SESSION_SECRET`; rotation requires re-login (document in runbooks).

## Automated regression (B-29)

| Area | Location |
|------|----------|
| Cookie issue / verify / expiry / tamper | `trembita/src/gateway/cluster_session.rs` (unit) |
| Dual gateway + shared secret, wrong secret, cap-store gate | `trembita/tests/gateway_cluster_session.rs` |
| Realtime showcase wiring | `examples/realtime/src/gateway_session.rs` |

```bash
./scripts/test-fast.sh -p trembita --lib cluster_session
./scripts/test-fast.sh -p trembita --test gateway_cluster_session
```

Wave index: [status § B-28–B-32](../status.md#product-scale-wave-b-28b32).

## Related

- [gateway-identity](gateway-identity.md)
- [realtime-sessions](../scenarios/realtime-sessions.md)
- [idempotency-contract](idempotency-contract.md) — cap store for handler markers, not gateway auth by default
