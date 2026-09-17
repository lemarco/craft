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

## B-40 — logic / storage split

**B-29** shipped cluster-safe **signed cookies**; **B-40** splits **issue / verify / optional registry** into composable ports so IdP callbacks and custom login handlers never embed session storage inside auth helpers.

| Port | Crate | Role |
|------|-------|------|
| [`SessionVerifier`](../../crates/trembita-http/src/routing/session_ports.rs) | `trembita-http` | Opaque or signed cookie token → [`VerifiedSession`](../../crates/trembita-http/src/routing/session_ports.rs) (`user` sticky key) |
| [`SessionIssuer`](../../crates/trembita-http/src/routing/session_ports.rs) | `trembita-http` | After edge identity / OIDC → session token + TTL |
| [`SessionGate::from_verifier`](../../crates/trembita-http/src/routing/session_ports.rs) | `trembita-http` | HTTP routes use the verifier trait instead of ad-hoc cookie parsing |
| [`GatewaySessionStore`](../../crates/trembita/src/gateway/cluster_session.rs) | `trembita` | Optional server-side registry ([`CapStoreGatewaySessionStore`](../../crates/trembita/src/gateway/cluster_session.rs)) |

**Adapters (pick one verify path per app):**

| Mode | Issue | Verify | Cluster note |
|------|-------|--------|--------------|
| **Signed cookie** (default product) | [`SignedCookieSessionIssuer`](../../crates/trembita/src/gateway/cluster_session.rs) | [`SignedCookieSessionVerifier`](../../crates/trembita/src/gateway/cluster_session.rs) | Stateless — same `TREMBITA_GATEWAY_SESSION_SECRET` on all nodes |
| **Cap store opaque** | [`register_capstore_session`](../../crates/trembita/src/gateway/cluster_session.rs) | [`CapStoreSessionVerifier`](../../crates/trembita/src/gateway/cluster_session.rs) | Requires shared/durable cap store ([B-29](#decision) registry row) |
| **External IdP** | App handler → `SessionIssuer` | Usually signed cookie or cap-store verifier on protected routes | OAuth/OIDC logic stays app-owned |

**Secret rotation:** set **`TREMBITA_GATEWAY_SESSION_SECRET`** to the new value and **`TREMBITA_GATEWAY_SESSION_SECRET_PREVIOUS`** to the old value on **all** nodes; [`SignedCookieSessionVerifier`](../../crates/trembita/src/gateway/cluster_session.rs) accepts cookies signed with either key; new logins use the primary via [`rotating_cluster_session_gate`](../../crates/trembita/src/gateway/cluster_session.rs). Runbook: [production-runbook § B-40](../ops/production-runbook.md#gateway-session-rotation-b-40).

**OIDC helper crate:** [`trembita-gateway-auth`](../../crates/trembita-gateway-auth/) — callback flow only; [`issue_gateway_session`](../../crates/trembita-gateway-auth/src/lib.rs) + [`DevOidcCallback`](../../crates/trembita-gateway-auth/src/dev_oidc.rs) for local demos. Enable on apps: `trembita` feature **`gateway-auth`**. Example: [`examples/oauth-gateway`](../../examples/oauth-gateway/).

Scenario index: [capabilities § B-40](../scenarios/capabilities.md#gateway-auth-split-b-40).

### Automated regression (B-40)

| Scenario | Regression |
|----------|------------|
| `SessionGate::from_verifier` (missing / bad / ok cookie) | `b40_session_gate_from_verifier_scenarios_table` |
| Signed cookie issuer ↔ verifier roundtrip | `b40_signed_cookie_issuer_verifier_port_roundtrip` |
| Cap-store issuer ↔ verifier + revoke | `b40_capstore_issuer_verifier_ports_roundtrip`, `b40_revoke_capstore_session` |
| Rotation accepts previous secret | `b40_rotating_verifier_accepts_previous_secret_token`, `b40_rotating_gate_accepts_token_signed_with_previous_secret` |
| Previous secret ignored when unset | `b40_rotating_verifier_rejects_previous_when_not_configured` |
| Cookie picked among multiple cookies | `b40_session_user_from_cookie_among_multiple_cookies` |
| HTTP login → `/me` via issuer + verifier ports | `b40_signed_cookie_issuer_login_then_verifier_me_route` |
| Revoked cap-store session → 401 on `/me` | `b40_capstore_revoked_session_returns_unauthorized_on_me` |
| `issue_gateway_session` JSON + `Set-Cookie` | `b40_issue_gateway_session_json_body_and_set_cookie` |
| Dev OIDC callback happy path | `b40_dev_oidc_callback_issues_session_cookie` |
| Dev OIDC rejects bad `user` query | `b40_dev_oidc_callback_rejects_invalid_user_scenarios_table` |

```bash
./scripts/test-fast.sh -p trembita-http --lib b40_
./scripts/test-fast.sh -p trembita --lib b40_
./scripts/test-fast.sh -p trembita --test gateway_cluster_session b40_
./scripts/test-fast.sh -p trembita-gateway-auth --lib b40_
```

**Related cluster session tests (B-29):** `./scripts/test-fast.sh -p trembita --test gateway_cluster_session` (full file, not only `b40_`).

## Related

- [gateway-identity](gateway-identity.md)
- [realtime-sessions](../scenarios/realtime-sessions.md)
- [idempotency-contract](idempotency-contract.md) — cap store for handler markers, not gateway auth by default
