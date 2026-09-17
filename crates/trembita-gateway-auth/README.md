# trembita-gateway-auth

Optional **OIDC-shaped** helpers for product gateways (B-40, **B-47**). This crate **does not**
store sessions — after IdP success it calls [`SessionIssuer`](https://docs.rs/trembita-http/latest/trembita_http/trait.SessionIssuer.html)
and [`SessionGate::set_session_cookie`](https://docs.rs/trembita-http/latest/trembita_http/struct.SessionGate.html).

**B-47 production guards:** [`OidcProductionConfig`](src/production.rs) (`TREMBITA_OAUTH_REDIRECT_ALLOWLIST`, PKCE **S256** default), [`DevOidcAuthorize`](src/authorize.rs), hardened [`DevOidcCallback`](src/dev_oidc.rs).

Production IdP wiring stays app-owned; see [`examples/oauth-gateway`](../../examples/oauth-gateway/) and [runbook § B-47](../../docs/ops/production-runbook.md#oauth-production-wiring-b-47).

**Regression:**

```bash
./scripts/test-fast.sh -p trembita-gateway-auth --lib b40_
./scripts/test-fast.sh -p trembita-gateway-auth --lib b47_
```

Index: [capabilities § B-40](../../docs/scenarios/capabilities.md#gateway-auth-split-b-40) · [§ B-47](../../docs/scenarios/capabilities.md#oauth-prod-hardening-b-47).
