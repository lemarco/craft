# trembita-gateway-auth

Optional **OIDC-shaped** helpers for product gateways (B-40). This crate **does not**
store sessions — after IdP success it calls [`SessionIssuer`](https://docs.rs/trembita-http/latest/trembita_http/trait.SessionIssuer.html)
and [`SessionGate::set_session_cookie`](https://docs.rs/trembita-http/latest/trembita_http/struct.SessionGate.html).

Production IdP wiring stays app-owned; see [`examples/oauth-gateway`](../../examples/oauth-gateway/).

**Regression (B-40):**

```bash
./scripts/test-fast.sh -p trembita-gateway-auth --lib b40_
```

Index: [capabilities § B-40](../../docs/scenarios/capabilities.md#gateway-auth-split-b-40).
