# OAuth gateway demo (B-40 / B-47)

Simulates OIDC **authorize + callback** with **PKCE (S256)** and a **redirect URI allowlist** — then mints a cluster session via [`SessionIssuer`](../../crates/trembita-http/src/routing/session_ports.rs).

```bash
cargo run --release
BIND=127.0.0.1:8395
REDIRECT="http://${BIND}/oauth/callback"

curl -s "http://${BIND}/oauth/start?redirect_uri=${REDIRECT}" -c /tmp/oauth-pending.txt
# Copy state + code_challenge from JSON (see callback_hint), then:
curl -s "http://${BIND}/oauth/callback?user=alice&redirect_uri=${REDIRECT}&state=ST&code_challenge=CH" \
  -b /tmp/oauth-pending.txt -c /tmp/sess.txt
curl -s "http://${BIND}/me" -b /tmp/sess.txt
```

Production: set **`TREMBITA_OAUTH_REDIRECT_ALLOWLIST`** (comma-separated exact URIs) and optional **`TREMBITA_OAUTH_PKCE_METHOD=S256`** on every gateway node. Session secret rotation stays [runbook § B-40](../../docs/ops/production-runbook.md#gateway-session-rotation-b-40).

Multi-node: same **`TREMBITA_GATEWAY_SESSION_SECRET`** on every process ([gateway-cluster-auth](../../docs/decisions/gateway-cluster-auth.md)).

Replace [`DevOidcCallback`](../../crates/trembita-gateway-auth/src/dev_oidc.rs) / [`DevOidcAuthorize`](../../crates/trembita-gateway-auth/src/authorize.rs) with your IdP’s handlers; keep [`issue_gateway_session`](../../crates/trembita-gateway-auth/src/lib.rs) as the cookie mint path.
