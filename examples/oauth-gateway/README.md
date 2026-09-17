# OAuth gateway demo (B-40)

Simulates an OIDC **`/oauth/callback`** that calls [`SessionIssuer`](../../crates/trembita-http/src/routing/session_ports.rs) — **no session storage in the auth crate**.

```bash
cargo run --release
curl -s 'http://127.0.0.1:8395/oauth/callback?user=alice' -c /tmp/sess.txt
curl -s 'http://127.0.0.1:8395/me' -b /tmp/sess.txt
```

Multi-node: set the same `TREMBITA_GATEWAY_SESSION_SECRET` on every process ([gateway-cluster-auth](../../docs/decisions/gateway-cluster-auth.md)).

Replace [`DevOidcCallback`](../../crates/trembita-gateway-auth/src/dev_oidc.rs) with your IdP’s authorization-code handler; keep [`issue_gateway_session`](../../crates/trembita-gateway-auth/src/lib.rs) as the only cookie mint path.
