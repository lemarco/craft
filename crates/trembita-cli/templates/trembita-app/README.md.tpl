# {{PROJECT_TITLE}}

trembita product app — scaffolded with [`trembita new`]({{TREMBITA_DOC_BASE}}/docs/decisions/framework-conventions.md).

## Layout

```
src/
  main.rs          boot
  app.rs           TrembitaApp wiring (gateway + run)
  manifest.rs      jobs / topics / capabilities registry (`// trembita:*` regions)
  config.rs        env config
  capabilities/    CapManifest ops (default product path)
  consumers/       job handlers
  actors/          UserActor groups (optional `--features actors`, advanced)
  domain/          business logic (no trembita imports)
  http/            gateway route tables (when gateway feature enabled)
```

Product HTTP is declared in `src/http/product.rs` via [`cap_fire`]({{TREMBITA_DOC_BASE}}/docs/decisions/capability-dx.md) / [`cap_invoke`]({{TREMBITA_DOC_BASE}}/docs/decisions/capability-dx.md) — not `/actors/*` cast ([greenfield wire]({{TREMBITA_DOC_BASE}}/docs/decisions/capability-greenfield-wire.md)).

## Run locally

```bash
cargo run
```

HTTP (when gateway enabled): same port as `TREMBITA_LISTEN` (default `0.0.0.0:443`) — product routes plus ops (`/health`, `/dashboard`, `/metrics`, `/introspect/*`) from `src/http/ops.rs` or [`TrembitaApp::from_env()`]({{TREMBITA_DOC_BASE}}/docs/getting-started.md) defaults.

Sample capability: `POST /ping` with JSON `{{"msg":"hello"}}` → inline `app.ping` op.

Set `GATEWAY_TOKEN` (or `TREMBITA_GATEWAY_TOKEN`) before calling identity-protected APIs.

## 3-node cluster

See `deploy/docker-compose.yml` and `deploy/.env.example`.

Docs: [getting-started]({{TREMBITA_DOC_BASE}}/docs/getting-started.md) · [capabilities scenario]({{TREMBITA_DOC_BASE}}/docs/scenarios/capabilities.md) · [domain patterns (B-52)]({{TREMBITA_DOC_BASE}}/docs/decisions/capability-dx.md#domain-dx-patterns-b-52) · [scenarios]({{TREMBITA_DOC_BASE}}/docs/scenarios/README.md)
