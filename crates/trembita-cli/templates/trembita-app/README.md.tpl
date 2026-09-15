# {{PROJECT_TITLE}}

trembita product app — scaffolded with [`trembita new`]({{TREMBITA_DOC_BASE}}/docs/decisions/framework-conventions.md).

## Layout

```
src/
  main.rs       boot
  app.rs        TrembitaApp wiring (gateway + run)
  manifest.rs   jobs / topics / workers registry (edit `// trembita:*` regions)
  config.rs     env config
  consumers/    job handlers
  domain/       business logic (no trembita imports)
  http/         gateway route tables (when gateway feature enabled)
```

## Run locally

```bash
cargo run
```

HTTP (when gateway enabled): same port as `TREMBITA_LISTEN` (default `0.0.0.0:443`) — product routes plus ops (`/health`, `/dashboard`, `/metrics`, `/introspect/*`) from `src/http/ops.rs` or [`TrembitaApp::from_env()`]({{TREMBITA_DOC_BASE}}/docs/getting-started.md) defaults.

Set `GATEWAY_TOKEN` (or `TREMBITA_GATEWAY_TOKEN`) before calling identity-protected APIs.

## 3-node cluster

See `deploy/docker-compose.yml` and `deploy/.env.example`.

Docs: [getting-started]({{TREMBITA_DOC_BASE}}/docs/getting-started.md) · [scenarios]({{TREMBITA_DOC_BASE}}/docs/scenarios/README.md)
