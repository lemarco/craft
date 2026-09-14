# {{PROJECT_TITLE}}

trembita product app — scaffolded with [`trembita new`]({{TREMBITA_DOC_BASE}}/docs/decisions/framework-conventions.md).

## Layout

```
src/
  main.rs       boot
  app.rs        TrembitaApp wiring
  config.rs     env config
  consumers/    job handlers
  domain/       business logic (no trembita imports)
  http/         gateway route tables (when gateway feature enabled)
```

## Run locally

```bash
cargo run
```

HTTP gateway (when enabled): `http://127.0.0.1:8090` — product routes plus ops (`/health`, `/dashboard`, `/metrics`, `/introspect/*`) from `src/http/ops.rs`.

Set `GATEWAY_TOKEN` (or `TREMBITA_GATEWAY_TOKEN`) before calling identity-protected APIs.

## 3-node cluster

See `deploy/docker-compose.yml` and `deploy/.env.example`.

Docs: [getting-started]({{TREMBITA_DOC_BASE}}/docs/getting-started.md) · [scenarios]({{TREMBITA_DOC_BASE}}/docs/scenarios/README.md)
