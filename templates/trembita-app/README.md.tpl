# {{PROJECT_TITLE}}

trembita product app — scaffolded with [`trembita new`](../../docs/decisions/framework-conventions.md).

## Layout

```
src/
  main.rs       boot
  app.rs        TrembitaApp wiring
  config.rs     env config
  consumers/    job handlers
  domain/       business logic (no trembita imports)
```

## Run locally

```bash
cargo run
```

Gateway (when enabled): `http://127.0.0.1:8090` · Admin: `http://127.0.0.1:8080`

Set `GATEWAY_TOKEN` (or `TREMBITA_GATEWAY_TOKEN`) before calling protected APIs.

## 3-node cluster

See `deploy/docker-compose.yml` and `deploy/.env.example`.

Docs: [getting-started](../../docs/getting-started.md) · [scenarios](../../docs/scenarios/README.md)
