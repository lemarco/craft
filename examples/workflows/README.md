# Workflows (Meta-Raft saga)

Multi-step onboarding with compensators; journal in `group-meta.redb`.

Uses [`TrembitaApp`](../../crates/trembita/src/app/mod.rs) — same onboarding path as the other product showcases.

## What you run

| Piece | Role |
|-------|------|
| This binary | `TrembitaApp` + explicit `/workflows/*` + ops routes on one HTTP listener |
| [`trigger.sh`](trigger.sh) | Run / resume saga via gateway HTTP |
| Ops (same listener) | `/dashboard` (Sagas panel), `/health`, `/metrics` |

## Quick start (local — two terminals)

**Terminal 1** — start HTTP + Raft:

```bash
cd examples/workflows
cargo run --release
```

**Terminal 2** — trigger saga:

```bash
./trigger.sh onboard-42
./trigger.sh resume onboard-42
```

CLI-only (no HTTP server):

```bash
cargo run --release -- run onboard-42
cargo run --release -- resume onboard-42
```

## Quick start (cluster)

Three **identical** nodes — each runs workflows + ops on the same HTTP port:

```bash
cd examples/workflows
./cluster.sh setup
./cluster.sh up
./cluster.sh health
./trigger.sh onboard-42
./trigger.sh resume onboard-42
```

| Node | `TREMBITA_LISTEN` (wire + HTTP) |
|------|----------------------------------|
| 1 | `:8490` |
| 2 | `:8491` |
| 3 | `:8492` |

Connect to any node's HTTP URL. Forward **8490** (or 8491/8492) in Cursor/SSH.

Docker Compose: `cd dev/compose/workflows && docker compose up --build`

## Env

| Var | Default | Meaning |
|-----|---------|---------|
| `TREMBITA_LISTEN` | `127.0.0.1:8490` | One port — QUIC + HTTP workflows + ops |
| `TREMBITA_JOIN_SEEDS` | unset | Joiners: `1@127.0.0.1:8490` (see `./cluster.sh`) |
| `TREMBITA_DATA_DIR` | `/tmp/trembita-showcase-workflows` | Raft + Meta-Raft redb + `node-id` |
| `GATEWAY_TOKEN` | unset | When set, `/workflows/*` requires `Authorization: Bearer` + `X-Trembita-User` ([`AuthMode::Identity`](../../crates/trembita-http/src/routing/auth.rs)) |

Workflow and ops route tables are merged explicitly in `src/main.rs` (no `with_workflows_api` env flags).

Guide: [docs/scenarios/workflows.md](../../docs/scenarios/workflows.md)
