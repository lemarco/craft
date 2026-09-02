# Workflows (Meta-Raft saga)

Multi-step onboarding with compensators; journal in `group-meta.redb`.

Uses [`CraftyApp`](../../crates/crafty/src/app.rs) — same onboarding path as the other product showcases.

## What you run

| Piece | Role |
|-------|------|
| This binary | `CraftyApp` + gateway `/workflows/*` |
| [`trigger.sh`](trigger.sh) | Run / resume saga via gateway HTTP |
| Admin | Dashboard **Sagas** panel |

## Quick start (local — two terminals)

**Terminal 1** — start gateway + Raft:

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

Three **identical** nodes — each runs admin + workflow gateway:

```bash
cd examples/workflows
./cluster.sh setup
./cluster.sh up
./cluster.sh health
./trigger.sh onboard-42
./trigger.sh resume onboard-42
```

| Node | QUIC | Admin | Gateway |
|------|------|-------|---------|
| 1 | `:7843` | `:9480` | `:8490` |
| 2 | `:7853` | `:9481` | `:8491` |
| 3 | `:7863` | `:9482` | `:8492` |

Connect to any node's gateway URL. Forward **8490** and **9480** in Cursor/SSH.

Docker Compose: `cd dev/compose/workflows && docker compose up --build`

## Env

| Var | Default | Meaning |
|-----|---------|---------|
| `CRAFTY_GATEWAY` | `127.0.0.1:8490` (local) | Product HTTP bind (`/workflows/run`, `/workflows/resume`) |
| `CRAFTY_DATA_DIR` | `/tmp/crafty-showcase-workflows` | Raft + Meta-Raft redb |
| `CRAFTY_PEERS` | unset | When set → QUIC cluster mode |
| `CRAFTY_GATEWAY_WORKFLOWS` | unset | Set `1` to mount `/workflows/*` when gateway comes from env only |

Guide: [docs/scenarios/workflows.md](../../docs/scenarios/workflows.md)
