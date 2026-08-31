# Workflows (Meta-Raft saga)

Multi-step onboarding with compensators; journal in `group-meta.redb`.

## What you run

| Piece | Role |
|-------|------|
| This binary | 3-node `KvMachine` cluster + Meta-Raft saga journal |
| [`trigger.sh`](trigger.sh) | Run saga (local CLI or cluster HTTP) |
| Admin | Dashboard **Sagas** panel |

## Quick start (local — one terminal)

In-process 3-member cluster (fastest way to see journal + resume):

```bash
cd examples/workflows
cargo run --release
# same as: cargo run --release -- run onboard-42
```

Resume (same `data_dir`):

```bash
cargo run --release -- resume onboard-42
# or: ./trigger.sh resume onboard-42   # falls back to local when no trigger HTTP
```

## Quick start (cluster)

Three **identical** nodes — each runs admin + workflow trigger HTTP:

```bash
cd examples/workflows
./cluster.sh setup
./cluster.sh up
./cluster.sh health
./trigger.sh onboard-42
./trigger.sh resume onboard-42   # idempotent when already completed
```

| Node | QUIC | Admin | Trigger HTTP |
|------|------|-------|--------------|
| 1 | `:7843` | `:9480` | `:8490` |
| 2 | `:7853` | `:9481` | `:8491` |
| 3 | `:7863` | `:9482` | `:8492` |

Connect to any node's trigger URL. Forward **8490** and **9480** in Cursor/SSH. Watch the **Sagas** table on the dashboard after `./trigger.sh`.

Docker Compose: `cd dev/compose/workflows && docker compose up --build`

## Env

| Var | Default | Meaning |
|-----|---------|---------|
| `CRAFTY_DATA_DIR` | `/tmp/crafty-showcase-workflows` | Raft + Meta-Raft redb |
| `CRAFTY_PEERS` | unset | When set → QUIC cluster server mode |
| `CRAFTY_TRIGGER` | unset | Workflow HTTP bind (`/workflows/run`, `/workflows/resume`) |

Guide: [docs/scenarios/workflows.md](../../docs/scenarios/workflows.md)
