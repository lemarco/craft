# Product showcases

Four standalone **product** binaries (plus self-update) — each with its own `Cargo.toml`, README, `cluster.sh` (QUIC/mTLS), and `trigger.sh`. They demonstrate jobs, actors, topics, and workflows on the same [`TrembitaApp`](../crates/trembita/src/app/mod.rs) path as [`trembita new`](../docs/decisions/framework-conventions.md), but use **builder wiring in `main.rs`** instead of the scaffold’s `manifest.rs` layout.

**Dev UX (repo contributors):** build the **debug** CLI, then use `dev` from the repo root — see [getting-started §4](../docs/getting-started.md#4-try-the-showcases). Legacy `./cluster.sh` remains for scripts/CI.

```bash
cargo build -p trembita-cli
./target/debug/trembita dev up --showcase stateful-workers --nodes 3
```

| Showcase | Pattern | Solo | Multi-node (debug CLI) |
|----------|---------|------|------------------------|
| [`background-jobs/`](background-jobs/) | HTTP `202` → queue → `#[consumer]` | `cargo run --release` | `./target/debug/trembita dev up --showcase background-jobs` |
| [`realtime/`](realtime/) | WebSocket + HTTP → sticky `ActorSession` | `cargo run --release` | `./target/debug/trembita dev up --showcase realtime` |
| [`stateful-workers/`](stateful-workers/) | `ActorStateStore` + idempotent cast + auth HTTP | `cargo run --release` | `./target/debug/trembita dev up --showcase stateful-workers` |
| [`workflows/`](workflows/) | Saga journal + actor/queue steps | `cargo run --release` | `./target/debug/trembita dev up --showcase workflows` |
| [`self-update/`](self-update/) | Leader-coordinated rolling self-update | `cargo run --release` | `./target/debug/trembita dev up --showcase self-update` |

## Quick solo run

```bash
./scripts/run-example.sh background-jobs
```

## Docker Compose (CI / demo)

See [`dev/compose/`](../dev/compose/) — not the primary contributor path.

## CI

`./scripts/check-examples.sh` compiles each showcase crate.

More: [scenarios](../docs/scenarios/README.md) · [deployment model](../docs/decisions/deployment-model.md)
