# Product showcases

Four standalone projects — one per [product scenario](../docs/scenarios/README.md). Each has its own `Cargo.toml`, README, `cluster.sh` (QUIC/mTLS), and `trigger.sh`.

Shared helpers: [`crafty-showcase-common`](../crates/crafty-showcase-common/) (env/cluster utilities). HTTP/WS client: [`crafty-showcase-client`](../crates/crafty-showcase-client/) (built by `./cluster.sh setup`).

Excluded from the root workspace `cargo check` (like `benchmarks/`). CI runs `./scripts/check-examples.sh` on pre-push.

| Folder | Tier | Pattern | Local | Cluster |
|--------|------|---------|-------|---------|
| [`background-jobs/`](background-jobs/) | C | HTTP `202` → queue → `#[consumer]` | `cargo run --release` | `./cluster.sh up` |
| [`realtime/`](realtime/) | B | WebSocket + HTTP → sticky `ActorSession` | `cargo run --release` | `./cluster.sh up` |
| [`stateful-workers/`](stateful-workers/) | B | `ActorStateStore` + idempotent cast + auth HTTP | `cargo run --release` | `./cluster.sh up` |
| [`workflows/`](workflows/) | coordination | Saga journal + actor/queue steps | `cargo run --release` | `./cluster.sh up` |
| [`self-update/`](self-update/) | ops | Leader-coordinated rolling self-update | `cargo run --release` | `./cluster.sh up` |

## Reading the code

Each showcase `src/main.rs` is heavily commented:

- **What** tier (A/B/C) and data flow diagram in the module doc
- **Why** that tier vs the others (jobs vs actors vs saga)
- **How** solo vs multi-node (`CRAFTY_JOIN_SEEDS` on nodes 2+; node 1 seed with `CRAFTY_ALLOW_JOIN`)
- **`cluster.sh`** header explains env vars and port layout

Start with [`background-jobs/src/main.rs`](background-jobs/src/main.rs) — the other three follow the same pattern.

### Shared cluster env

Every showcase runs the **same binary on every node** — gateway + workers/consumers on each VPS. The cluster routes work (Raft leader, queue lease, actor directory); you do not split ingress vs worker roles in the happy path.

- Readiness: `RunOpts::default().with_wait_queue("emails")` (or `.with_wait_ready(...)`)
- **Advanced:** the library still supports `CRAFTY_ROLE=gateway|worker|both` for production edge-only nodes — not used by default in these showcases

### Internal HTTP/WS client (`crafty-showcase-client`, not on crates.io)

Built automatically by `./cluster.sh setup`:

```bash
./target/debug/crafty-showcase-client job 127.0.0.1:8090 emails hello
./target/debug/crafty-showcase-client cast 127.0.0.1:8190 orders 1001
./target/debug/crafty-showcase-client submit 127.0.0.1:8190 tenant-1 1001
./target/debug/crafty-showcase-client chat 127.0.0.1:8294 alice hello
./target/debug/crafty-showcase-client ws 127.0.0.1:8294 alice hello
./target/debug/crafty-showcase-client workflow run 127.0.0.1:8490 onboard-42
```

`trigger.sh` scripts use this binary when present, otherwise fall back to `curl` / `websocat`.

### Docker Compose (all showcases)

| Showcase | Command |
|----------|---------|
| background-jobs | `cd dev/compose/background-jobs && docker compose up --build` |
| stateful-workers | `cd dev/compose/stateful-workers && docker compose up --build` |
| realtime | `cd dev/compose/realtime && docker compose up --build` |
| workflows | `cd dev/compose/workflows && docker compose up --build` |

### QUIC migration demo (stateful-workers)

```bash
cd examples/stateful-workers
./cluster.sh setup
./cluster.sh 1-migrate   # terminal 1
./cluster.sh 2-migrate   # terminal 2
./cluster.sh migrate-run # POST /demo/migrate/run on node 1
```

## Quick start (local)

```bash
cd examples/background-jobs && cargo run --release
# another terminal:
./trigger.sh hello
```

Or:

```bash
./scripts/run-example.sh background-jobs
./scripts/run-example.sh stateful-workers
./scripts/run-example.sh realtime
./scripts/run-example.sh workflows
```

## Quick start (3-node cluster)

```bash
cd examples/<showcase>
./cluster.sh setup
./cluster.sh up          # background all 3 nodes
./cluster.sh health
./trigger.sh             # or trigger-batch.sh where provided
./cluster.sh logs 2      # optional: tail node 2
./cluster.sh stop
```

Manual terminals still work: `./cluster.sh 1`, `./cluster.sh 2`, `./cluster.sh 3`.

Certs and shared dev infra: [`dev/`](../dev/) (`dev/certs/`, `dev/cluster-common.sh`).

### Ports (defaults, no overlap between showcases)

| Showcase | Gateway / WS / Trigger | Admin (node 1) |
|----------|------------------------|----------------|
| background-jobs | HTTP `:8090–8092` | `:9180` |
| stateful-workers | HTTP `:8190–8192` | `:9280` |
| realtime | HTTP + WS `:8294–8296` | `:9380` |
| workflows | HTTP trigger `:8490–8492` | `:9480` |

Forward gateway/WS/trigger + admin in Cursor/SSH when developing remotely.

## Debug logging

All showcases emit structured **`tracing`** events on target `showcase`:

```bash
# local
RUST_LOG=showcase=debug cargo run --release

# cluster (default filter already includes showcase=debug)
./cluster.sh up
```

Key events: startup config, cluster readiness polls, worker/saga/ws handling, shutdown.
Set `RUST_LOG=showcase=trace` for maximum verbosity.
