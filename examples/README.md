# Product showcases

Four standalone projects — one per [product scenario](../docs/scenarios/README.md), plus a fifth ops showcase. Each has its own `Cargo.toml`, README, `cluster.sh` (QUIC/mTLS), and `trigger.sh`.

Shared helpers: [`crafty-showcase-common`](../crates/crafty-showcase-common/) (env/cluster utilities). HTTP/WS client: [`crafty-showcase-client`](../crates/crafty-showcase-client/) (built by `./cluster.sh setup`).

Excluded from the root workspace `cargo check` (like `benchmarks/`). CI runs `./scripts/check-examples.sh` on pre-push.

| Folder | Pattern | Local | Cluster |
|--------|---------|-------|---------|
| [`background-jobs/`](background-jobs/) | HTTP `202` → queue → `#[consumer]` | `cargo run --release` | `./cluster.sh up` |
| [`realtime/`](realtime/) | WebSocket + HTTP → sticky `ActorSession` | `cargo run --release` | `./cluster.sh up` |
| [`stateful-workers/`](stateful-workers/) | `ActorStateStore` + idempotent cast + auth HTTP | `cargo run --release` | `./cluster.sh up` |
| [`workflows/`](workflows/) | Saga journal + actor/queue steps | `cargo run --release` | `./cluster.sh up` |
| [`self-update/`](self-update/) | Leader-coordinated rolling self-update | `cargo run --release` | `./cluster.sh up` |

## Reading the code

Each showcase `src/main.rs` is heavily commented:

- **What** scenario and data flow diagram in the module doc
- **Why** jobs vs actors vs saga (when to use each mechanism)
- **How** solo vs multi-node (`CRAFTY_JOIN_SEEDS` on nodes 2+; node 1 seed with `CRAFTY_ALLOW_JOIN`)
- **`cluster.sh`** header explains env vars and port layout

Start with [`background-jobs/src/main.rs`](background-jobs/src/main.rs) — the other three follow the same pattern.

### Shared cluster env

Every showcase runs the **same binary on every node** — gateway + workers/consumers on each VPS. The cluster routes work (Raft leader, queue lease, actor directory); you do not split ingress vs worker roles in the happy path.

- Readiness: `RunOpts::default().with_wait_queue("emails")` (or `.with_wait_ready(...)`)
- Optional: `CRAFTY_ROLE=gateway|worker|both` for edge-only nodes — not used by default in these showcases

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
./scripts/run-example.sh self-update
```

3-node QUIC cluster (any showcase):

```bash
cd examples/background-jobs
./cluster.sh setup && ./cluster.sh up && ./trigger.sh hello
```

Shared infra: [`dev/`](../dev/README.md) (`cluster-common.sh`, `certs/generate.sh`). Docker Compose per showcase: `dev/compose/<name>/`.

## Related

- [docs/scenarios/README.md](../docs/scenarios/README.md) — product scenario guides
- [docs/getting-started.md](../docs/getting-started.md) — `CraftyApp` tutorial
- [docs/status.md](../docs/status.md) — capabilities and limits
