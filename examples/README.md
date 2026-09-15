# Product showcases

Eight standalone projects — product scenarios, WebSocket variants, plus a self-update ops showcase. Each has its own `Cargo.toml` and local `cargo run`.

**Dev UX (preferred):** from the repo root, [`trembita dev`](../crates/trembita-cli/README.md) — `dev setup`, `dev up --showcase … --nodes 3`, `dev trigger …`. Legacy `./cluster.sh` remains for scripts/CI.

Shared helpers: [`trembita-showcase-common`](../crates/trembita-tools/) (env/cluster utilities). HTTP/WS client: [`trembita-showcase-client`](../crates/trembita-tools/) (built by `./cluster.sh setup`).

Excluded from the root workspace `cargo check` (like `benchmarks/`). CI runs `./scripts/check-examples.sh` on pre-push.

| Folder | Pattern | Local | Cluster |
|--------|---------|-------|---------|
| [`background-jobs/`](background-jobs/) | HTTP `202` → queue → `#[consumer]` | `cargo run --release` | `trembita dev up --showcase background-jobs` |
| [`realtime/`](realtime/) | WebSocket + HTTP → sticky `ActorSession` | `cargo run --release` | `trembita dev up --showcase realtime` |
| [`ws-minimal/`](ws-minimal/) | Raw WebSocket echo (no adapters) | `cargo run --release` | — |
| [`market-ws/`](market-ws/) | Open WS + topic broadcast hub | `cargo run --release` | — |
| [`ws-notify/`](ws-notify/) | Identity WS + per-user push | `cargo run --release` | — |
| [`stateful-workers/`](stateful-workers/) | `ActorStateStore` + idempotent cast + auth HTTP | `cargo run --release` | `trembita dev up --showcase stateful-workers` |
| [`workflows/`](workflows/) | Saga journal + actor/queue steps | `cargo run --release` | `trembita dev up --showcase workflows` |
| [`self-update/`](self-update/) | Leader-coordinated rolling self-update | `cargo run --release` | `trembita dev up --showcase self-update` |

## Reading the code

Each showcase `src/main.rs` is heavily commented:

- **What** scenario and data flow diagram in the module doc
- **Why** jobs vs actors vs saga (when to use each mechanism)
- **How** solo vs multi-node (`TREMBITA_JOIN_SEEDS` on nodes 2+; node 1 seed with `TREMBITA_ALLOW_JOIN`)
- **`cluster.sh`** header explains env vars and port layout

Start with [`background-jobs/src/main.rs`](background-jobs/src/main.rs) — the other showcases follow the same pattern.

### Shared cluster env

Every showcase runs the **same binary on every node** — HTTP (product + merged ops routes) + workers/consumers on each VPS. The cluster routes work (Raft leader, queue lease, actor directory); you do not split ingress vs worker roles in the happy path.

- **One port:** `TREMBITA_LISTEN` — UDP (QUIC wire) and TCP (HTTP product + ops) on the **same** `host:port`. `./cluster.sh` sets only `TREMBITA_LISTEN` (`TREMBITA_HTTP=-` on optional QUIC-only nodes).
- Readiness: `RunOpts::default().with_wait_queue("emails")` (or `.with_wait_ready(...)`)
- Optional: homogeneous cluster — same env on every node; see [workload governor](../docs/decisions/workload-governor.md) (`.workload()` compute tokens).

### Internal HTTP/WS client (`trembita-showcase-client`, not on crates.io)

Built automatically by `./cluster.sh setup`:

```bash
./target/debug/trembita-showcase-client job 127.0.0.1:8090 emails hello
./target/debug/trembita-showcase-client cast 127.0.0.1:8190 orders 1001
./target/debug/trembita-showcase-client submit 127.0.0.1:8190 tenant-1 1001
./target/debug/trembita-showcase-client chat 127.0.0.1:8290 alice hello
./target/debug/trembita-showcase-client ws 127.0.0.1:8290 alice hello
./target/debug/trembita-showcase-client workflow run 127.0.0.1:8490 onboard-42
./target/debug/trembita-showcase-client topic 127.0.0.1:8090 orders hello
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

Shared infra: [`dev/`](../dev/README.md) (`cluster-common.sh`, `certs/generate.sh`). Docker Compose per showcase: `dev/compose/<name>/` — **dynamic join** (no `TREMBITA_NODE_ID`; seed + `TREMBITA_JOIN_SEEDS`; ids in `TREMBITA_DATA_DIR/node-id`).

## Related

- [docs/scenarios/README.md](../docs/scenarios/README.md) — product scenario guides
- [docs/getting-started.md](../docs/getting-started.md) — `TrembitaApp` tutorial
- [docs/status.md](../docs/status.md) — capabilities and limits
