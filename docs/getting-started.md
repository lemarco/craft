# Getting started — product apps (no Redis)

Quick path for **product teams** using [`TrembitaApp`](../crates/trembita/src/app/mod.rs) — **capabilities** (`capabilities/` + `#[cap_handler]`), jobs, and durable keys via **`trembita::capstore`** on **embedded redb** (library-first VPS deploy, no mandatory Redis).

**Not the product path:** implementing [`UserActor`](../crates/trembita-runtime/src/registry/actor.rs) or exposing `/actors/*` — advanced only ([product-terminology](decisions/product-terminology.md), default [`without_actors_api`](../crates/trembita/src/configure.rs)).

**Scenarios:** [scenarios/README.md](scenarios/README.md) · **Showcases:** [examples/README.md](../examples/README.md) · **Backlog:** [backlog.md](backlog.md)

## 1. Add dependency

```toml
[dependencies]
trembita = "0.6"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal"] }
```

The default feature set includes `http-jobs` (product HTTP gateway helpers). For local QUIC without PEM files, add `dev-certs`:

```toml
trembita = { version = "0.6", features = ["dev-certs"] }
```

### Optional integrations (facade features)

Enable on the same `trembita` dependency — no separate adapter crates in your `Cargo.toml`. Full reference: [facade ADR](decisions/facade.md).

| Feature | When to enable |
|---------|----------------|
| `http-jobs` (default) | Product gateway, `/jobs/*`, optional `/actors/*` (advanced), `/workflows/*`, custom [`RouteTable`](decisions/gateway-routing-v2.md) + [`cap_*`](decisions/capability-dx.md) |
| `dev-certs` | Solo local seed without operator-provided mTLS PEMs |
| `gateway-auth` | [`trembita-gateway-auth`](../crates/trembita-gateway-auth/) OIDC-shaped helpers → [`SessionIssuer`](../crates/trembita-http/src/routing/session_ports.rs) (B-40) |
| `schedule-postgres` | Postgres [`ScheduleSource`](../crates/trembita-jobs/src/schedule_source.rs) via [`PgScheduleSource`](../crates/trembita-schedule-postgres/) (B-41) |
| `redis-store` | Redis-backed [`ActorStateStore`](decisions/actor-state-redis.md) instead of embedded redb |
| `external-backlog` | Postgres (or compatible) work table as [`ExternalBacklog`](decisions/external-backlog.md) source |
| `domain-outbox` | Postgres transactional outbox → event topic drainer ([event-outbox](decisions/event-outbox.md)) |

```toml
trembita = { version = "0.6", features = ["http-jobs", "dev-certs", "external-backlog"] }
```

```rust
// With `external-backlog`:
use trembita::{PgBacklog, JobOpts, BacklogFeedOpts};
```

## 2. Minimal app

Every process is a **QUIC cluster member**: solo `cargo run` is a one-node seed (`TREMBITA_ALLOW_JOIN=1` by default); add nodes with `TREMBITA_JOIN_SEEDS`. Same binary, same `.run()`.

**Two wiring styles:**

| Style | When | Where capabilities go |
|-------|------|------------------------|
| **Scaffold** (`trembita new`) | Product services with standard layout | [`manifest.rs`](decisions/framework-conventions.md) + thin [`app.rs`](decisions/framework-conventions.md); edit capabilities manually — see [§9](#9-scaffold-a-new-project) |
| **Builder in `main`** | Examples, prototypes, custom layouts | [`.manifest()`](../crates/trembita/src/app/manifest.rs) with [`AppManifest`](../crates/trembita/src/app/manifest.rs) (jobs, topics, **capabilities**); avoid `.workers()` unless advanced |

Single-file minimal (same runtime as scaffold; no `manifest.rs`):

```rust
use std::time::Duration;
use trembita::{JobOpts, RunOpts, TrembitaApp};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    trembita::init_tracing();

    TrembitaApp::from_env()?
        .jobs([JobOpts::new("jobs", Duration::from_secs(60))])
        .run()
        .await
}
```

Equivalent using the registry type (what scaffold generates under the hood):

```rust
use std::time::Duration;
use trembita::{AppManifest, JobOpts, RunOpts, TrembitaApp};

let manifest = AppManifest::new()
    .jobs([JobOpts::new("jobs", Duration::from_secs(60)).http_enqueue(true)]);

TrembitaApp::from_env()?
    .manifest(manifest)
    .run()
    .await?;
```

Wire + HTTP bind to **`TREMBITA_LISTEN`**; ops + `/jobs/*` mount automatically ([`TrembitaApp::from_env`](../crates/trembita/src/app/runtime.rs)). **Scaffolded** apps add product routes in `src/http/product.rs` via [`.gateway_routes()`](../crates/trembita/src/app/builder.rs) in `app.rs`. Host split: [`Gateway::surface_hosts`](../crates/trembita-http/src/gateway/mod.rs) + [`GatewayOpts::from_env()`](../crates/trembita/src/gateway/opts.rs).

With `dev-certs` and no PEM env vars, a solo seed uses ephemeral mTLS automatically.

With `data_dir`, trembita opens `{data_dir}/actor-store.redb`, `{data_dir}/queue-*.redb`, and `{data_dir}/node-id` (assigned id, persisted across restarts).

## 3. Multi-node (env only)

Same code as above. [`cluster.sh`](../examples/background-jobs/cluster.sh) sets env per process — no separate “cluster main”.

**Product env (4–5 variables)** — full reference: **[env.md](env.md)**.

| Variable | Purpose |
|----------|---------|
| `TREMBITA_LISTEN` | One port: QUIC (UDP) + HTTP (TCP) on the same `host:port` |
| `TREMBITA_DATA_DIR` | redb + persisted `node-id` after join |
| `TREMBITA_CERT_DIR` | `ca.pem` + `node-{id}.pem` (or `dev-certs` locally) |
| `TREMBITA_JOIN_SEEDS` | Joiners only: `1@seed:443` |
| `GATEWAY_TOKEN` | Optional Bearer auth for product HTTP |

Optional: `TREMBITA_JOB_QUEUE`, `TREMBITA_ALLOW_JOIN` (seed, default on).

**Do not set** `TREMBITA_NODE_ID` or static `TREMBITA_PEERS` on elastic product clusters. **`TREMBITA_HTTP` / `TREMBITA_GATEWAY`** — internal only (`-` = QUIC-only node); omit otherwise.

**Dynamic join is learner-only by default.** Voter join needs `TREMBITA_JOIN_ROLE=voter` and seed `TREMBITA_ALLOW_VOTER_JOIN=1` ([env.md](env.md#advanced-same-binary-explicit-tuning)). Static voter bootstrap (`TREMBITA_PEERS`) is for `trembita-node` / ops, not typical app compose.

**Ops (zero config):** `/health`, `/ready`, `/metrics`, `/dashboard`, `/introspect/*` on **`TREMBITA_LISTEN`** — enabled with [`TrembitaApp::from_env()`](../crates/trembita/src/app/runtime.rs) until you call [`.without_ops()`](../crates/trembita/src/app/builder.rs).

**Pre-deploy:** from your app repo, run `trembita doctor --preflight` (ports/listen format, cert dir, missing ops/jobs route merges, deprecated env). CI-friendly: non-zero exit when any `[error]` finding.

**Homogeneous nodes:** every VPS runs the same binary (gateway + consumers when configured). Local **API vs jobs** fairness uses [`.workload()`](../crates/trembita/src/app/builder.rs) compute tokens ([workload governor](decisions/workload-governor.md)) — not static node roles. Edge-only ingress without local consumers: omit `.jobs()` / `.workers()` on those nodes (deployment choice), not a role env var.

**Production ingress:** point DNS / floating IP / reverse proxy at each node’s [`TREMBITA_LISTEN`](env.md#trembita_listen); health-check **`GET /ready`** (not `/health` alone for pool membership — B-30). Recipe: [ops/ingress-lb.md](ops/ingress-lb.md).

**Scaling on N VPS (B-28–B-32):** capability groups default to **PerNode** for stateless ops; use **`trembita doctor`** before deploy (B-31); set **`TREMBITA_GATEWAY_SESSION_SECRET`** for cookie login behind LB (B-29); optional sharded queues / multi-Raft via manifest or env (B-32). Index: [status § Product scale wave](status.md#product-scale-wave-b-28b32).

### When to enable coordination growth (B-37)

Use a **preset** when manual B-32 knobs are too low-level. Call [`.with_coordination_growth_preset`](../crates/trembita/src/configure.rs) on [`TrembitaConfigure`](../crates/trembita/src/configure.rs) **before** [`.manifest()`](../crates/trembita/src/app/manifest.rs), or set **`TREMBITA_COORDINATION_PROFILE`** for env-only queue boot (`TREMBITA_JOB_QUEUE` + `TREMBITA_DATA_DIR`).

| Profile | Enable when | Effect |
|---------|-------------|--------|
| `standard` (default) | Solo node or modest backlog | Single Raft group, standard queue layout |
| `jobs_backlog` | Job depth grows on every node but enqueue is the bottleneck | Leader **auto-shard** on standard queues (depth **256**, max **16** physical shards) |
| `write_sharding` | Keyed cap store / topics / coordination hot spots hit **R1** on one Raft group | **2** coordination Raft groups, **64** virtual shards |
| `full` | Large deployments with both backlog and keyed coordination pressure | Auto-shard (**512** / **12** shards) + multi-Raft |

**Env aliases:** `jobs`, `jobs-backlog`, `write`, `sharding`, `growth` — see [capabilities § B-37](scenarios/capabilities.md#coordination-growth-presets-b-37). Explicit `TREMBITA_RAFT_GROUPS`, `TREMBITA_RAFT_SHARD_COUNT`, and `TREMBITA_JOB_QUEUE_AUTO_SHARD` **override** the profile.

After deploy, confirm with **`GET /introspect/product-scale`** (or boot log `trembita::product_scale=info`) — [production-runbook § B-37](ops/production-runbook.md#coordination-growth-preset-b-37).

**Regression:** `./scripts/test-fast.sh -p trembita --test coordination_growth_preset b37_` — full matrix in [capabilities § Automated regression (B-37)](scenarios/capabilities.md#automated-regression-b-37).

## 4. Try the showcases

From the **trembita repo root** (set `TREMBITA_ROOT` if needed):

```bash
cargo build -p trembita-cli   # debug CLI (`dev` is omitted from `--release` / crates.io install)
./target/debug/trembita dev list
./target/debug/trembita dev setup --showcase stateful-workers
./target/debug/trembita dev up --showcase stateful-workers --nodes 3
./target/debug/trembita dev trigger stateful-workers -- 1001
./target/debug/trembita dev stop --showcase stateful-workers
```

| Showcase | Pattern |
|----------|---------|
| [background-jobs](../examples/background-jobs/) | Durable job queue |
| [stateful-workers](../examples/stateful-workers/) | Capabilities + idempotent store (+ advanced migration demo) |
| [realtime](../examples/realtime/) | Sticky sessions / WebSocket |
| [workflows](../examples/workflows/) | Saga journal + steps |

Solo node: `cargo run --release` inside `examples/<name>/`. Multi-node scripts: `./cluster.sh` in each example. **Compose** under [`dev/compose/`](../dev/compose/) is for CI/demo — not the primary dev path.

Internal HTTP/WS client (not on crates.io; built by `./cluster.sh setup` or `trembita dev setup`):

```bash
cargo build -p trembita-tools --bin trembita-showcase-client
./target/debug/trembita-showcase-client job 127.0.0.1:8090 emails hello
./target/debug/trembita-showcase-client ws 127.0.0.1:8290 alice hello
```

Prefer `./target/debug/trembita dev http --showcase background-jobs -- job emails hello` when using the debug CLI from the repo root.

### Local 3-node cluster (B-39)

Exercise **multi-node gateway** with a shared **`TREMBITA_GATEWAY_SESSION_SECRET`** (default **`realtime`** on **8290–8292**):

```bash
./scripts/local-cluster.sh setup
./scripts/local-cluster.sh up
./scripts/local-cluster.sh session-smoke          # login node1 → /me node2
./scripts/local-cluster.sh lb-up                  # nginx :18290 (Docker)
./scripts/local-cluster.sh session-smoke --lb
./scripts/local-cluster.sh stop
```

Debug CLI (repo checkout only): `./target/debug/trembita dev cluster-up --setup` · `dev cluster-lb-up`.

Docs: [dev/local-3node](../dev/local-3node/README.md) · [capabilities § B-39](scenarios/capabilities.md#local-3-node-cluster-b-39). Regression: `./scripts/test-fast.sh -p trembita-cli --lib b39_`.

**Local elastic (B-42)** — same proof story as [B-34](scenarios/capabilities.md#elastic-join--lb-b-34) without Docker E2E containers (**realtime**, ports **8290–8293**):

```bash
./scripts/local-cluster.sh setup
./scripts/local-cluster.sh elastic-up
./scripts/local-cluster.sh lb-up
./scripts/local-cluster.sh elastic-smoke
./scripts/local-cluster.sh stop
```

Regression: `./scripts/test-fast.sh -p trembita-cli --lib b42_` · `./scripts/test-fast.sh -p trembita-tools --lib b42_`. Heavy CI lane: [`e2e/elastic_lb.sh`](../e2e/elastic_lb.sh).

Reference KV [`StateMachine`](../crates/trembita-core/src/kv.rs) (`trembita::kv` on the facade) for low-level Raft `propose` / `query` without a full product app.

## 5. Product workers

### Capabilities (recommended)

Register ops in `capabilities/` + [`CapManifest`](decisions/capability-dx.md), call with `.via(&app)` — **prefer [`Route::Queued`](../crates/trembita/src/capability/route.rs) + `.default_queue_for::<YourReq>()`** on the group for durable work (same handler as inline; bridge idempotency when `data_dir` is set). HTTP: [`cap_fire` / `cap_invoke` / `cap_enqueue` / `cap_queued_wait` / `cap_schedule`](decisions/capability-dx.md#http-wave-2). Handlers use [`OpCtx::require_store()`](../crates/trembita/src/capability/ctx.rs) and [`.cap_deps(AppDeps)`](../crates/trembita/src/app/builder.rs) → [`OpCtx::deps`](../crates/trembita/src/capability/ctx.rs) for idempotency and injected ports. Policy: [capability-greenfield-wire](decisions/capability-greenfield-wire.md). Guide: [scenarios/capabilities.md](scenarios/capabilities.md).

**Advanced — `consumers/` only:** raw [`#[consumer]`](../crates/trembita-macros/src/lib.rs) streams without a matching capability op, or advanced queue→actor bridges (prefer capability ops + `Route::Queued`). New backlog work should be a capability op + optional `Route::Queued`, not a standalone consumer module.

Scaffolded apps ship sample `POST /ping` → inline `app.ping` in `src/http/product.rs`.

### Durable mailbox + leader tasks (B-41)

**Most apps skip this** — prefer capabilities + queued routes. Enable when you use advanced [`UserActor`](../crates/trembita-runtime/src/registry/actor.rs) and cross-node `/actor/deliver` ([protocol § mailbox spool](protocol.md#actor-mailbox-spool-durable-delivery)).

Cross-node actor delivery with redb spool (`{data_dir}/mailbox-spool.redb`):

```rust
TrembitaApp::builder().configure(
    TrembitaConfigure::default()
        .with_data_dir("/var/lib/trembita")
        .with_durable_mailbox(true),
);
// or: .with_durable_mailbox(true) on the builder (same flag)
```

Leader-only side work ([leader-task § B-41](decisions/leader-task.md#product-surface-b-41)):

```rust
use std::time::Duration;
use trembita::{LeaderLoopOpts, TrembitaApp, TrembitaConfigure};

TrembitaApp::builder()
    .configure(TrembitaConfigure::default().with_data_dir("/var/lib/trembita"))
    .on_leader(
        LeaderLoopOpts::new(Duration::from_secs(30)).run_on_acquire(),
        |gate| async move {
            if gate.first_in_term() {
                // one-shot after election
            }
        },
    );
```

Postgres-backed recurring jobs — feature **`schedule-postgres`**, [`PgScheduleSource`](../crates/trembita-schedule-postgres/src/lib.rs) on [`AppManifest::schedule_source`](../crates/trembita/src/app/manifest.rs) ([schedule-source § B-41](decisions/schedule-source.md#postgres-adapter-b-41)).

**Regression:** [capabilities § B-41](scenarios/capabilities.md#product-surface-gaps-b-41) · `./scripts/test-fast.sh -p trembita --test app_cluster b41_`.

### Advanced — `UserActor`

Realtime, migration demos, custom mailbox protocols. Register with [`.workers()`](../crates/trembita/src/worker_opts.rs) and explicit [`WorkerScale`](../crates/trembita/src/worker_opts.rs) (`Fixed`, `PerNode`, or queue-driven `Auto`):

```rust
use trembita::runtime::{UserActor, actor};
use trembita::{TrembitaApp, RunOpts, WorkerOpts, WorkerScale, workers};

struct EmailWorker;

#[actor]
impl UserActor for EmailWorker {
    type Config = ();
    type Message = Vec<u8>;
    type Error = std::convert::Infallible;
    // …
}

TrembitaApp::builder()
    .data_dir("/var/lib/trembita")
    .workers(workers!(
        WorkerOpts::<EmailWorker>::new("email")
            .config(())
            .scale(WorkerScale::PerNode),
    ))
    .run(RunOpts::default())
    .await?;
```

Alternate API: [`.actors()`](../crates/trembita/src/app/mod.rs) + [`ActorGroupOpts`](../crates/trembita/src/actor_group.rs).

Stateful workflow keys: use [`OpCtx::cap_store()`](../crates/trembita/src/capability/ctx.rs) / [`trembita::capstore`](../crates/trembita/src/capstore.rs) with `store_get` / `store_set` — backed by redb when `data_dir` is set.

## 6. HTTP job enqueue (optional)

Prefer [`.jobs()`](../crates/trembita/src/job_opts.rs) to register queue + consumer + HTTP enqueue in one call. Enable the `http-jobs` feature (default on the facade):

```toml
trembita = { version = "0.6", features = ["http-jobs"] }
```

```rust
use std::time::Duration;
use trembita::{TrembitaApp, GatewayOpts, JobOpts, RunOpts, consumer};

#[consumer("jobs")]
async fn handle_job(_payload: &[u8]) -> Result<(), ()> {
    Ok(())
}

// QUIC wire and product TCP must share TREMBITA_LISTEN (see env.md).
TrembitaApp::builder()
    .data_dir("/var/lib/trembita")
    .jobs([JobOpts::new("jobs")
        .lease(Duration::from_secs(300))
        .consumer(&HandleJobConsumer)
        .http_enqueue(true)])
    .gateway(GatewayOpts::from_env()?)
    .run(RunOpts::default().with_wait_queue("jobs"))
    .await?;

// POST /jobs/{stream} → 202 { "job_id": … }
```

Lower-level [`.queue()`](../crates/trembita/src/app/mod.rs) + [`.consumer()`](../crates/trembita/src/app/mod.rs) remain available. See [facade features](decisions/facade.md) and [background-jobs](scenarios/background-jobs.md).

## 7. Workflows (sagas)

Build a named plan with [`WorkflowBuilder`](../crates/trembita/src/workflow.rs) and run it on the cluster journal:

```rust
use trembita::{WorkflowBuilder, TrembitaApp};

let plan = WorkflowBuilder::new("onboard-user")
    .step("create_account", &key, payload)
    .compensate("create_account", undo_payload)
    .build()?;

app.run_workflow(&plan).await?;
```

Example: `./scripts/run-example.sh workflows`.

## 8. Real-time gateway

WebSocket helpers live in `trembita::` (re-export `tokio_tungstenite` / `futures_util`). Use **raw** upgrade, **sticky** sessions, or **broadcast / notify** hubs — see [`examples/`](../examples/README.md).

```rust
use std::time::Duration;
use trembita::{
    AuthMode, Gateway, GatewayOpts, RouteTable, TrembitaGatewayState,
    mount_sticky_websocket, server_stream, WsMessage,
};

fn gateway_surfaces(state: TrembitaGatewayState) -> Gateway {
    let table = mount_sticky_websocket::<Append>(
        RouteTable::new(),
        "/ws",
        AuthMode::Identity,
        state,
        Some(Duration::from_secs(3600)),
        |sticky| Box::pin(async move {
            let mut ws = server_stream(sticky.stream).await;
            let _ = ws.send(WsMessage::Text("connected".into())).await;
            // cast via sticky.handle, or custom protocol
        }),
    );
    Gateway::new(false).dev_fallback(table)
}

// Set TREMBITA_LISTEN=127.0.0.1:8090 (or use TrembitaApp::from_env()?) so wire matches the gateway bind.
TrembitaApp::builder()
    .gateway(
        GatewayOpts::from_env()?
            .identity(MyAuth)
            .surfaces(gateway_surfaces),
    );
// Or: GatewayOpts::realtime_ws("/ws", "chat", Duration::from_secs(3600), |sticky| …)
// Scaffold: add src/http/ws.rs and merge routes in app.rs (see docs/scenarios/websocket-wiring.md)
```

Showcases:

```bash
cd examples/realtime && cargo run --release    # sticky + HTTP session
cd examples/market-ws && cargo run --release   # open broadcast
cd examples/ws-notify && cargo run --release   # identity push
cd examples/ws-minimal && cargo run --release  # raw echo
```

See [WebSocket wiring](scenarios/websocket-wiring.md), [realtime-sessions](scenarios/realtime-sessions.md), and [gateway-identity](decisions/gateway-identity.md).

## 9. Scaffold a new project

```bash
# From the trembita repo (path dependency — init script resolves an absolute repo root):
./scripts/trembita-init.sh my-app
# or (use an absolute --trembita-path if the project lives outside the repo):
cargo run -p trembita-cli -- new my-app --trembita-path "$(pwd)"

# With feature selection:
cargo run -p trembita-cli -- new my-app \
  --features jobs,gateway,telemetry,topics,external-backlog
```

Generates the [framework layout](decisions/framework-conventions.md):

| Path | Role |
|------|------|
| `main.rs` | Boot only — `App::new(config::from_env()?).run().await` + tracing init |
| `manifest.rs` | [`manifest::build()`](../crates/trembita/src/app/manifest.rs) → [`AppManifest`](../crates/trembita/src/app/manifest.rs) (jobs, capabilities; `// trembita:*` marker regions for topics/workers) |
| `app.rs` | [`TrembitaApp::from_config`](../crates/trembita/src/app/builder.rs), [`.manifest(manifest::build())`](../crates/trembita/src/app/builder.rs), [`.configure(TrembitaConfigure::…)`](../crates/trembita/src/configure.rs), [`.without_actors_api()`](../crates/trembita/src/app/builder.rs), [`.cap_deps`](../crates/trembita/src/capability/deps.rs), [`.gateway_routes()`](../crates/trembita/src/app/builder.rs), `.run()` |
| `capabilities/`, `consumers/`, `http/`, `domain/` | Typed ops, `#[consumer]` handlers, `cap_*` in `http/product.rs`, hexagon |
| `actors/` | Optional (`--features actors`) — advanced `UserActor` only |
| `deploy/` | `.env.example` + optional `docker-compose.yml` for local cluster |

**Profiles (B-38):** `--profile jobs|realtime|api` is an alias for `--template` — see [capabilities § B-38](scenarios/capabilities.md#scale-scaffold-dx-b-38). The **jobs** profile adds **`src/capabilities/task.rs`** (queued + `require_store` idempotency sample).

Add capabilities by editing **`src/manifest.rs`** (`// trembita:capabilities` region) and **`src/capabilities/`**,
wire HTTP in **`src/http/product.rs`** ([`cap_invoke`](../crates/trembita/src/gateway/cap_handlers.rs)), add job handlers under `consumers/`. Then:

```bash
trembita doctor --explain-scale   # product scale narrative (B-38; no layout lint)
trembita doctor                   # manifest ↔ files consistency (read-only)
cargo check       # greenfield scaffold should compile (sample job + /ping cap route)
```

## 10. Observability & ops

| Need | How |
|------|-----|
| Live dashboard | Automatic with [`TrembitaApp::from_env()`](../crates/trembita/src/app/runtime.rs) on `TREMBITA_LISTEN`; brownfield apps merge [`OpsApi::route_table()`](../crates/trembita-http/src/ops_routes.rs) in [`GatewayOpts::surfaces`](../crates/trembita/src/gateway/opts.rs) (see scaffold `src/http/ops.rs`) |
| Queue / workflow panels | Dashboard polls `/introspect/queues` and `/introspect/sagas` |
| Prometheus | Scrape `GET /metrics` (includes `trembita_queue_*`, `trembita_saga_*`) |
| Push export | `.metrics_sink(Arc::new(my_sink))` on [`TrembitaAppBuilder`](../crates/trembita/src/app/mod.rs) — see [`MetricsSink`](../crates/trembita-dashboard/src/metrics_sink.rs) |
| Live events | `cluster.events().subscribe()` — forward [`TrembitaEvent`](../crates/trembita-dashboard/src/telemetry.rs) to your sink |
| Production checklist | [ops/production-runbook.md](ops/production-runbook.md) |
| Ingress / load balancing | [ops/ingress-lb.md](ops/ingress-lb.md) |

## 11. Cluster APIs

Most apps stay on `TrembitaApp`. For custom state machines, multi-Raft, or direct supervisor/queue access, use [`trembita::cluster`](../crates/trembita/src/cluster.rs) or the [`TrembitaApp`](../crates/trembita/src/app/mod.rs) methods (`control`, `registry`, `supervisor`, …).

| Need | Doc |
|------|-----|
| Background jobs | [scenarios/background-jobs.md](scenarios/background-jobs.md) |
| Event topics | [scenarios/event-topics.md](scenarios/event-topics.md) |
| Capabilities (typed ops) | [scenarios/capabilities.md](scenarios/capabilities.md) |
| Stateful workers | [scenarios/stateful-workers.md](scenarios/stateful-workers.md) |
| Sessions / WebSocket | [scenarios/realtime-sessions.md](scenarios/realtime-sessions.md) |
| Workflows | [scenarios/workflows.md](scenarios/workflows.md) |
| Workload governor | [decisions/workload-governor.md](decisions/workload-governor.md) |

## Related

- [deployment-model](decisions/deployment-model.md)
- [product-scenarios](decisions/product-scenarios.md)
- [facade](decisions/facade.md)
- [actor-state-store](decisions/actor-state-store.md)
- [ops/production-runbook.md](ops/production-runbook.md)
