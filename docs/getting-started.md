# Getting started — product apps (no Redis)

Quick path for **product teams** using [`TrembitaApp`](../crates/trembita/src/app/mod.rs) — actors, jobs, and durable workflow keys on **embedded redb**, no Kubernetes, no mandatory Redis.

**Scenarios:** [scenarios/README.md](scenarios/README.md) · **Showcases:** [examples/README.md](../examples/README.md) · **Backlog:** [backlog.md](backlog.md)

## 1. Add dependency

```toml
[dependencies]
trembita = "0.3"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal"] }
```

The default feature set includes `http-jobs` (product HTTP gateway helpers). For local QUIC without PEM files, add `dev-certs`:

```toml
trembita = { version = "0.3", features = ["dev-certs"] }
```

### Optional integrations (facade features)

Enable on the same `trembita` dependency — no separate adapter crates in your `Cargo.toml`. Full reference: [facade ADR](decisions/facade.md).

| Feature | When to enable |
|---------|----------------|
| `http-jobs` (default) | Product gateway, `/jobs/*`, `/actors/*`, `/workflows/*`, custom [`RouteTable`](decisions/gateway-routing-v2.md) |
| `dev-certs` | Solo local seed without operator-provided mTLS PEMs |
| `redis-store` | Redis-backed [`ActorStateStore`](decisions/actor-state-redis.md) instead of embedded redb |
| `external-backlog` | Postgres (or compatible) work table as [`ExternalBacklog`](decisions/external-backlog.md) source |
| `domain-outbox` | Postgres transactional outbox → event topic drainer ([event-outbox](decisions/event-outbox.md)) |

```toml
trembita = { version = "0.3", features = ["http-jobs", "dev-certs", "external-backlog"] }
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
| **Builder in `main`** | Examples, prototypes, custom layouts | [`.jobs()` / `.topics()` / `.workers()`](../crates/trembita/src/app/builder.rs) on [`TrembitaAppBuilder`](../crates/trembita/src/app/mod.rs), or [`.manifest()`](../crates/trembita/src/app/manifest.rs) with [`AppManifest`](../crates/trembita/src/app/manifest.rs) |

Single-file minimal (same runtime as scaffold; no `manifest.rs`):

```rust
use std::time::Duration;
use trembita::{JobOpts, RunOpts, TrembitaApp};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    trembita::init_tracing();

    TrembitaApp::from_env()?
        .jobs([JobOpts::new("jobs", Duration::from_secs(60)).http_enqueue(true)])
        .run(RunOpts::from_env()?)
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
    .run(RunOpts::from_env()?)
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

**Homogeneous nodes:** every VPS runs the same binary (gateway + consumers when configured). Local **API vs jobs** fairness uses [`.workload()`](../crates/trembita/src/workload.rs) compute tokens ([workload governor](decisions/workload-governor.md)) — not static node roles. Edge-only ingress without local consumers: omit `.jobs()` / `.workers()` on those nodes (deployment choice), not a role env var.

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
| [stateful-workers](../examples/stateful-workers/) | Stateful actors + migration |
| [realtime](../examples/realtime/) | Sticky sessions / WebSocket |
| [workflows](../examples/workflows/) | Saga journal + steps |

Solo node: `cargo run --release` inside `examples/<name>/`. Legacy: `./cluster.sh` in each example. **Compose** under [`dev/compose/`](../dev/compose/) is for CI/demo — not the primary dev path.

Internal HTTP/WS client (not on crates.io; built by `./cluster.sh setup`):

```bash
cargo build -p trembita-showcase-client
./target/debug/trembita-showcase-client job 127.0.0.1:8090 emails hello
./target/debug/trembita-showcase-client ws 127.0.0.1:8290 alice hello
```

Reference KV [`StateMachine`](../crates/trembita-core/src/kv.rs) (`trembita::kv` on the facade) for low-level Raft `propose` / `query` without a full product app.

## 5. Workers (actors)

Register worker types with [`.workers()`](../crates/trembita/src/worker_opts.rs) and explicit [`WorkerScale`](../crates/trembita/src/worker_opts.rs) (`Fixed`, `PerNode`, or queue-driven `Auto`):

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

Legacy [`.actors()`](../crates/trembita/src/app/mod.rs) + [`ActorGroupOpts`](../crates/trembita/src/actor_group.rs) remain supported.

Stateful workflow keys: use `app.actor_state_store()` with [`store_get` / `store_set`](../crates/trembita-actor-store/src/store_codec.rs) — backed by redb when `data_dir` is set.

## 6. HTTP job enqueue (optional)

Prefer [`.jobs()`](../crates/trembita/src/job_opts.rs) to register queue + consumer + HTTP enqueue in one call. Enable the `http-jobs` feature (default on the facade):

```toml
trembita = { version = "0.3", features = ["http-jobs"] }
```

```rust
use std::time::Duration;
use trembita::{TrembitaApp, GatewayOpts, JobOpts, RunOpts, consumer};

#[consumer("jobs")]
async fn handle_job(_payload: &[u8]) -> Result<(), ()> {
    Ok(())
}

TrembitaApp::builder()
    .data_dir("/var/lib/trembita")
    .jobs([JobOpts::new("jobs")
        .lease(Duration::from_secs(300))
        .consumer(&HandleJobConsumer)
        .http_enqueue(true)])
    .gateway(GatewayOpts::new("0.0.0.0:3000".parse()?))
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
    let table = mount_sticky_websocket(
        RouteTable::new(),
        "/ws",
        AuthMode::Identity,
        state,
        "chat",
        Some(Duration::from_secs(3600)),
        |sticky| Box::pin(async move {
            let mut ws = server_stream(sticky.stream).await;
            let _ = ws.send(WsMessage::Text("connected".into())).await;
            // cast via sticky.handle, or custom protocol
        }),
    );
    Gateway::new(false).dev_fallback(table)
}

TrembitaApp::builder()
    .gateway(GatewayOpts::new("127.0.0.1:8090".parse()?).identity(MyAuth).surfaces(gateway_surfaces));
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
# From the trembita repo (path dependency):
./scripts/trembita-init.sh my-app
# or:
cargo run -p trembita-cli -- new my-app --trembita-path .

# With feature selection:
cargo run -p trembita-cli -- new my-app \
  --features jobs,gateway,telemetry,topics,external-backlog
```

Generates the [framework layout](decisions/framework-conventions.md):

| Path | Role |
|------|------|
| `main.rs` | Boot only — `App::new(AppConfig::from_env()).run().await` |
| `manifest.rs` | [`AppManifest::build()`](../../crates/trembita/src/app/manifest.rs) — jobs, topics, workers (`// trembita:*` marker comments) |
| `app.rs` | [`.manifest(manifest::build())`](../crates/trembita/src/app/builder.rs), gateway [`.gateway_routes()`](../crates/trembita/src/app/builder.rs), `.run()` |
| `consumers/`, `actors/`, `http/`, `domain/` | Handlers, surfaces, hexagon |
| `deploy/` | Local cluster env + compose |

Add capabilities by editing **`src/manifest.rs`** (inside `// trembita:jobs` / `:topics` / … regions),
adding handlers under `consumers/` / `actors/`, and wiring gateway routes in **`app.rs`** / `src/http/`
(see scaffold `src/http/ops.rs` and [`examples/`](../examples/)). Then:

```bash
trembita doctor   # manifest ↔ files consistency (read-only)
```

## 10. Observability & ops

| Need | How |
|------|-----|
| Live dashboard | Merge [`OpsApi::route_table()`](../crates/trembita-http/src/ops_routes.rs) in [`GatewayOpts::surfaces`](../crates/trembita/src/gateway/opts.rs) (see scaffold `src/http/ops.rs`) |
| Queue / workflow panels | Dashboard polls `/introspect/queues` and `/introspect/sagas` |
| Prometheus | Scrape `GET /metrics` (includes `trembita_queue_*`, `trembita_saga_*`) |
| Push export | `.metrics_sink(Arc::new(my_sink))` on [`TrembitaAppBuilder`](../crates/trembita/src/app/mod.rs) — see [`MetricsSink`](../crates/trembita-dashboard/src/metrics_sink.rs) |
| Live events | `cluster.events().subscribe()` — forward [`TrembitaEvent`](../crates/trembita-dashboard/src/telemetry.rs) to your sink |
| Production checklist | [ops/production-runbook.md](ops/production-runbook.md) |

## 11. Cluster APIs

Most apps stay on `TrembitaApp`. For custom state machines, multi-Raft, or direct supervisor/queue access, use [`trembita::cluster`](../crates/trembita/src/cluster.rs) or the [`TrembitaApp`](../crates/trembita/src/app/mod.rs) methods (`control`, `registry`, `supervisor`, …).

| Need | Doc |
|------|-----|
| Background jobs | [scenarios/background-jobs.md](scenarios/background-jobs.md) |
| Event topics | [scenarios/event-topics.md](scenarios/event-topics.md) |
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
