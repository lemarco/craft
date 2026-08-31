# Getting started — product apps (no Redis)

Quick path for **product teams** using [`CraftyApp`](../../crates/crafty/src/app.rs) — actors, jobs, and durable workflow keys on **embedded redb**, no Kubernetes, no mandatory Redis.

**Scenarios:** [scenarios/README.md](scenarios/README.md) · **Showcases:** [examples/README.md](../examples/README.md) · **Backlog:** [backlog.md](backlog.md)

## 1. Add dependency

```toml
[dependencies]
crafty = "0.2"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal"] }
```

Enable `dev-certs` for local single-node without PEM files:

```toml
crafty = { version = "0.2", features = ["dev-certs"] }
```

## 2. Minimal app (local dev)

```rust
use std::time::Duration;
use crafty::{CraftyApp, NodeId};
use crafty::net::LocalNetwork;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    crafty::init_tracing();
    let net = LocalNetwork::new();
    let app = CraftyApp::builder(NodeId(1))
        .data_dir("/tmp/my-app")
        .job_stream("jobs", Duration::from_secs(60))
        .members([NodeId(1)])
        .start_local_shared(&net)
        .await;

    app.enqueue("jobs", b"hello").await?;
    app.run_until_shutdown().await
}
```

With `data_dir`, crafty automatically opens:

- `{data_dir}/actor-store.redb` — durable actor workflow keys (voter replicated)
- `{data_dir}/queue-jobs.redb` — when `.job_stream("jobs", …)` is registered

## 3. Production (env-driven)

Same binary on every VPS; config via environment (see `crafty-node` for the full list):

| Variable | Purpose |
|----------|---------|
| `CRAFTY_NODE_ID` | This node |
| `CRAFTY_LISTEN` | QUIC `host:port` |
| `CRAFTY_DATA_DIR` | redb directory |
| `CRAFTY_JOB_QUEUE` | Job stream name (optional) |
| `CRAFTY_PEERS` | Static `id@host:port,...` |
| `CRAFTY_JOIN_SEEDS` | Dynamic join seeds |
| `CRAFTY_NODE_CERT` / `KEY` / `CA` | mTLS (required multi-node) |
| `CRAFTY_GATEWAY` | Product HTTP/WS bind (optional) |

Every node can run gateway + workers — the cluster routes work. For edge-only ingress (no local consumers), set `CRAFTY_ROLE=gateway` (advanced; see [`env_config.rs`](../crates/crafty/src/env_config.rs)).

```rust
use crafty::ReadyOpts;

let app = CraftyApp::start_from_env_shared().await?;
app.wait_until_ready(ReadyOpts::default().with_queue("jobs")).await;
// spawn consumers, then:
app.run_until_shutdown_shared().await?;
```

Or build from env manually:

```rust
let cfg = crafty::app_config_from_env()?;
let app = CraftyApp::builder(cfg.node_id)
    .data_dir(cfg.data_dir.expect("CRAFTY_DATA_DIR"))
    .job_stream("jobs", cfg.job_queue_lease)
    .members(cfg.members)
    .start_quic_shared(cfg.security, cfg.listen, cfg.peers)
    .await?;
```

## 4. Try the showcases

Four end-to-end examples under [`examples/`](../examples/README.md):

```bash
./scripts/run-example.sh background-jobs
cd examples/background-jobs && ./cluster.sh setup && ./cluster.sh up
```

Internal HTTP/WS client (not on crates.io):

```bash
cargo build -p crafty-showcase-client
./target/debug/crafty-showcase-client job 127.0.0.1:8090 emails hello
./target/debug/crafty-showcase-client ws 127.0.0.1:8294 alice hello
```

Docker Compose clusters: [`dev/compose/`](../dev/compose/).

## 5. Workers (actors)

Register a worker type and let the leader place instances (one per VPS by default):

```rust
use crafty::actor::{UserActor, remote_actor};

struct EmailWorker;

#[remote_actor]
impl UserActor for EmailWorker {
    type Config = ();
    type Message = Vec<u8>;
    type Error = std::convert::Infallible;
    // …
}

let app = CraftyApp::builder(NodeId(1))
    .data_dir("/var/lib/crafty")
    .manage_auto::<EmailWorker>("email", ())
    .start_local_shared(&net)
    .await;
```

Stateful workflow keys: use `app.actor_state_store()` with [`store_get` / `store_set`](../../crates/crafty-actor/src/store_codec.rs) — backed by redb when `data_dir` is set.

## 6. HTTP job enqueue (optional)

Enable the `http-jobs` feature and mount the Axum router on your gateway VPS:

```toml
crafty = { version = "0.2", features = ["http-jobs"] }
```

```rust
use std::sync::Arc;
use crafty::CraftyApp;

let app = CraftyApp::builder(NodeId(1))
    .data_dir("/var/lib/crafty")
    .gateway_addr("0.0.0.0:3000".parse()?)
    .gateway_jobs_api(true)
    .start_local_shared(&net)
    .await;

// POST /jobs/{stream} → 202 { "job_id": … }
```

See [crafty-http README](../../crates/crafty-http/README.md) and [background-jobs](scenarios/background-jobs.md).

## 7. Workflows (sagas)

Build a named plan with [`WorkflowBuilder`](../../crates/crafty/src/workflow.rs) and run it on the cluster journal:

```rust
use crafty::{WorkflowBuilder, CraftyApp};

let plan = WorkflowBuilder::new("onboard-user")
    .step("create_account", &key, payload)
    .compensate("create_account", undo_payload)
    .build()?;

app.run_workflow(&plan).await?;
```

Example: `./scripts/run-example.sh workflows`.

## 8. Real-time gateway

WebSocket + session routing showcase:

```bash
cd examples/realtime && cargo run --release
./trigger.sh alice hello   # uses crafty-showcase-client or websocat
```

See [realtime-sessions](scenarios/realtime-sessions.md).

## 9. Scaffold a new project

```bash
./scripts/crafty-init.sh my-app
cd my-app
cargo run
```

Generates a `CraftyApp` stub, docker-compose for 3-node local dev, and links to scenario guides.

## 10. Observability & ops

| Need | How |
|------|-----|
| Live dashboard | `.admin_addr("0.0.0.0:8080")` or `CRAFTY_ADMIN` → `http://host:8080/dashboard` |
| Queue / workflow panels | Dashboard polls `/introspect/queues` and `/introspect/sagas` |
| Prometheus | Scrape `GET /metrics` (includes `crafty_queue_*`, `crafty_saga_*`) |
| Production checklist | [ops/production-runbook.md](ops/production-runbook.md) |

## 11. Advanced APIs

Use `app.cluster()` for full [`CraftyCluster`](../../crates/crafty/src/cluster.rs) access (saga, multi-Raft, supervisor). Product scenarios:

| Need | Doc |
|------|-----|
| Background jobs | [scenarios/background-jobs.md](scenarios/background-jobs.md) |
| Stateful workers | [scenarios/stateful-workers.md](scenarios/stateful-workers.md) |
| Sessions / WebSocket | [scenarios/realtime-sessions.md](scenarios/realtime-sessions.md) |
| Workflows | [scenarios/workflows.md](scenarios/workflows.md) |

## Related

- [deployment-model](decisions/deployment-model.md)
- [product-scenarios](decisions/product-scenarios.md)
- [actor-state-store](decisions/actor-state-store.md)
- [ops/production-runbook.md](ops/production-runbook.md)
