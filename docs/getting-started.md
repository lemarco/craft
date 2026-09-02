# Getting started — product apps (no Redis)

Quick path for **product teams** using [`TrembitaApp`](../crates/trembita/src/app.rs) — actors, jobs, and durable workflow keys on **embedded redb**, no Kubernetes, no mandatory Redis.

**Scenarios:** [scenarios/README.md](scenarios/README.md) · **Showcases:** [examples/README.md](../examples/README.md) · **Backlog:** [backlog.md](backlog.md)

## 1. Add dependency

```toml
[dependencies]
trembita = "0.5"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal"] }
```

Enable `dev-certs` for local single-node without PEM files:

```toml
trembita = { version = "0.5", features = ["dev-certs"] }
```

## 2. Minimal app

Every process is a **QUIC cluster member**: solo `cargo run` is a one-node seed (`TREMBITA_ALLOW_JOIN=1` by default); add nodes with `TREMBITA_JOIN_SEEDS`. Same binary, same `.run()`.

```rust
use std::time::Duration;
use trembita::{TrembitaApp, GatewayOpts, QueueOpts, RunOpts};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    trembita::init_tracing();

    TrembitaApp::builder()
        .data_dir("/tmp/my-app")
        .queue([QueueOpts::new("jobs", Duration::from_secs(60))])
        .gateway(GatewayOpts::new("127.0.0.1:8090".parse()?)) // add .routes(...) and opt-in APIs as needed
        .run(RunOpts::default().with_wait_queue("jobs"))
        .await
}
```

With `dev-certs` and no PEM env vars, a solo seed uses ephemeral mTLS automatically.

With `data_dir`, trembita opens `{data_dir}/actor-store.redb`, `{data_dir}/queue-*.redb`, and `{data_dir}/node-id` (assigned id, persisted across restarts).

## 3. Multi-node (env only)

Same code as above. [`cluster.sh`](../examples/background-jobs/cluster.sh) sets env per process — no separate “cluster main”.

| Variable | Purpose |
|----------|---------|
| `TREMBITA_LISTEN` | QUIC `host:port` (default `0.0.0.0:7443`) |
| `TREMBITA_DATA_DIR` | redb + `node-id` file |
| `TREMBITA_CERT_DIR` | Shared dir with `node-{id}.pem` + `ca.pem` |
| `TREMBITA_JOIN_SEEDS` | Join existing cluster (`id@host:port`) |
| `TREMBITA_ALLOW_JOIN` | Seed accepts joins (default `1` when not joining) |
| `TREMBITA_GATEWAY` | Product HTTP/WS bind |
| `TREMBITA_GATEWAY_TLS_CERT` / `TREMBITA_GATEWAY_TLS_KEY` | Gateway HTTPS / WSS (optional; both required) |
| `TREMBITA_ADMIN_TLS_CERT` / `TREMBITA_ADMIN_TLS_KEY` | Admin HTTPS (optional; both required) |
| `TREMBITA_JOB_QUEUE` | Job stream name (optional) |

Node id is **not** configured — seed gets `1`, joiners are assigned by the leader and persisted under `TREMBITA_DATA_DIR`.

**Homogeneous nodes:** every VPS runs the same binary (gateway + consumers when configured). Local **API vs jobs** fairness uses [`.workload()`](../crates/trembita/src/workload.rs) compute tokens ([workload governor](decisions/workload-governor.md)) — not static node roles. Edge-only ingress without local consumers: omit `.jobs()` / `.workers()` on those nodes (deployment choice), not a role env var.

## 4. Try the showcases

Four standalone projects under [`examples/`](../examples/README.md) — excluded from workspace default-members; each has `Cargo.toml`, README, `trigger.sh`, and QUIC `cluster.sh`:

| Showcase | Pattern | Run |
|----------|---------|-----|
| [background-jobs](../examples/background-jobs/) | Durable job queue | `./scripts/run-example.sh background-jobs` |
| [stateful-workers](../examples/stateful-workers/) | Stateful actors + migration | `./scripts/run-example.sh stateful-workers` |
| [realtime](../examples/realtime/) | Sticky sessions / WebSocket | `./scripts/run-example.sh realtime` |
| [workflows](../examples/workflows/) | Saga journal + steps | `./scripts/run-example.sh workflows` |

3-node QUIC cluster (any showcase):

```bash
cd examples/background-jobs
./cluster.sh setup && ./cluster.sh up && ./trigger.sh hello
```

Shared infra: [`dev/`](../dev/README.md) (`cluster-common.sh`, `certs/generate.sh`). Docker Compose per showcase: `dev/compose/<name>/`.

Internal HTTP/WS client (not on crates.io; built by `./cluster.sh setup`):

```bash
cargo build -p trembita-showcase-client
./target/debug/trembita-showcase-client job 127.0.0.1:8090 emails hello
./target/debug/trembita-showcase-client ws 127.0.0.1:8294 alice hello
```

Reference KV [`StateMachine`](../crates/trembita-core/src/kv.rs) (`trembita::kv` on the facade) for low-level Raft `propose` / `query` without a full product app.

## 5. Workers (actors)

Register worker types with [`.workers()`](../crates/trembita/src/worker_opts.rs) and explicit [`WorkerScale`](../crates/trembita/src/worker_opts.rs) (`Fixed`, `PerNode`, or queue-driven `Auto`):

```rust
use trembita::actor::{UserActor, actor};
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

Legacy [`.actors()`](../crates/trembita/src/app.rs) + [`ActorGroupOpts`](../crates/trembita/src/actor_group.rs) remain supported.

Stateful workflow keys: use `app.actor_state_store()` with [`store_get` / `store_set`](../crates/trembita-actor/src/store_codec.rs) — backed by redb when `data_dir` is set.

## 6. HTTP job enqueue (optional)

Prefer [`.jobs()`](../crates/trembita/src/job_opts.rs) to register queue + consumer + HTTP enqueue in one call. Enable the `http-jobs` feature (default on the facade):

```toml
trembita = { version = "0.5", features = ["http-jobs"] }
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

Lower-level [`.queue()`](../crates/trembita/src/app.rs) + [`.consumer()`](../crates/trembita/src/app.rs) remain available. See [trembita-http README](../crates/trembita-http/README.md) and [background-jobs](scenarios/background-jobs.md).

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

WebSocket + sticky session routing. Auth stays in your code via [`GatewayIdentity`](scenarios/realtime-sessions.md); trembita maps identity → session key and opens a [`SessionHandle`](scenarios/realtime-sessions.md).

```rust
use axum::http::{HeaderMap, Method, Uri};
use axum::response::IntoResponse;
use trembita::{TrembitaGatewayState, GatewayOpts, GatewayIdentity, GatewayRequest, SessionKey};

// WebSocket upgrade: use Method + Uri + HeaderMap (not Request — upgrade consumes the body).
async fn ws(
    ws: WebSocketUpgrade,
    State(state): State<TrembitaGatewayState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let mut handle = match state
        .open_actor_session_parts("chat", &method, &uri, &headers, Some(Duration::from_secs(3600)))
        .await
    {
        Ok(h) => h,
        Err(e) => return e.into_response(),
    };
    // ws.on_upgrade(|socket| async move { handle.cast(...).await; … })
}

TrembitaApp::builder()
    .gateway(GatewayOpts::new("127.0.0.1:8090".parse()?).identity(MyAuth).routes(|state| { /* Router */ }));
```

Runnable showcase:

```bash
cd examples/realtime && cargo run --release
./trigger.sh alice hello   # uses trembita-showcase-client or websocat
```

See [realtime-sessions](scenarios/realtime-sessions.md) and [gateway-identity](decisions/gateway-identity.md).

## 9. Scaffold a new project

```bash
./scripts/trembita-init.sh my-app
cd my-app
cargo run
```

Generates a `TrembitaApp` stub, docker-compose for 3-node local dev, and links to scenario guides.

## 10. Observability & ops

| Need | How |
|------|-----|
| Live dashboard | `.configure(TrembitaConfigure { admin_addr: Some(...), ..Default::default() })` or `TREMBITA_ADMIN` |
| Queue / workflow panels | Dashboard polls `/introspect/queues` and `/introspect/sagas` |
| Prometheus | Scrape `GET /metrics` (includes `trembita_queue_*`, `trembita_saga_*`) |
| Push export | `.metrics_sink(Arc::new(my_sink))` on [`TrembitaAppBuilder`](../crates/trembita/src/app.rs) — see [`MetricsSink`](../crates/trembita-dashboard/src/metrics_sink.rs) |
| Live events | `cluster.events().subscribe()` — forward [`TrembitaEvent`](../crates/trembita-dashboard/src/telemetry.rs) to your sink |
| Production checklist | [ops/production-runbook.md](ops/production-runbook.md) |

## 11. Cluster APIs

Most apps stay on `TrembitaApp`. For custom state machines, multi-Raft, or direct supervisor/queue access, use [`trembita::cluster`](../crates/trembita/src/cluster.rs) or the [`TrembitaApp`](../crates/trembita/src/app.rs) methods (`control`, `registry`, `supervisor`, …).

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
- [actor-state-store](decisions/actor-state-store.md)
- [ops/production-runbook.md](ops/production-runbook.md)
