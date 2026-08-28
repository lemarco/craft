# Getting started — product apps (no Redis)

Quick path for **product teams** using [`CraftyApp`](../../crates/crafty/src/app.rs) — actors, jobs, and durable workflow keys on **embedded redb**, no Kubernetes, no mandatory Redis.

**Scenarios:** [scenarios/README.md](scenarios/README.md) · **Backlog:** [backlog.md](backlog.md)

## 1. Add dependency

```toml
[dependencies]
crafty = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal"] }
```

Enable `dev-certs` for local single-node without PEM files:

```toml
crafty = { version = "0.1", features = ["dev-certs"] }
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
        .start_local(&net)
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

```rust
let app = CraftyApp::from_env()?
    .start_quic(security, listen, peers)
    .await?;
```

Or build from env manually:

```rust
let cfg = crafty::app_config_from_env()?;
let app = CraftyApp::builder(cfg.node_id)
    .data_dir(cfg.data_dir.expect("CRAFTY_DATA_DIR"))
    .job_stream("jobs", cfg.job_queue_lease)
    .members(cfg.members)
    .start_quic(cfg.security, cfg.listen, cfg.peers)
    .await?;
```

## 4. Workers (actors)

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
    .start_local(&net)
    .await;
```

Stateful workflow keys: use `app.actor_state_store()` with [`store_get` / `store_set`](../../crates/crafty-actor/src/store_codec.rs) — backed by redb when `data_dir` is set.

## 5. Advanced APIs

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
