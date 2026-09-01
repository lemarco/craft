# crafty

**A distributed Raft + actor framework for Rust: one codebase, N nodes, elastic and self-healing.**

Write your state machine and actors once, then run the *same* binary on as many
nodes as you like. Nodes form a [Raft](https://raft.github.io/) cluster over
HTTP/3 (QUIC + mTLS), replicate a linearizable state machine, and host
supervised actors that can message, spawn, and migrate across the cluster.

This crate is the **facade**: it re-exports the stable public API, so most users
depend only on `crafty`.

```toml
[dependencies]
crafty = { version = "0.4", features = ["http-jobs", "dev-certs"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal"] }
```

## Product quickstart (`CraftyApp`)

Every process is a QUIC cluster member. Topology comes from `CRAFTY_*` env; domain logic from Rust.

```rust,no_run
use std::time::Duration;
use crafty::{CraftyApp, GatewayOpts, QueueOpts, RunOpts};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
CraftyApp::builder()
    .data_dir("/tmp/my-app")
    .queue([QueueOpts::new("jobs", Duration::from_secs(300))])
    .gateway(GatewayOpts::new("127.0.0.1:8090".parse()?).with_jobs_api(true))
    .run(RunOpts::default().with_wait_queue("jobs"))
    .await?;
# Ok(())
# }
```

See [getting-started.md](../../docs/getting-started.md) and runnable [examples/](../../examples/README.md).

## Cluster APIs (`crafty::cluster`)

Custom [`StateMachine`](https://docs.rs/crafty-core/latest/crafty_core/trait.StateMachine.html) wiring, tests, and low-level control: [`crafty::cluster`](src/cluster.rs) (`CraftyCluster`, `CraftyClusterBuilder`, queues, journals). Product apps use [`CraftyApp`](#product-quickstart-craftyapp) above.

## Features

- `http-jobs` — product HTTP gateway helpers (`GatewayOpts`, `/jobs/*`, `/actors/*`, `/workflows/*`)
- `dev-certs` — ephemeral mTLS for solo local seeds without PEM files

## Learn more

- Product showcases: `./scripts/run-example.sh background-jobs` — full index in [examples/README.md](../../examples/README.md).
- The reference runner binary: [`crafty-node`](../crafty-node) (repo only, not on crates.io).
- Architecture, ADRs, and the wire protocol: [repository docs](https://gitlab.com/lemarco/craft/-/tree/master/docs)

## License

Dual-licensed under `MIT OR Apache-2.0`.
