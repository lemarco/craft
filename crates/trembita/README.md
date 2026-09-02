# trembita

**A distributed Raft + actor framework for Rust: one codebase, N nodes, elastic and self-healing.**

Write your state machine and actors once, then run the *same* binary on as many
nodes as you like. Nodes form a [Raft](https://raft.github.io/) cluster over
HTTP/3 (QUIC + mTLS), replicate a linearizable state machine, and host
supervised actors that can message, spawn, and migrate across the cluster.

This crate is the **facade**: it re-exports the stable public API, so most users
depend only on `trembita`.

```toml
[dependencies]
trembita = { version = "0.5", features = ["http-jobs", "dev-certs"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "signal"] }
```

## Product quickstart (`TrembitaApp`)

Every process is a QUIC cluster member. Topology comes from `TREMBITA_*` env; domain logic from Rust.

```rust,no_run
use std::time::Duration;
use trembita::{TrembitaApp, GatewayOpts, QueueOpts, RunOpts};

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
TrembitaApp::builder()
    .data_dir("/tmp/my-app")
    .queue([QueueOpts::new("jobs", Duration::from_secs(300))])
    .gateway(GatewayOpts::new("127.0.0.1:8090".parse()?).with_jobs_api(true))
    .run(RunOpts::default().with_wait_queue("jobs"))
    .await?;
# Ok(())
# }
```

See [getting-started.md](../../docs/getting-started.md) and runnable [examples/](../../examples/README.md).

## Cluster APIs (`trembita::cluster`)

Custom [`StateMachine`](https://docs.rs/trembita-core/latest/trembita_core/trait.StateMachine.html) wiring, tests, and low-level control: [`trembita::cluster`](src/cluster.rs) (`TrembitaCluster`, `TrembitaClusterBuilder`, queues, journals). Product apps use [`TrembitaApp`](#product-quickstart-trembitaapp) above.

## Features

- `http-jobs` — product HTTP gateway helpers (`GatewayOpts`, `/jobs/*`, `/actors/*`, `/workflows/*`)
- `dev-certs` — ephemeral mTLS for solo local seeds without PEM files

## Learn more

- Product showcases: `./scripts/run-example.sh background-jobs` — full index in [examples/README.md](../../examples/README.md).
- The reference runner binary: [`trembita-node`](../trembita-node) (repo only, not on crates.io).
- Architecture, ADRs, and the wire protocol: [repository docs](https://gitlab.com/lemarco/trembita/-/tree/master/docs)

## License

Dual-licensed under `MIT OR Apache-2.0`.
