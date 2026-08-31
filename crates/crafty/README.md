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
crafty = { version = "0.3", features = ["http-jobs", "dev-certs"] }
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
    .gateway(
        "127.0.0.1:8090".parse()?,
        GatewayOpts::default().with_jobs_api(true),
    )
    .run(RunOpts::default().with_wait_queue("jobs"))
    .await?;
# Ok(())
# }
```

See [getting-started.md](../../docs/getting-started.md) and runnable [examples/](../../examples/README.md).

## Advanced (`CraftyCluster`)

`CraftyCluster::builder` assembles a whole node with your own `StateMachine`. Use
`start_local` for in-process clusters (tests) and `start_quic` for production QUIC/mTLS.

```rust,no_run
use std::time::Duration;
use crafty::{CraftyCluster, NodeId};
use crafty::net::LocalNetwork;
use crafty::core::StateMachine;
use crafty::proto::LogIndex;

#[derive(Default)]
struct Counter(u64);

impl StateMachine for Counter {
    type Command = u64;
    type Query = ();
    type Response = u64;
    type Error = std::convert::Infallible;
    fn apply(&mut self, _: LogIndex, c: &u64) -> Result<u64, Self::Error> {
        self.0 += *c;
        Ok(self.0)
    }
    fn query(&self, _: &()) -> Result<u64, Self::Error> { Ok(self.0) }
    fn snapshot(&self) -> Result<Vec<u8>, Self::Error> { Ok(self.0.to_le_bytes().to_vec()) }
    fn restore(&mut self, b: &[u8]) -> Result<(), Self::Error> {
        self.0 = u64::from_le_bytes(b.try_into().unwrap());
        Ok(())
    }
}

# async fn run() {
let net = LocalNetwork::new();
let cluster = CraftyCluster::builder(NodeId(1), Counter::default())
    .members([NodeId(1), NodeId(2), NodeId(3)])
    .tick_period(Duration::from_millis(10))
    .start_local(&net)
    .await;
# let _ = cluster;
# }
```

## Features

- `http-jobs` — product HTTP gateway helpers (`GatewayOpts`, `/jobs/*`, `/actors/*`, `/workflows/*`)
- `dev-certs` — ephemeral mTLS for solo local seeds without PEM files

## Learn more

- Product showcases: `./scripts/run-example.sh background-jobs` — full index in [examples/README.md](../../examples/README.md).
- The reference runner binary: [`crafty-node`](../crafty-node) (repo only, not on crates.io).
- Architecture, ADRs, and the wire protocol: [repository docs](https://gitlab.com/lemarco/craft/-/tree/master/docs)

## License

Dual-licensed under `MIT OR Apache-2.0`.
