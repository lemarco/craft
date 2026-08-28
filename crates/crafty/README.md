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
crafty = "0.1"
```

## Quickstart

`CraftyCluster::builder` assembles a whole node — consensus runtime, actors,
supervisor, and observability — from one call. Use `start_local` for in-process
clusters (tests, single-process multi-node dev) and `start_quic` for the live
QUIC/mTLS transport.

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

- `dev-certs` *(off by default)* — helpers to mint a throwaway cluster CA and
  per-node identities for local development. Production deployments supply real
  certificates instead (see the `examples/certs/` provisioning script).

## Learn more

- Runnable examples: `cargo run -p crafty --example kv_store`
  (also `three_node_local`, `actors_cluster`).
- The reference runner binary: [`crafty-node`](https://crates.io/crates/crafty-node).
- Architecture, ADRs, and the wire protocol live in the
  [repository](https://gitlab.com/lemarco/craft) `docs/` directory.

## License

Dual-licensed under `MIT OR Apache-2.0`.
