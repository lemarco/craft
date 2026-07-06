# craft

**A distributed Raft + actor framework for Rust: one codebase, N nodes, elastic and self-healing.**

[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![rust](https://img.shields.io/badge/rustc-1.85%2B-orange.svg)](#msrv)

Write your state machine and actors once, then run the *same* binary on as many
nodes as you like. Nodes form a [Raft](https://raft.github.io/) cluster over
HTTP/3 (QUIC + mTLS), replicate a linearizable state machine, and host
supervised actors that can message, spawn, and migrate across the cluster.

```rust
use std::time::Duration;
use craft::{CraftCluster, NodeId};
use craft::net::LocalNetwork;

let net = LocalNetwork::new();
let cluster = CraftCluster::builder(NodeId(1), Counter::default())
    .members([NodeId(1), NodeId(2), NodeId(3)])
    .tick_period(Duration::from_millis(10))
    .start_local(&net)
    .await;
```

## Why craft

- **Library-first.** No sidecar, no separate control plane — `craft` is a crate
  you embed. Your app *is* the cluster node ([ADR 004](docs/decisions/004-deployment-model.md)).
- **One dependency.** The `craft` facade re-exports the whole stable API; add it
  and go. Advanced users can depend on the sub-crates directly.
- **Linearizable.** A replicated `StateMachine` with leader-based writes and
  `ReadIndex` reads ([ADR 005](docs/decisions/005-read-consistency.md)).
- **Transparent routing.** Clients hit *any* node; requests are forwarded to the
  leader automatically ([ADR 003](docs/decisions/003-client-routing.md)).
- **Cross-node actors.** Supervised actors message, spawn, and migrate across
  nodes; the leader auto-places one worker per node as the cluster grows
  ([ADR 013](docs/decisions/013-cross-node-actors.md), [ADR 015](docs/decisions/015-auto-spawn-on-join.md)).
- **Secure by default.** HTTP/3 over QUIC with mutual TLS between every node
  ([ADR 006](docs/decisions/006-security.md)).
- **Observable.** Built-in health/admin endpoints and a live introspection view
  ([ADR 025](docs/decisions/025-health-admin-port.md), [ADR 026](docs/decisions/026-observability.md)).

## Install

```toml
[dependencies]
craft = "0.1"
```

Or run the reference node binary:

```sh
cargo install craft-node
```

## Workspace crates

| Crate | Purpose |
|-------|---------|
| [`craft`](crates/craft) | **Start here.** Facade + `CraftCluster` builder; re-exports the public API |
| [`craft-core`](crates/craft-core) | Pure Raft consensus state machine (no I/O) |
| [`craft-proto`](crates/craft-proto) | Wire types + `postcard` codec |
| [`craft-storage`](crates/craft-storage) | Durable log, hard state, snapshots |
| [`craft-net`](crates/craft-net) | HTTP/3 (QUIC) transport with mTLS |
| [`craft-actor`](crates/craft-actor) | Actor runtime, registry, cluster supervision |
| [`craft-client`](crates/craft-client) | In-process + remote (HTTP/3) client API |
| [`craft-macros`](crates/craft-macros) | Derive macros (`StateMachine`, `remote_actor`) |
| [`craft-store-redis`](crates/craft-store-redis) | Redis-backed `ActorStateStore` |
| [`craft-dashboard`](crates/craft-dashboard) | Observability dashboard + admin endpoints |
| [`craft-sim`](crates/craft-sim) | Deterministic simulation harness |
| [`craft-node`](crates/craft-node) | Reference binary that runs a node from env config |

## Examples

Runnable examples live in [`crates/craft/examples`](crates/craft/examples):

```sh
cargo run -p craft --example kv_store          # single-node KV store
cargo run -p craft --example three_node_local  # 3 nodes + transparent forwarding
cargo run -p craft --example actors_cluster    # auto-placed, cross-node actors
```

A full multi-process cluster over **real QUIC + mTLS** lives in [`e2e/`](e2e):

```sh
./e2e/run.sh   # boots 3 craft-node containers, asserts election + failover
```

Provision certificates for a real deployment with
[`examples/certs/generate.sh`](examples/certs/generate.sh) — see [docs/certs.md](docs/certs.md).

Benchmarks and a soak harness live in [`benchmarks/`](benchmarks) (a standalone
crate):

```sh
cargo bench --manifest-path benchmarks/Cargo.toml               # append / apply / deliver
SOAK_SECS=60 cargo run --release --manifest-path benchmarks/Cargo.toml --bin soak
```

## Documentation

- API docs: [docs.rs/craft](https://docs.rs/craft)
- Architecture & rationale: [`docs/`](docs) — every design decision is an
  [ADR](docs/decisions); the wire protocol is in [docs/protocol.md](docs/protocol.md).
- Roadmap / status: [docs/backlog.md](docs/backlog.md).

## MSRV

Minimum Supported Rust Version is **1.85**. MSRV bumps are a minor-version event
([ADR 028](docs/decisions/028-library-and-publishing.md)).

## Status

Pre-1.0 (`0.x`): the API may change on minor bumps, documented in
[CHANGELOG.md](CHANGELOG.md). Not yet recommended for production.

## License

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option. Unless you explicitly state otherwise, any contribution
intentionally submitted for inclusion in this project by you, as defined in the
Apache-2.0 license, shall be dual-licensed as above, without any additional
terms or conditions.
