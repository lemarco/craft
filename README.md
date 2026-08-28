# craft

**A distributed Raft + actor framework for Rust: one codebase, N nodes, elastic and self-healing.**

[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![rust](https://img.shields.io/badge/rustc-1.98%2B-orange.svg)](#msrv)

---

## Why this exists

**Problem:** Running a stateful app on multiple VPS/K8s nodes usually means bolting together separate pieces — a consensus library, an actor runtime, a transport layer, mTLS, membership, observability — and wiring them yourself.

**Idea:** Embed consensus + actors in *your* binary. Same artifact on every node; the cluster bootstraps, elects a leader, replicates a linearizable state machine, and hosts supervised actors that can message and migrate across nodes. No sidecar, no separate control plane.



---

## Current status

| | |
|---|---|
| **Version** | `0.1.0` (pre-1.0) |
| **Maturity** | Advanced prototype — E2E, chaos, sim, release CI; not yet on crates.io |
| **Full status** | [docs/status.md](docs/status.md) |

### Highlights

- Pure Raft FSM, HTTP/3/mTLS, redb persistence, cross-node actors
- Multi-Raft write scaling: **Meta-Raft coordinator** (join/catalog/saga isolated from user groups), dynamic catalog, stable shards, group migration, per-group membership
- Cross-shard saga coordinator + optional 2PC; follower/lease reads
- K8s manifests, cert hot reload, reachability-driven supervisor, `craft-ops` backup
- Design decision records — [docs/decisions/](docs/decisions/)

### Not yet (by design or process)

- crates.io / docs.rs publish ([releasing.md](docs/releasing.md))
- Linearizable actor `ask`, global cross-shard serializable isolation
- See [docs/status.md](docs/status.md) for the full deferred list and known limits (R1–R6)

---

## Quick start

```sh
cd craft
lefthook install
./scripts/quality-gate-pre-commit.sh

cargo run -p craft --example kv_store
cargo run -p craft --example three_node_local
cargo run -p craft --example actors_cluster

./e2e/run.sh      # 3-node QUIC/mTLS
./e2e/chaos.sh    # partition + heal
```

**Read next:** [docs/status.md](docs/status.md) → [docs/architecture.md](docs/architecture.md) → [crates/craft/src/builder.rs](crates/craft/src/builder.rs)

**`craft-node` env:** `CRAFT_NODE_ID`, `CRAFT_LISTEN` (`:7443`), `CRAFT_ADMIN` (`:8080`), `CRAFT_PEERS`, `CRAFT_JOIN_SEEDS`, `CRAFT_DISCOVERY` — [docs/certs.md](docs/certs.md)

---

## Quick API sketch

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

Multi-Raft: `.raft_groups(n)`, `.stable_shards()`, `.data_dir(path)`. With `raft_groups > 1`, cluster metadata (join/leave, catalog, saga journal) lives on a dedicated **Meta-Raft** group (`group-meta.redb`); group 0 is user data only — [multi-raft](docs/decisions/multi-raft.md). Keyed client: `propose_keyed` / `query_keyed`. Cross-shard: `run_keyed_saga` / `resume_keyed_saga`.

## Design principles

- **Library-first** — embed `craft`, no sidecar ([deployment-model](docs/decisions/deployment-model.md))
- **Linearizable SM** — `propose` / `query` via Raft ([client-and-routing](docs/decisions/client-and-routing.md))
- **Transparent routing** — any node forwards to leader ([client-and-routing](docs/decisions/client-and-routing.md))
- **Pure core** — `craft-core` is I/O-free; ports & adapters ([architecture-style](docs/decisions/architecture-style.md))
- **Testable** — sim-first + E2E ([testing-strategy](docs/decisions/testing-strategy.md))

## Install

```toml
[dependencies]
craft = "0.1"
```

```sh
cargo install craft-node
```

## Workspace crates

| Crate | Purpose |
|-------|---------|
| [`craft`](crates/craft) | Facade + `CraftCluster` builder |
| [`craft-core`](crates/craft-core) | Pure Raft FSM + shard planners |
| [`craft-proto`](crates/craft-proto) | Wire types + codec |
| [`craft-storage`](crates/craft-storage) | Durable log, snapshots |
| [`craft-net`](crates/craft-net) | HTTP/3 / QUIC + mTLS |
| [`craft-actor`](crates/craft-actor) | Runtime, registry, supervisor |
| [`craft-client`](crates/craft-client) | Client, saga, keyed/batch APIs |
| [`craft-macros`](crates/craft-macros) | Derive macros |
| [`craft-store-redis`](crates/craft-store-redis) | Redis `ActorStateStore` |
| [`craft-dashboard`](crates/craft-dashboard) | Admin + observability |
| [`craft-sim`](crates/craft-sim) | Deterministic sim harness |
| [`craft-ops`](crates/craft-ops) | Backup/restore CLI |
| [`craft-node`](crates/craft-node) | Reference binary |

## Documentation map

| Doc | When to read |
|-----|----------------|
| [docs/status.md](docs/status.md) | **Current capabilities and limits** |
| [docs/architecture.md](docs/architecture.md) | Crate graph, data flows |
| [docs/decisions/](docs/decisions/) | Design rationale (multi-Raft, membership, actors, …) |
| [docs/testing-coverage.md](docs/testing-coverage.md) | Test inventory |
| [docs/protocol.md](docs/protocol.md) | HTTP/3 routes |
| [docs/releasing.md](docs/releasing.md) | crates.io workflow |
| [CHANGELOG.md](CHANGELOG.md) | Version history |

## MSRV

Minimum Supported Rust Version is **1.98** ([library-and-publishing](docs/decisions/library-and-publishing.md)).

## License

Dual-licensed under MIT OR Apache-2.0 — [LICENSE-MIT](LICENSE-MIT), [LICENSE-APACHE](LICENSE-APACHE).
