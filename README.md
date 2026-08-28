# crafty

**A distributed Raft + actor framework for Rust: one codebase, N nodes, elastic and self-healing.**

[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![rust](https://img.shields.io/badge/rustc-1.90%2B-orange.svg)](#msrv)

---

## Why this exists

**Problem:** Running a stateful app on multiple VPS or bare-metal nodes usually means bolting together separate pieces — a consensus library, an actor runtime, a transport layer, mTLS, membership, observability — and wiring them yourself.

**Idea:** Embed consensus + actors in *your* binary. Same artifact on every node; the cluster bootstraps, elects a leader, replicates a linearizable state machine, and hosts supervised actors that can message and migrate across nodes. No sidecar, no separate control plane.



---

## Current status

| | |
|---|---|
| **Version** | `0.1.0` (pre-1.0) |
| **Maturity** | Advanced prototype — E2E, chaos, sim, release CI |
| **Release** | Ready — [releasing.md](docs/releasing.md) |
| **Full status** | [docs/status.md](docs/status.md) |

### Highlights

- Pure Raft FSM, HTTP/3/mTLS, redb persistence, cross-node actors
- Multi-Raft write scaling: **Meta-Raft coordinator** (join/catalog/saga isolated from user groups), dynamic catalog, stable shards, group migration, per-group membership
- Cross-shard saga coordinator + optional 2PC; follower/lease reads
- mTLS hot reload, reachability-driven supervisor, `crafty-ops` backup
- **Durable job queue** (tier C): `job_queue`, worker autoscale, sync voter replication — [job-queue](docs/decisions/job-queue.md)
- Design decision records — [docs/decisions/](docs/decisions/)

### Not yet (by design or process)

- Linearizable actor `ask`, global cross-shard serializable isolation
- See [docs/status.md](docs/status.md) for the full deferred list and known limits (R1–R6)

---

## Quick start

```sh
cd crafty
lefthook install
./scripts/quality-gate-pre-commit.sh

cargo run -p crafty --example kv_store
cargo run -p crafty --example three_node_local
cargo run -p crafty --example actors_cluster
cargo run -p crafty --example job_queue_worker   # cluster queue: follower worker + failover
cargo run -p crafty --example job_queue_cluster  # sharded queue, dedup, autoscale

./e2e/run.sh      # 3-node QUIC/mTLS election + failover
./e2e/queue.sh    # job queue over QUIC (enqueue, follower lease/ack, leader failover)
./e2e/chaos.sh    # partition + heal
```

**Read next:** [docs/status.md](docs/status.md) → [docs/architecture.md](docs/architecture.md) → [crates/crafty/src/builder.rs](crates/crafty/src/builder.rs)

**`crafty-node` env:** `CRAFTY_NODE_ID`, `CRAFTY_LISTEN` (`:7443`), `CRAFTY_ADMIN` (`:8080`), `CRAFTY_PEERS`, `CRAFTY_JOIN_SEEDS`, `CRAFTY_DISCOVERY` — [docs/certs.md](docs/certs.md)

**Job queue on `crafty-node`** (optional, requires `CRAFTY_DATA_DIR`):

| Var | Meaning |
|-----|---------|
| `CRAFTY_DATA_DIR` | Persistent redb directory (Raft log + queue file) |
| `CRAFTY_JOB_QUEUE` | Enable durable queue stream name (e.g. `jobs`) |
| `CRAFTY_JOB_QUEUE_LEASE_SECS` | Lease visibility timeout (default `60`) |

See [job-queue](docs/decisions/job-queue.md) and [protocol.md](docs/protocol.md#job-queue-cross-node-tier-c).

---

## Quick API sketch

```rust
use std::time::Duration;
use crafty::{CraftyCluster, NodeId};
use crafty::net::LocalNetwork;

let net = LocalNetwork::new();
let cluster = CraftyCluster::builder(NodeId(1), Counter::default())
    .members([NodeId(1), NodeId(2), NodeId(3)])
    .tick_period(Duration::from_millis(10))
    .start_local(&net)
    .await;
```

Multi-Raft: `.raft_groups(n)`, `.stable_shards()`, `.data_dir(path)`. With `raft_groups > 1`, cluster metadata (join/leave, catalog, saga journal) lives on a dedicated **Meta-Raft** group (`group-meta.redb`); group 0 is user data only — [multi-raft](docs/decisions/multi-raft.md). Keyed client: `propose_keyed` / `query_keyed`. Cross-shard: `run_keyed_saga` / `resume_keyed_saga`.

Job queue (tier C): `.data_dir(path).job_queue("jobs", lease_timeout)` → `cluster.job_queue("jobs")` for enqueue/lease/ack; workers use `run_queue_consumer`. Wire routes under `/raft/v1/queue/*` — [job-queue](docs/decisions/job-queue.md).

Durable mailbox (tier B spool): `.data_dir(path).durable_mailbox(true)` — write-ahead outbox/inbox for cross-node casts/asks; `{data_dir}/mailbox-spool.redb`.

## Design principles

- **Library-first** — embed `crafty`, no sidecar ([deployment-model](docs/decisions/deployment-model.md))
- **Linearizable SM** — `propose` / `query` via Raft ([client-and-routing](docs/decisions/client-and-routing.md))
- **Transparent routing** — any node forwards to leader ([client-and-routing](docs/decisions/client-and-routing.md))
- **Pure core** — `crafty-core` is I/O-free; ports & adapters ([architecture-style](docs/decisions/architecture-style.md))
- **Testable** — sim-first + E2E ([testing-strategy](docs/decisions/testing-strategy.md))

## Install

```toml
[dependencies]
crafty = "0.1"
```

```sh
cargo install crafty-node
```

## Workspace crates

| Crate | Purpose |
|-------|---------|
| [`crafty`](crates/crafty) | Facade + `CraftyCluster` builder |
| [`crafty-core`](crates/crafty-core) | Pure Raft FSM + shard planners |
| [`crafty-proto`](crates/crafty-proto) | Wire types + codec |
| [`crafty-storage`](crates/crafty-storage) | Durable log, snapshots |
| [`crafty-net`](crates/crafty-net) | HTTP/3 / QUIC + mTLS |
| [`crafty-actor`](crates/crafty-actor) | Runtime, registry, supervisor |
| [`crafty-client`](crates/crafty-client) | Client, saga, keyed/batch APIs |
| [`crafty-macros`](crates/crafty-macros) | Derive macros |
| [`crafty-store-redis`](crates/crafty-store-redis) | Redis `ActorStateStore` |
| [`crafty-dashboard`](crates/crafty-dashboard) | Admin + observability |
| [`crafty-sim`](crates/crafty-sim) | Deterministic sim harness |
| [`crafty-ops`](crates/crafty-ops) | Backup/restore CLI |
| [`crafty-node`](crates/crafty-node) | Reference binary |

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

Minimum Supported Rust Version is **1.90** ([library-and-publishing](docs/decisions/library-and-publishing.md)).

## License

Dual-licensed under MIT OR Apache-2.0 — [LICENSE-MIT](LICENSE-MIT), [LICENSE-APACHE](LICENSE-APACHE).
