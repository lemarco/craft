# crafty

**A distributed Raft + actor framework for Rust: one codebase, N nodes, elastic and self-healing.**

[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![rust](https://img.shields.io/badge/rustc-1.90%2B-orange.svg)](#msrv)

[![crates.io](https://img.shields.io/crates/v/crafty.svg)](https://crates.io/crates/crafty)
[![docs.rs](https://docs.rs/crafty/badge.svg)](https://docs.rs/crafty)

---

## Why this exists

**Problem:** Running a stateful app on multiple VPS or bare-metal nodes usually means bolting together separate pieces — a consensus library, an actor runtime, a transport layer, mTLS, membership, observability — and wiring them yourself.

**Idea:** Embed consensus + actors in *your* binary. Same artifact on every node; the cluster bootstraps, elects a leader, replicates a linearizable state machine, and hosts supervised actors that can message and migrate across nodes. No sidecar, no separate control plane.

**Product teams:** four scenarios (background jobs, stateful workers, real-time sessions, workflows) on **embedded redb** — no mandatory Redis or Kubernetes. Start with [docs/getting-started.md](docs/getting-started.md) and [docs/scenarios/README.md](docs/scenarios/README.md).

---

## Current status

| | |
|---|---|
| **Version** | `0.5.2` (pre-1.0) |
| **Maturity** | Advanced prototype — published on [crates.io](https://crates.io/crates/crafty) |
| **Release** | v0.5.2 — [CHANGELOG.md](CHANGELOG.md) · [docs.rs/crafty/0.5.2](https://docs.rs/crafty/0.5.2) |
| **Full status** | [docs/status.md](docs/status.md) |

### Highlights

- Pure Raft FSM, HTTP/3/mTLS, redb persistence, cross-node actors
- Multi-Raft write scaling: **Meta-Raft coordinator** (join/catalog/saga isolated from user groups), dynamic catalog, stable shards, group migration, per-group membership
- Cross-shard saga coordinator + optional 2PC; follower/lease reads
- mTLS hot reload, reachability-driven supervisor, `crafty-ops` backup
- **Product showcases** — five standalone apps in [`examples/`](examples/README.md) (jobs, stateful workers, realtime, workflows, self-update)
- **`CraftyApp`** + HTTP gateway (sticky sessions, TLS, drain), batch jobs, actor-store TTL/GC — [getting-started](docs/getting-started.md)
- **Self-update coordinator** — leader reconcile + local executor ([upgrade-coordinator](docs/decisions/upgrade-coordinator.md))
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

./scripts/run-example.sh background-jobs
./scripts/run-example.sh stateful-workers
./scripts/run-example.sh realtime
./scripts/run-example.sh workflows

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

## Quick API sketch (product)

```rust
use std::time::Duration;
use crafty::{CraftyApp, CraftyConfigure, GatewayOpts, JobOpts, RunOpts, consumer};

#[consumer("jobs")]
async fn handle_job(_payload: &[u8]) -> Result<(), ()> {
    Ok(())
}

CraftyApp::builder()
    .data_dir("/var/lib/crafty")
    .jobs([JobOpts::new("jobs")
        .lease(Duration::from_secs(300))
        .consumer(&HandleJobConsumer)
        .http_enqueue(true)])
    .configure(CraftyConfigure {
        admin_addr: Some("127.0.0.1:8080".parse()?),
        ..CraftyConfigure::default()
    })
    .gateway(GatewayOpts::new("127.0.0.1:8090".parse()?))
    .run(RunOpts::default().with_wait_queue("jobs"))
    .await?;
```

## Cluster APIs (`crafty::cluster`)

Custom [`StateMachine`](crates/crafty-core/src/lib.rs) wiring, multi-Raft, keyed client, and direct queue access live in [`crafty::cluster`](crates/crafty/src/cluster.rs). Product apps use [`CraftyApp`](docs/getting-started.md) instead.

See [multi-raft](docs/decisions/multi-raft.md), [job-queue](docs/decisions/job-queue.md), and [getting-started](docs/getting-started.md).

## Design principles

- **Library-first** — embed `crafty`, no sidecar ([deployment-model](docs/decisions/deployment-model.md))
- **Linearizable SM** — `propose` / `query` via Raft ([client-and-routing](docs/decisions/client-and-routing.md))
- **Transparent routing** — any node forwards to leader ([client-and-routing](docs/decisions/client-and-routing.md))
- **Pure core** — `crafty-core` is I/O-free; ports & adapters ([architecture-style](docs/decisions/architecture-style.md))
- **Testable** — sim-first + E2E ([testing-strategy](docs/decisions/testing-strategy.md))

## Install

```toml
[dependencies]
crafty = "0.4"
```

Product apps: enable `http-jobs` for HTTP job routes and `dev-certs` for local QUIC without PEM files — see [getting-started](docs/getting-started.md).

## Workspace crates

| Crate | Purpose |
|-------|---------|
| [`crafty`](crates/crafty) | Facade — `CraftyApp` + `crafty::cluster` |
| [`crafty-http`](crates/crafty-http) | Product HTTP (`POST /jobs/{stream}` → 202) |
| [`crafty-core`](crates/crafty-core) | Pure Raft FSM + shard planners + reference [`kv`](crates/crafty-core/src/kv.rs) StateMachine |
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
| [`crafty-node`](crates/crafty-node) | Reference binary (repo/e2e only) |

## Documentation map

| Doc | When to read |
|-----|----------------|
| [docs/getting-started.md](docs/getting-started.md) | **Product app tutorial** (CraftyApp, no Redis) |
| [docs/scenarios/README.md](docs/scenarios/README.md) | **Four product scenarios** |
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
