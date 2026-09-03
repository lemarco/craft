# trembita

**A distributed Raft + actor framework for Rust: one codebase, N nodes, elastic and self-healing.**

[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![rust](https://img.shields.io/badge/rustc-1.90%2B-orange.svg)](#msrv)

[![crates.io](https://img.shields.io/crates/v/trembita.svg)](https://crates.io/crates/trembita)
[![docs.rs](https://docs.rs/trembita/badge.svg)](https://docs.rs/trembita)

---

## Why this exists

**Problem:** Running a stateful app on multiple VPS or bare-metal nodes usually means bolting together separate pieces — a consensus library, an actor runtime, a transport layer, mTLS, membership, observability — and wiring them yourself.

**Idea:** Embed consensus + actors in *your* binary. Same artifact on every node; the cluster bootstraps, elects a leader, replicates a linearizable state machine, and hosts supervised actors that can message and migrate across nodes. No sidecar, no separate control plane.

**Product teams:** jobs, event topics, stateful workers, real-time sessions, and workflows on **embedded redb** — no mandatory Redis or Kubernetes. Start with [docs/getting-started.md](docs/getting-started.md) and [docs/scenarios/README.md](docs/scenarios/README.md).

---

## Current status

| | |
|---|---|
| **Version** | `0.1.0` |
| **Distribution** | Published on [crates.io](https://crates.io/crates/trembita) — E2E/chaos, product showcases |
| **Release** | v0.1.0 — [CHANGELOG.md](CHANGELOG.md) · [docs.rs/trembita/0.1.0](https://docs.rs/trembita/0.1.0) |
| **Full status** | [docs/status.md](docs/status.md) |

### Highlights

- Pure Raft FSM, HTTP/3/mTLS, redb persistence, cross-node actors
- Multi-Raft write scaling: **Meta-Raft coordinator** (join/catalog/saga isolated from user groups), dynamic catalog, stable shards, group migration, per-group membership
- Cross-shard saga coordinator + optional 2PC; follower/lease reads
- mTLS hot reload, reachability-driven supervisor, `trembita-ops` backup
- **Product showcases** — five standalone apps in [`examples/`](examples/README.md) (jobs, stateful workers, realtime, workflows, self-update)
- **`TrembitaApp`** + HTTP gateway (sticky sessions, TLS, drain), batch jobs, event topics, external backlog, workload governor — [getting-started](docs/getting-started.md)
- **Self-update coordinator** — leader reconcile + local executor ([upgrade-coordinator](docs/decisions/upgrade-coordinator.md))
- Design decision records — [docs/decisions/](docs/decisions/)

### Scope boundaries

These are explicit non-goals, not gaps in quality:

- Linearizable actor `ask`, global cross-shard serializable isolation
- See [docs/status.md](docs/status.md) for the full deferred list and known limits (R1–R6)

---

## Quick start

```sh
cd trembita
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

**Read next:** [docs/status.md](docs/status.md) → [docs/architecture.md](docs/architecture.md) → [crates/trembita/src/builder.rs](crates/trembita/src/builder.rs)

**`trembita-node` env:** `TREMBITA_NODE_ID`, `TREMBITA_LISTEN` (`:7443`), `TREMBITA_ADMIN` (`:8080`), `TREMBITA_PEERS`, `TREMBITA_JOIN_SEEDS`, `TREMBITA_DISCOVERY` — [docs/certs.md](docs/certs.md)

**Job queue on `trembita-node`** (optional, requires `TREMBITA_DATA_DIR`):

| Var | Meaning |
|-----|---------|
| `TREMBITA_DATA_DIR` | Persistent redb directory (Raft log + queue file) |
| `TREMBITA_JOB_QUEUE` | Enable durable queue stream name (e.g. `jobs`) |
| `TREMBITA_JOB_QUEUE_LEASE_SECS` | Lease visibility timeout (default `60`) |

See [job-queue](docs/decisions/job-queue.md) and [protocol.md](docs/protocol.md#job-queue).

---

## Quick API sketch (product)

```rust
use std::time::Duration;
use trembita::{TrembitaApp, TrembitaConfigure, GatewayOpts, JobOpts, RunOpts, consumer};

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
    .configure(TrembitaConfigure {
        admin_addr: Some("127.0.0.1:8080".parse()?),
        ..TrembitaConfigure::default()
    })
    .gateway(GatewayOpts::new("127.0.0.1:8090".parse()?))
    .run(RunOpts::default().with_wait_queue("jobs"))
    .await?;
```

## Cluster APIs (`trembita::cluster`)

Custom [`StateMachine`](crates/trembita-core/src/lib.rs) wiring, multi-Raft, keyed client, and direct queue access live in [`trembita::cluster`](crates/trembita/src/cluster.rs). Product apps use [`TrembitaApp`](docs/getting-started.md) instead.

See [multi-raft](docs/decisions/multi-raft.md), [job-queue](docs/decisions/job-queue.md), and [getting-started](docs/getting-started.md).

## Design principles

- **Library-first** — embed `trembita`, no sidecar ([deployment-model](docs/decisions/deployment-model.md))
- **Linearizable SM** — `propose` / `query` via Raft ([client-and-routing](docs/decisions/client-and-routing.md))
- **Transparent routing** — any node forwards to leader ([client-and-routing](docs/decisions/client-and-routing.md))
- **Pure core** — `trembita-core` is I/O-free; ports & adapters ([architecture-style](docs/decisions/architecture-style.md))
- **Testable** — sim-first + E2E ([testing-strategy](docs/decisions/testing-strategy.md))

## Install

```toml
[dependencies]
trembita = "0.5"
```

Product apps: enable `http-jobs` for HTTP job routes and `dev-certs` for local QUIC without PEM files — see [getting-started](docs/getting-started.md).

## Workspace crates

| Crate | Purpose |
|-------|---------|
| [`trembita`](crates/trembita) | Facade — `TrembitaApp` + `trembita::cluster` |
| [`trembita-http`](crates/trembita-http) | Product HTTP (`POST /jobs/{stream}` → 202) |
| [`trembita-core`](crates/trembita-core) | Pure Raft FSM + shard planners + reference [`kv`](crates/trembita-core/src/kv.rs) StateMachine |
| [`trembita-proto`](crates/trembita-proto) | Wire types + codec |
| [`trembita-storage`](crates/trembita-storage) | Durable log, snapshots |
| [`trembita-net`](crates/trembita-net) | HTTP/3 / QUIC + mTLS |
| [`trembita-runtime`](crates/trembita-runtime) | Node runtime, registry, supervisor |
| [`trembita-jobs`](crates/trembita-jobs) | Job queue, autoscale, backlog |
| [`trembita-events`](crates/trembita-events) | Durable pub/sub topics |
| [`trembita-actor-store`](crates/trembita-actor-store) | Stateful actor workflow keys |
| [`trembita-client`](crates/trembita-client) | Client, saga, keyed/batch APIs |
| [`trembita-macros`](crates/trembita-macros) | Derive macros |
| [`trembita-store-redis`](crates/trembita-store-redis) | Redis `ActorStateStore` |
| [`trembita-dashboard`](crates/trembita-dashboard) | Admin + observability |
| [`trembita-sim`](crates/trembita-sim) | Deterministic sim harness |
| [`trembita-ops`](crates/trembita-tools) | Backup/restore CLI |
| [`trembita-node`](crates/trembita-tools) | Reference binary (repo/e2e only) |

## Documentation map

| Doc | When to read |
|-----|----------------|
| [docs/getting-started.md](docs/getting-started.md) | **Product app tutorial** (TrembitaApp, no Redis) |
| [docs/scenarios/README.md](docs/scenarios/README.md) | **Four product scenarios** |
| [docs/status.md](docs/status.md) | **Current capabilities and limits** |
| [CONTRIBUTING.md](CONTRIBUTING.md) | **How to contribute** (humans) |
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
