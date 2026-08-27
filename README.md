# craft

**A distributed Raft + actor framework for Rust: one codebase, N nodes, elastic and self-healing.**

[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)
[![rust](https://img.shields.io/badge/rustc-1.98%2B-orange.svg)](#msrv)

> **Paused since 2026-07-06.** v0.1 milestone is feature-complete per [backlog](docs/backlog.md); not production-hardened. This README is written so you can pick the project up months later without re-reading 32 ADRs.

---

## Why this exists

**Problem:** Running a stateful app on multiple VPS/K8s nodes usually means bolting together separate pieces — a consensus library, an actor runtime, a transport layer, mTLS, membership, observability — and wiring them yourself.

**Idea:** Embed consensus + actors in *your* binary. Same artifact on every node; the cluster bootstraps, elects a leader, replicates a linearizable state machine, and hosts supervised actors that can message and migrate across nodes. No sidecar, no separate control plane.

**Not the same as [lmrc-cloud](https://gitlab.com/lemarco/lmrc-cloud):** `lmrc-cloud` is a Kubernetes-native platform (controller, operators, GitLab CI deploy). `craft` is a **library** for apps that *become* the cluster node. Different deployment model; no shared code today.

**Built in:** intense 2-day sprint (2026-07-05 → 2026-07-06), 46 commits, then paused.

---

## Current status (read this first when returning)

| | |
|---|---|
| **Version** | `0.1.0` (pre-1.0, API may change on minor bumps) |
| **Last commit** | 2026-07-06 — `fix(scale): retry forwarded scale while leadership settles` |
| **Maturity** | Advanced prototype — E2E, chaos tests, benchmarks, release CI exist; README still says *not for production* |
| **Backlog** | Waves 0–4 + most post-v1 items marked **done** in [docs/backlog.md](docs/backlog.md) |

### What works (v0.1)

- Pure Raft FSM (`craft-core`) — election, replication, joint-consensus membership, snapshots, ReadIndex + lease reads + **follower reads**
- HTTP/3 / QUIC + mTLS between nodes (`craft-net`)
- Durable log via redb (`craft-storage`)
- Cross-node actors, auto-spawn on join, Redis actor state (optional)
- Transparent client routing (any node → leader)
- Dynamic join via seed addresses / DNS discovery
- `craft-node` reference binary + K8s StatefulSet manifests
- Docker e2e (3-node cluster, partition/heal chaos), Criterion benches, soak harness
- 32 accepted ADRs documenting every design choice

### What's deferred (good next steps if you resume)

From [future-work-and-risks](docs/decisions/future-work-and-risks.md) and [backlog post-v1](docs/backlog.md):

1. **Cross-node group migration RPC** — multi-Raft rebalancing adopts/retires locally; wire migration between physical nodes remains deferred ([write-sharding-multi-raft](docs/decisions/write-sharding-multi-raft.md))
2. **Worker migration on node failure** — leader supervisor reconciles against `reachable_nodes()`; crashed hosts lose workers and survivors respawn them without a `ConfChange` ([liveness-vs-membership](docs/decisions/liveness-vs-membership.md))
3. **Production hardening** — real-world soak, fuzzing, docs.rs publish, crates.io release (tooling ready in [docs/releasing.md](docs/releasing.md))

---

## 5-minute re-entry

```sh
cd craft
./scripts/quality-gate-pre-commit.sh   # manual; lefthook runs fmt + this on commit
./scripts/quality-gate-pre-push.sh     # check + tests + doctests

# Git hooks (recommended once per clone):
lefthook install

# Examples
cargo run -p craft --example kv_store
cargo run -p craft --example three_node_local
cargo run -p craft --example actors_cluster

# Real QUIC + mTLS cluster (needs Docker)
./e2e/run.sh
./e2e/chaos.sh
./e2e/cert_renew.sh
```

**Read next (in order):**

1. [docs/architecture.md](docs/architecture.md) — crate graph, data flow
2. [docs/backlog.md](docs/backlog.md) — what's done vs deferred (check ✅ columns)
3. [docs/decisions/future-work-and-risks.md](docs/decisions/future-work-and-risks.md) — known limits
4. [crates/craft/src/builder.rs](crates/craft/src/builder.rs) — main public API entry (`CraftCluster::builder`)

**Reference binary env vars** (`craft-node`): `CRAFT_NODE_ID`, `CRAFT_LISTEN` (default `:7443`), `CRAFT_ADMIN` (`:8080`), `CRAFT_PEERS`, `CRAFT_JOIN_SEEDS`, `CRAFT_DISCOVERY`, cert paths — see [docs/certs.md](docs/certs.md).

---

## Quick API sketch

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

## Why craft (design principles)

- **Library-first.** No sidecar — `craft` is a crate you embed ([deployment-model](docs/decisions/deployment-model.md)).
- **One dependency.** The `craft` facade re-exports the stable API.
- **Linearizable.** Replicated `StateMachine` with leader writes + `ReadIndex`/lease reads ([read-consistency](docs/decisions/read-consistency.md)).
- **Transparent routing.** Clients hit any node; requests forward to the leader ([client-routing](docs/decisions/client-routing.md)).
- **Cross-node actors.** Supervised actors across nodes; leader auto-places workers ([cross-node-actors](docs/decisions/cross-node-actors.md), [auto-spawn-on-join](docs/decisions/auto-spawn-on-join.md)).
- **Secure by default.** mTLS on every inter-node connection ([security](docs/decisions/security.md)).
- **Observable.** Health/admin endpoints + dashboard ([health-admin-port](docs/decisions/health-admin-port.md), [observability](docs/decisions/observability.md)).
- **Testable.** `craft-core` is I/O-free FSM; effects-as-data ([architecture-style](docs/decisions/architecture-style.md)).

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
| [`craft`](crates/craft) | **Start here.** Facade + `CraftCluster` builder |
| [`craft-core`](crates/craft-core) | Pure Raft consensus FSM (no I/O) |
| [`craft-proto`](crates/craft-proto) | Wire types + `postcard` codec |
| [`craft-storage`](crates/craft-storage) | Durable log, hard state, snapshots |
| [`craft-net`](crates/craft-net) | HTTP/3 (QUIC) transport with mTLS |
| [`craft-actor`](crates/craft-actor) | Actor runtime, registry, supervision |
| [`craft-client`](crates/craft-client) | In-process + remote client API |
| [`craft-macros`](crates/craft-macros) | Derive macros (`StateMachine`, `remote_actor`) |
| [`craft-store-redis`](crates/craft-store-redis) | Redis-backed `ActorStateStore` |
| [`craft-dashboard`](crates/craft-dashboard) | Observability dashboard + admin |
| [`craft-sim`](crates/craft-sim) | Deterministic simulation harness |
| [`craft-node`](crates/craft-node) | Reference binary (env-driven config) |

## Examples & tests

```sh
cargo run -p craft --example kv_store          # single-node KV
cargo run -p craft --example three_node_local  # 3 nodes + forwarding
cargo run -p craft --example actors_cluster    # cross-node actors
cargo run -p craft --example vps_join --features dev-certs  # elastic join

./e2e/run.sh    # 3-node QUIC/mTLS + failover
./e2e/chaos.sh  # network partition + heal
./e2e/cert_renew.sh  # PEM reissue + hot reload (SIGHUP / poll)

cargo bench --manifest-path benchmarks/Cargo.toml
SOAK_SECS=60 cargo run --release --manifest-path benchmarks/Cargo.toml --bin soak
```

Certs for real deploy: [`examples/certs/generate.sh`](examples/certs/generate.sh) — [docs/certs.md](docs/certs.md).

## Quality gates (local)

| When | What runs |
|------|-----------|
| **pre-commit** | parallel: `fmt --check`, shellcheck, `cargo doc` → piped: clippy → check |
| **pre-push** | piped: check → tests (nextest if installed) → doctests → release build |
| **commit-msg** | conventional commits (`feat`, `fix`, `chore`, …) |
| **CI fast lane** | fmt, clippy, nextest, doctests (see `.gitlab-ci.yml`) |

Setup: `lefthook install` · manual: `./scripts/quality-gate-pre-commit.sh` / `quality-gate-pre-push.sh`  
Bypass: `LEFTHOOK=0 git commit|push …` · lock issues: `./scripts/cargo-status.sh`

## Documentation map

| Doc | When to read |
|-----|----------------|
| [docs/architecture.md](docs/architecture.md) | System overview |
| [docs/backlog.md](docs/backlog.md) | Implementation status |
| [docs/testing-coverage.md](docs/testing-coverage.md) | Test inventory, coverage matrix, known gaps |
| [docs/protocol.md](docs/protocol.md) | HTTP/3 routes, wire format |
| [docs/decisions/](docs/decisions/) | 32 ADRs — full design rationale |
| [docs/releasing.md](docs/releasing.md) | crates.io publish workflow |
| [CHANGELOG.md](CHANGELOG.md) | Version history |
| [docs.rs/craft](https://docs.rs/craft) | API reference (when published) |

## MSRV

Minimum Supported Rust Version is **1.98**. MSRV bumps are a minor-version event ([library-and-publishing](docs/decisions/library-and-publishing.md)).

## License

Dual-licensed under MIT OR Apache-2.0 — see [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE).
