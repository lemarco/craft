# Documentation

Architecture and decision records for the distributive Raft actor system.

## Decision records

| Topic | Status |
|-------|--------|
| [State machine API (trait + macros)](decisions/state-machine.md) | **Accepted** |
| [Client API (Rust-native, no gRPC)](decisions/client-api.md) | **Accepted** |
| [Wire transport (HTTP/3 everywhere)](decisions/wire-transport.md) | **Accepted** |
| [Wire serialization (postcard)](decisions/serialization.md) | **Accepted** |
| [TLS / mTLS (peers + client wire)](decisions/security.md) | **Accepted** |
| [Client routing (transparent forward)](decisions/client-routing.md) | **Accepted** |
| [Deployment (library-first framework, VPS)](decisions/deployment-model.md) | **Accepted** |
| [Elastic cluster (incremental join, actors)](decisions/elastic-cluster.md) | **Accepted** |
| [Cross-node actors (v1)](decisions/cross-node-actors.md) | **Accepted** |
| [One worker per VPS (production)](decisions/one-worker-per-vps.md) | **Accepted** |
| [Cluster discovery (JOIN_ADDR + Raft config)](decisions/discovery.md) | **Accepted** |
| [Read consistency (ReadIndex / linearizable)](decisions/read-consistency.md) | **Accepted** |
| [Scale targets (1:1 worker:VPS)](decisions/scale-targets.md) | **Accepted** |
| [Naming (`craft-*` + facade `craft`)](decisions/naming.md) | **Accepted** |
| [Auto-spawn workers on VPS join](decisions/auto-spawn-on-join.md) | **Accepted** |
| [Joint-consensus membership early](decisions/membership-early.md) | **Accepted** |
| [Join RPC `/cluster/join`](decisions/join-rpc.md) | **Accepted** |
| [Leader-only ClusterSupervisor](decisions/supervisor-leader.md) | **Accepted** |
| [Cluster routing (RR + keyed)](decisions/cluster-routing.md) | **Accepted** |
| [Join version skew (hard reject)](decisions/join-version-skew.md) | **Accepted** |
| [Stateful actors → Redis / external store](decisions/actor-state-redis.md) | **Accepted** |
| [Drain timeout (default 60s, configurable)](decisions/drain-timeout.md) | **Accepted** |
| [Default port 7443/udp](decisions/default-port.md) | **Accepted** |
| [Cert script + docs](decisions/cert-provisioning.md) | **Accepted** |
| [Health/admin HTTP port (`:8080`)](decisions/health-admin-port.md) | **Accepted** |
| [Observability & monitoring (BEAM-style)](decisions/observability.md) | **Accepted** |
| [Future work, deferrals & known risks](decisions/future-work-and-risks.md) | **Accepted** |
| [Library distribution & publishing](decisions/library-and-publishing.md) | **Accepted** |
| [Testing strategy (sim-first, containers, E2E)](decisions/testing-strategy.md) | **Accepted** |
| [Architecture style (pragmatic ports & adapters)](decisions/architecture-style.md) | **Accepted** |
| [mTLS automation (step-ca, cert-manager, hot reload)](decisions/cert-automation.md) | **Accepted** |
| [Write sharding / multi-Raft](decisions/write-sharding-multi-raft.md) | **Accepted** |
| [Liveness signal vs membership](decisions/liveness-vs-membership.md) | **Accepted** |
| [Per-group Raft membership (multi-Raft)](decisions/per-group-raft-membership.md) | **Accepted** |

## Architecture

- [../AGENTS.md](../AGENTS.md) — agent/AI entry point (rules, skills, quality gates)
- [architecture.md](architecture.md) — system design
- [protocol.md](protocol.md) — HTTP/3 routes and postcard bodies
- [certs.md](certs.md) — mTLS provisioning for VPS
- [backlog.md](backlog.md) — full implementation backlog + parallel tracks
- [testing-coverage.md](testing-coverage.md) — test inventory, coverage matrix, known gaps
- [open-questions.md](open-questions.md) — all resolved

## How we decide

Each open record lists **context**, **options**, and a **recommendation**. When you pick an option, we update the record to **Accepted** and note the choice here.
