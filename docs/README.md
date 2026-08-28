# Documentation

Architecture and decision records for the distributive Raft actor system.

**Start here:** [status.md](status.md) (current capabilities and limits) · [architecture.md](architecture.md) (crate graph)

## Decision records (40 accepted ADRs)

### Core & API

| Topic | Record |
|-------|--------|
| State machine API (trait + macros) | [state-machine](decisions/state-machine.md) |
| Client API (Rust-native, no gRPC) | [client-api](decisions/client-api.md) |
| Client routing (transparent forward) | [client-routing](decisions/client-routing.md) |
| Cluster routing (RR + keyed) | [cluster-routing](decisions/cluster-routing.md) |
| Read consistency (ReadIndex / linearizable) | [read-consistency](decisions/read-consistency.md) |
| Architecture style (ports & adapters) | [architecture-style](decisions/architecture-style.md) |
| Naming (`craft-*` + facade `craft`) | [naming](decisions/naming.md) |
| Library distribution & publishing | [library-and-publishing](decisions/library-and-publishing.md) |

### Wire, transport & security

| Topic | Record |
|-------|--------|
| Wire transport (HTTP/3 everywhere) | [wire-transport](decisions/wire-transport.md) |
| Wire serialization (postcard) | [serialization](decisions/serialization.md) |
| TLS / mTLS (peers + client wire) | [security](decisions/security.md) |
| Cert script + docs | [cert-provisioning](decisions/cert-provisioning.md) |
| mTLS automation (step-ca, cert-manager, hot reload) | [cert-automation](decisions/cert-automation.md) |
| Health/admin HTTP port (`:8080`) | [health-admin-port](decisions/health-admin-port.md) |
| Default port 7443/udp | [default-port](decisions/default-port.md) |

### Cluster lifecycle

| Topic | Record |
|-------|--------|
| Deployment (library-first framework, VPS) | [deployment-model](decisions/deployment-model.md) |
| Elastic cluster (incremental join, actors) | [elastic-cluster](decisions/elastic-cluster.md) |
| Cluster discovery (seeds + DNS) | [discovery](decisions/discovery.md) |
| Joint-consensus membership early | [membership-early](decisions/membership-early.md) |
| Join RPC `/cluster/join` | [join-rpc](decisions/join-rpc.md) |
| Leave RPC `/cluster/leave` | [leave-rpc](decisions/leave-rpc.md) |
| Join version skew (hard reject) | [join-version-skew](decisions/join-version-skew.md) |
| Leader-only ClusterSupervisor | [supervisor-leader](decisions/supervisor-leader.md) |
| Liveness signal vs membership | [liveness-vs-membership](decisions/liveness-vs-membership.md) |
| Scale targets (1:1 worker:VPS) | [scale-targets](decisions/scale-targets.md) |

### Actors

| Topic | Record |
|-------|--------|
| Cross-node actors (v1) | [cross-node-actors](decisions/cross-node-actors.md) |
| One worker per VPS (production) | [one-worker-per-vps](decisions/one-worker-per-vps.md) |
| Auto-spawn workers on VPS join | [auto-spawn-on-join](decisions/auto-spawn-on-join.md) |
| Stateful actors → Redis / external store | [actor-state-redis](decisions/actor-state-redis.md) |
| Drain timeout (default 60s, configurable) | [drain-timeout](decisions/drain-timeout.md) |
| Actor / routing UX — Tier 3 | [actor-routing-tier3](decisions/actor-routing-tier3.md) |

### Multi-Raft & write scaling

| Topic | Record |
|-------|--------|
| Write sharding / multi-Raft | [write-sharding-multi-raft](decisions/write-sharding-multi-raft.md) |
| Per-group Raft membership | [per-group-raft-membership](decisions/per-group-raft-membership.md) |
| Tier 1 multi-Raft advances | [tier1-multi-raft-advances](decisions/tier1-multi-raft-advances.md) |
| Tier 2 multi-Raft architecture | [tier2-multi-raft-architecture](decisions/tier2-multi-raft-architecture.md) |
| Cross-shard atomic transactions | [cross-shard-transactions](decisions/cross-shard-transactions.md) |
| Tier 2 production reliability | [tier2-production-reliability](decisions/tier2-production-reliability.md) |

### Quality & risks

| Topic | Record |
|-------|--------|
| Testing strategy (sim-first, containers, E2E) | [testing-strategy](decisions/testing-strategy.md) |
| Observability & monitoring | [observability](decisions/observability.md) |
| Known risks & structural limits | [future-work-and-risks](decisions/future-work-and-risks.md) |

## Operations runbooks

- [ops/backup-restore.md](ops/backup-restore.md) — `craft-ops` snapshot export/import
- [ops/rolling-upgrade.md](ops/rolling-upgrade.md) — wire N/N−1 vs app semver

## Reference

| Doc | Purpose |
|-----|---------|
| [../AGENTS.md](../AGENTS.md) | Agent/AI entry (rules, skills, quality gates) |
| [architecture.md](architecture.md) | System design |
| [protocol.md](protocol.md) | HTTP/3 routes and wire format |
| [certs.md](certs.md) | mTLS provisioning |
| [status.md](status.md) | **Current capabilities and limits** |
| [testing-coverage.md](testing-coverage.md) | Test inventory and coverage matrix |
| [releasing.md](releasing.md) | crates.io publish workflow |
| [../CHANGELOG.md](../CHANGELOG.md) | Version history |

## How we decide

Each record lists **context**, **options**, and a **decision**. Accepted records are listed above; update [status.md](status.md) when shipping or deferring capability.
