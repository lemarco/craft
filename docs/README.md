# Documentation

Architecture and decision records for the distributive Raft actor system.

**Start here:** [status.md](status.md) (current capabilities and limits) · [architecture.md](architecture.md) (crate graph)

## Decision records

### Core & API

| Topic | Record |
|-------|--------|
| State machine API (trait + macros) | [state-machine](decisions/state-machine.md) |
| Client API, routing & read consistency | [client-and-routing](decisions/client-and-routing.md) |
| Architecture style (ports & adapters) | [architecture-style](decisions/architecture-style.md) |
| Naming (`craft-*` + facade `craft`) | [naming](decisions/naming.md) |
| Library distribution & publishing | [library-and-publishing](decisions/library-and-publishing.md) |

### Wire, transport & security

| Topic | Record |
|-------|--------|
| HTTP/3 transport, postcard, ports & admin | [wire-protocol](decisions/wire-protocol.md) |
| TLS / mTLS (peers + client wire) | [security](decisions/security.md) |
| Certificates (manual PKI + automation) | [certificates](decisions/certificates.md) |

### Cluster lifecycle

| Topic | Record |
|-------|--------|
| Deployment (library-first framework, VPS) | [deployment-model](decisions/deployment-model.md) |
| Elasticity, workers, auto-spawn & supervisor | [cluster-elasticity](decisions/cluster-elasticity.md) |
| Membership, discovery, join/leave & liveness | [cluster-membership](decisions/cluster-membership.md) |

### Actors

| Topic | Record |
|-------|--------|
| Cross-node actors (v1) | [cross-node-actors](decisions/cross-node-actors.md) |
| Stateful actors → Redis / external store | [actor-state-redis](decisions/actor-state-redis.md) |
| Durable job queue (mailbox vs backlog, autoscale) | [job-queue](decisions/job-queue.md) |
| Drain timeout (default 60s, configurable) | [drain-timeout](decisions/drain-timeout.md) |
| Actor / routing UX — Tier 3 | [actor-routing-tier3](decisions/actor-routing-tier3.md) |

### Multi-Raft & write scaling

| Topic | Record |
|-------|--------|
| Multi-Raft (sharding, catalog, meta-Raft, saga, 2PC, ops) | [multi-raft](decisions/multi-raft.md) |

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
