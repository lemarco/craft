# Documentation

Architecture and decision records for the distributive Raft actor system.

## Decision records

| ID | Topic | Status |
|----|-------|--------|
| [001](decisions/001-state-machine.md) | State machine API (trait + macros) | **Accepted** |
| [002](decisions/002-client-api.md) | Client API (Rust-native, no gRPC) | **Accepted** |
| [010](decisions/010-wire-transport.md) | Wire transport (HTTP/3 everywhere) | **Accepted** |
| [011](decisions/011-serialization.md) | Wire serialization (postcard) | **Accepted** |
| [006](decisions/006-security.md) | TLS / mTLS (peers + client wire) | **Accepted** |
| [003](decisions/003-client-routing.md) | Client routing (transparent forward) | **Accepted** |
| [004](decisions/004-deployment-model.md) | Deployment (library-first framework, VPS) | **Accepted** |
| [012](decisions/012-elastic-cluster.md) | Elastic cluster (incremental join, actors) | **Accepted** |
| [013](decisions/013-cross-node-actors.md) | Cross-node actors (v1) | **Accepted** |
| [014](decisions/014-one-worker-per-vps.md) | One worker per VPS (production) | **Accepted** |
| [007](decisions/007-discovery.md) | Cluster discovery (JOIN_ADDR + Raft config) | **Accepted** |
| [005](decisions/005-read-consistency.md) | Read consistency (ReadIndex / linearizable) | **Accepted** |
| [008](decisions/008-scale-targets.md) | Scale targets (1:1 worker:VPS) | **Accepted** |
| [009](decisions/009-naming.md) | Naming (`craft-*` + facade `craft`) | **Accepted** |
| [015](decisions/015-auto-spawn-on-join.md) | Auto-spawn workers on VPS join | **Accepted** |
| [016](decisions/016-membership-early.md) | Joint-consensus membership early | **Accepted** |
| [017](decisions/017-join-rpc.md) | Join RPC `/cluster/join` | **Accepted** |
| [018](decisions/018-supervisor-leader.md) | Leader-only ClusterSupervisor | **Accepted** |
| [019](decisions/019-cluster-routing.md) | Cluster routing (RR + keyed) | **Accepted** |
| [020](decisions/020-join-version-skew.md) | Join version skew (hard reject) | **Accepted** |
| [021](decisions/021-actor-state-redis.md) | Stateful actors → Redis / external store | **Accepted** |
| [022](decisions/022-drain-timeout.md) | Drain timeout (default 60s, configurable) | **Accepted** |
| [023](decisions/023-default-port.md) | Default port 7443/udp | **Accepted** |
| [024](decisions/024-cert-provisioning.md) | Cert script + docs | **Accepted** |
| [025](decisions/025-health-admin-port.md) | Health/admin HTTP port (`:8080`) | **Accepted** |
| [026](decisions/026-observability.md) | Observability & monitoring (BEAM-style) | **Accepted** |
| [027](decisions/027-future-work-and-risks.md) | Future work, deferrals & known risks | **Accepted** |
| [028](decisions/028-library-and-publishing.md) | Library distribution & publishing | **Accepted** |
| [029](decisions/029-testing-strategy.md) | Testing strategy (sim-first, containers, E2E) | **Accepted** |
| [030](decisions/030-architecture-style.md) | Architecture style (pragmatic ports & adapters) | **Accepted** |

## Architecture

- [architecture.md](architecture.md) — system design
- [protocol.md](protocol.md) — HTTP/3 routes and postcard bodies
- [certs.md](certs.md) — mTLS provisioning for VPS
- [backlog.md](backlog.md) — full implementation backlog + parallel tracks
- [open-questions.md](open-questions.md) — all resolved

## How we decide

Each open record lists **context**, **options**, and a **recommendation**. When you pick an option, we update the record to **Accepted** and note the choice here.
