# Deployment model — library-first framework

**Status:** Accepted  
**Date:** 2026-07-05  

## Context

Should this repo ship a standalone binary, an embeddable library, or both? The product goal is a **framework** where the user writes **one application codebase** (capabilities, jobs, topics, optional custom Raft SM), deploys it to **any VPS**, and **adds more VPS instances over time** — each new instance **joins the existing cluster** (first node, then second connects to first, and so on). App-authored **`UserActor`** groups are **advanced**, not the default product path ([product-terminology](product-terminology.md)).

Operational model: **VPS / bare metal** — one process per node; docker-compose optional for local multi-node demos.

## Decision

**Library-first framework (Option B+).**

| Artifact | Role |
|----------|------|
| **`trembita` facade + `TrembitaApp`** | Primary product — user embeds in their app |
| **`examples/`** | Product showcases (jobs, stateful workers, realtime, workflows, self-update) — each standalone `Cargo.toml`, local + QUIC `cluster.sh` |
| **`dev/`** | Shared cluster helpers (`cluster-common.sh`), certs, optional Docker Compose per showcase |
| **`trembita-node` (optional)** | Reference product node from env (empty SM, ops HTTP) — e2e/Docker/3-node demos; not a plugin host |

The user ships **one binary** built from their app. Production runs **N processes** (N VPSes), each process = **one Raft peer** + **local runtime** (capability hosts, job consumers, optional advanced workers). Same codebase everywhere; config differs per VPS (`TREMBITA_*` — assigned `node-id` under `data_dir`, listen addr, join seeds).

## User application shape (draft)

```rust
use trembita::{AppManifest, TrembitaApp, TrembitaConfigure};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    trembita::init_tracing();
    TrembitaApp::from_env()?
        .manifest(
            AppManifest::new()
                .capabilities(/* CapManifest from manifest.rs */)
                .jobs(/* JobOpts */),
        )
        .configure(TrembitaConfigure::default())
        .run()
        .await?;
}
```

Cluster join, certs, and listen addresses come from **`TREMBITA_*`** ([env.md](../env.md)) — not programmatic cluster builder setters. Custom Raft state machines are maintainer-only ([public-api-1.0](public-api-1.0.md)).

## VPS deployment flow

```mermaid
sequenceDiagram
    participant V1 as VPS 1 (seed)
    participant V2 as VPS 2
    participant V3 as VPS 3

    V1->>V1: TREMBITA_JOIN_SEEDS unset; TREMBITA_ALLOW_JOIN=1 → accept joins
    V2->>V1: TREMBITA_JOIN_SEEDS=1@vps1:443 → join cluster (membership)
    V3->>V1: TREMBITA_JOIN_SEEDS=1@vps1:443 (or any member) → join cluster
    Note over V1,V3: Same binary, same manifest; scale by adding VPSes
```

1. **First VPS:** omit `TREMBITA_JOIN_SEEDS` — becomes seed (single-node Raft until peers arrive).
2. **Next VPS:** `TREMBITA_JOIN_SEEDS` points at any live member (typically first); framework runs **join protocol** ([cluster-elasticity](cluster-elasticity.md)).
3. **Further VPSes:** same — connect to seed or any healthy peer.

Client traffic: any node (transparent forward, [client-and-routing](client-and-routing.md)). Load balancers can round-robin across VPS addresses.

## Two layers of “scale on demand”

The framework separates:

| Layer | What scales | Mechanism |
|-------|-------------|-----------|
| **Cluster** | Raft peers (VPS count) | Incremental join/leave ([cluster-elasticity](cluster-elasticity.md), [cluster-membership](cluster-membership.md#discovery)) |
| **Application** | Capability groups + job consumers | Per-group placement; prod often **1 instance/VPS** ([cluster-elasticity](cluster-elasticity.md#one-worker-per-vps-production)); scale via new VPS |

Raft gives **consistent replicated state** (via user `StateMachine` when used). **Capabilities** and queues handle concurrent product work on the runtime; advanced **`UserActor`** pools use the same placement rules.

**Important:** adding Raft nodes improves **fault tolerance** and **capacity for runtime work**; it does **not** linearly multiply write throughput to one Raft log. Document this in user-facing guides.

## What we do not require

- Cloud-specific orchestration or container platforms as the **product** deployment path
- Dynamic `.so` plugins or separate `raft-node` config language
- Multiple Raft peers inside one OS process (dev/sim only)

## Out of scope for this repository

No in-tree **orchestration-platform** artifacts (charts, operators, “run on cluster X” guides). Production scale = **more VPS processes** running the **same embeddable binary**, not more replicas of a stateless Deployment. See [.cursor/rules/trembita-deployment.mdc](../../.cursor/rules/trembita-deployment.mdc) for agent/doc conventions.

## Consequences

**Positive**

- One codebase, deploy anywhere, grow cluster incrementally
- Natural fit for ractor + embed model ([state-machine](state-machine.md))
- VPS-friendly: product env in [env.md](../env.md) (`TREMBITA_LISTEN`, `TREMBITA_DATA_DIR`, `TREMBITA_CERT_DIR`, `TREMBITA_JOIN_SEEDS`, …)

**Negative**

- Join/membership uses joint consensus ([cluster-membership](cluster-membership.md))
- User must operate certs, firewall (**UDP + TCP** on `TREMBITA_LISTEN`, often 443), and seed node availability
- Actor placement across nodes needs explicit design ([cluster-elasticity](cluster-elasticity.md))

## Related

- [cluster-elasticity](cluster-elasticity.md) — join flow, actor scaling contract
- [cluster-membership](cluster-membership.md#discovery) — bootstrap vs join address
- [state-machine.md](state-machine.md)
