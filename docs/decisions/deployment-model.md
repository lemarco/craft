# Deployment model — library-first framework

**Status:** Accepted  
**Date:** 2026-07-05  
**Amended:** 2026-07-05 — VPS incremental join + actor scaling vision

## Context

Should this repo ship a standalone binary, an embeddable library, or both? The product goal is a **framework** where the user writes **one application codebase** (state machine + actors + business logic), deploys it to **any VPS**, and **adds more VPS instances over time** — each new instance **joins the existing cluster** (first node, then second connects to first, and so on).

Operational model: **VPS / bare metal** — one process per node; docker-compose optional for local multi-node demos.

## Decision

**Library-first framework (Option B+).**

| Artifact | Role |
|----------|------|
| **`crafty-*` crates + `CraftyCluster` API** | Primary product — user embeds in their app |
| **`examples/`** | Four product showcases (jobs, stateful workers, realtime, workflows) — each standalone `Cargo.toml`, local + QUIC `cluster.sh` |
| **`dev/`** | Shared cluster helpers (`cluster-common.sh`), certs, optional Docker Compose per showcase |
| **`crafty-node` (optional)** | Thin wrapper around the same API for demos only — not a plugin host |

The user ships **one binary** built from their app. Production runs **N processes** (N VPSes), each process = **one Raft peer** + **local actor runtime**. Same codebase everywhere; config differs per VPS (`node_id`, listen addr, join target).

## User application shape (draft)

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let cluster = CraftyCluster::builder()
        .node_id(env("NODE_ID"))
        .listen(env("LISTEN_ADDR"))           // e.g. 0.0.0.0:7443
        .join(env_optional("JOIN_ADDR"))      // None = seed/first node; Some = join existing
        .allow_join(env_bool("RAFT_ALLOW_JOIN")) // seed/members: accept joins only when true
        .state_machine(MyAppState::default())
        .resource_profile(ResourceProfile::UseAllAvailable)
        .auto_workers([AutoWorkerSpec::new("workers", WorkerConfig::default)])
        .spawn()
        .await?;

    // Workers spawn automatically after join (auto-spawn-on-join) — no manual spawn in main
    cluster.client().propose(MyCommand::Init).await?;
    // cluster.leave().await?;  // graceful: migrates actors then removes node
    cluster.run_until_shutdown().await?;
}
```

## VPS deployment flow

```mermaid
sequenceDiagram
    participant V1 as VPS 1 (seed)
    participant V2 as VPS 2
    participant V3 as VPS 3

    V1->>V1: JOIN_ADDR unset; --allow-join → accept joins
    V2->>V1: JOIN_ADDR=vps1:7443 → join cluster (membership)
    V3->>V1: JOIN_ADDR=vps1:7443 (or any member) → join cluster
    Note over V1,V3: Same binary, same actor definitions; scale by adding VPSes
```

1. **First VPS:** no join address — becomes seed (single-node Raft until peers arrive).
2. **Next VPS:** `JOIN_ADDR` points at any live member (typically first); framework runs **join protocol** ([cluster-elasticity](cluster-elasticity.md)).
3. **Further VPSes:** same — connect to seed or any healthy peer.

Client traffic: any node (transparent forward, [client-and-routing](client-and-routing.md)). Load balancers can round-robin across VPS addresses.

## Two layers of “scale on demand”

The framework separates:

| Layer | What scales | Mechanism |
|-------|-------------|-----------|
| **Cluster** | Raft peers (VPS count) | Incremental join/leave ([cluster-elasticity](cluster-elasticity.md), [cluster-membership](cluster-membership.md#discovery)) |
| **Application** | User **actors** | 1 worker/VPS (prod); scale via new VPS ([cluster-elasticity](cluster-elasticity.md#one-worker-per-vps-production)) |

Raft gives **consistent replicated state** (via user `StateMachine`). **Actors** handle concurrent work, messages, and domain logic — scaled by spawning more actor instances (local or distributed — see elastic-cluster).

**Important:** adding Raft nodes improves **fault tolerance** and **capacity for actor work**; it does **not** linearly multiply write throughput to one Raft log. Document this in user-facing guides.

## What we do not require

- Cloud-specific orchestration or container platforms
- Dynamic `.so` plugins or separate `raft-node` config language
- Multiple Raft peers inside one OS process (dev/sim only)

## Consequences

**Positive**

- One codebase, deploy anywhere, grow cluster incrementally
- Natural fit for ractor + embed model ([state-machine](state-machine.md))
- VPS-friendly: env vars `NODE_ID`, `LISTEN_ADDR`, `JOIN_ADDR`, TLS paths

**Negative**

- Join/membership via joint consensus in v1 ([cluster-membership](cluster-membership.md))
- User must operate certs, firewall (UDP 7443), and seed node availability
- Actor placement across nodes needs explicit design ([cluster-elasticity](cluster-elasticity.md))

## Related

- [cluster-elasticity](cluster-elasticity.md) — join flow, actor scaling contract
- [cluster-membership](cluster-membership.md#discovery) — bootstrap vs join address
- [state-machine.md](state-machine.md)
