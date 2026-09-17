# Future work & known risks

**Status:** Accepted  
**Date:** 2026-07-05  

## Context

Structural limits and mitigations for trembita. Shipped capabilities are listed in [status.md](../status.md); this ADR records **risks that remain by design** and **safeguards in place**.

## Safeguards (adopted)

**Peer RPC connection isolation** — consensus traffic (`/peer/wire`) uses a dedicated QUIC connection, separate from client (`/client/wire`) and actor (`/actor/*`) traffic on the same listener/port.

**Traffic admission control** — opt-in per-traffic-class token-bucket rate limiting (`trembita_net::TrafficPolicy` / `RateLimiter`) so bulk client/actor sends cannot starve consensus RPCs.

## Known risks

### R1 — Write throughput ceiling (per Raft group)

Adding VPSes improves **fault tolerance** and **runtime compute** (capability hosts, consumers), not linear write throughput through a **single** Raft log ([cluster-elasticity](cluster-elasticity.md#scale-targets)).

- **Mitigation (shipped):** multi-Raft — partition keys across groups; add groups via `add_raft_groups` ([multi-raft](multi-raft.md)).
- **Guidance:** keep commands small; use Redis ([actor-state-redis](actor-state-redis.md)) for high-churn workflow state outside consensus.

### R2 — Consensus starvation on shared QUIC listener

Peer, client, and actor traffic share one port ([wire-protocol](wire-protocol.md)). Heavy payloads could delay heartbeats.

- **Mitigation (shipped):** dedicated peer connection + optional `TrafficPolicy` throttling on client/actor classes.

### R3 — Directory eventual consistency

Actor directory ([cross-node-actors](cross-node-actors.md)) is eventually consistent; brief stale entries after node changes.

- **Mitigation:** TTL + liveness; `DirectoryPolicy::ReadYourWrites` (default for [`CapManifest`](../../crates/trembita/src/capability/manifest.rs) via [`TrembitaAppBuilder`](../../crates/trembita/src/app/builder.rs)); brief retry after spawn, scale, and group rebalance ([actor-routing](actor-routing.md), [structural-limits § R3](../scenarios/structural-limits.md#r3--actor-directory-is-eventually-consistent)).

### R4 — Hot handler memory is not durable by default

On crash, in-memory session/handler state not written to **cap store** or Redis ([actor-state-redis](actor-state-redis.md)) is lost.

- **Mitigation:** `OpCtx::require_store` / write-through keys; authoritative domain data via `propose` → `StateMachine`.

### R5 — Observability performance cost

Deep introspection/tracing is costlier than BEAM-native ([observability](observability.md)).

- **Mitigation:** metrics + telemetry always-on; per-message tracing opt-in; bounded broadcast (drop, never block).

### R6 — mTLS operational burden

Per-node and per-client certs, rotation ([security](security.md), [certificates](certificates.md)).

- **Mitigation:** cert script + docs; hot reload + step-ca ([certificates](certificates.md#automation--hot-reload-landed)).

## Related

- [status.md](../status.md) — current capabilities and intentional deferrals
- [cluster-elasticity](cluster-elasticity.md#scale-targets)
- [wire-protocol](wire-protocol.md)
- [client-and-routing](client-and-routing.md#read-consistency)
- [actor-state-redis.md](actor-state-redis.md)
- [observability.md](observability.md)
