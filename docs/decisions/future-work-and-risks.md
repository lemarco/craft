# Future work & known risks

**Status:** Accepted  
**Date:** 2026-07-05  
**Updated:** 2026-08-28 — consolidated with [status.md](../status.md)

## Context

Structural limits and mitigations for crafty. Shipped capabilities are listed in [status.md](../status.md); this ADR records **risks that remain by design** and **safeguards in place**.

## Safeguards (adopted)

**Peer RPC connection isolation** — consensus traffic (`/peer/wire`) uses a dedicated QUIC connection, separate from client (`/client/wire`) and actor (`/actor/*`) traffic on the same listener/port.

**Traffic admission control** — opt-in per-traffic-class token-bucket rate limiting (`crafty_net::TrafficPolicy` / `RateLimiter`) so bulk client/actor sends cannot starve consensus RPCs.

## Known risks

### R1 — Write throughput ceiling (per Raft group)

Adding VPSes improves **fault tolerance** and **actor compute**, not linear write throughput through a **single** Raft log ([cluster-elasticity](cluster-elasticity.md#scale-targets)).

- **Mitigation (shipped):** multi-Raft — partition keys across groups; add groups via `add_raft_groups` ([multi-raft](multi-raft.md)).
- **Guidance:** keep commands small; use Redis ([actor-state-redis](actor-state-redis.md)) for high-churn workflow state outside consensus.

### R2 — Consensus starvation on shared QUIC listener

Peer, client, and actor traffic share one port ([wire-protocol](wire-protocol.md)). Heavy payloads could delay heartbeats.

- **Mitigation (shipped):** dedicated peer connection + optional `TrafficPolicy` throttling on client/actor classes.

### R3 — Directory eventual consistency

Actor directory ([cross-node-actors](cross-node-actors.md)) is eventually consistent; brief stale entries after node changes.

- **Mitigation:** TTL + liveness; `DirectoryPolicy::ReadYourWrites`; `ClusterRef` retries after directory update ([actor-routing](actor-routing.md)).

### R4 — Stateful actor durability depends on external store

On crash, actor memory not in Redis ([actor-state-redis](actor-state-redis.md)) is lost.

- **Mitigation:** write-through to `ActorStateStore`; consensus data via `propose` → `StateMachine`.

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
