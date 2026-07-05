# ADR 027: Future work, deferrals & known risks

**Status:** Accepted  
**Date:** 2026-07-05

## Context

Low-priority items were deferred throughout the design. This ADR records them explicitly, states the **v1 stance**, and captures **known risks** so nothing is silently lost.

## Decision

All items below are **deferred from v1**, except one **cheap safeguard adopted now** (peer connection isolation, see Risk R2).

### Deferred features

| # | Item | v1 stance | Revisit when |
|---|------|-----------|--------------|
| 1 | **Follower reads** | Deferred — reads use leader ReadIndex ([ADR 005](005-read-consistency.md)) | Read load exceeds leader capacity |
| 2 | **Lease reads** | Deferred — ReadIndex only (safer, no clock assumptions) | Read latency becomes critical |
| 3 | **Gossip discovery** | Deferred — explicit `JOIN_ADDR` ([ADR 007](007-discovery.md)) | Dynamic/cloud environments need auto-discovery |
| 4 | **Dev-only JSON wire** | Skip likely — admin introspection ([ADR 026](026-observability.md)) covers debugging | Only if non-Rust wire debugging demanded |
| 5 | **Write sharding / multi-Raft** | Deferred — **primary future write-scaling path** (see R1) | Single-leader write throughput is the bottleneck |
| 6 | **K8s / cloud integrations** | Deferred — admin `/health` `/ready` ([ADR 025](025-health-admin-port.md)) makes it possible externally | Users want managed orchestration |
| 7 | **QUIC traffic priorities** | Partial safeguard now (R2); full tuning deferred | Consensus degrades under client load |

### Adopted now (cheap safeguard)

**Peer RPC connection isolation** — see Risk R2. Consensus traffic (`/peer/wire`) uses a **dedicated QUIC connection**, separate from client (`/client/wire`) and actor (`/actor/*`) traffic on the same listener/port. No priority scheduler yet, just connection separation so heartbeats/appends are not head-of-line blocked by bulk client/actor payloads.

---

## Known risks

### R1 — Write throughput ceiling (single Raft log)

Adding VPSes improves **fault tolerance** and **actor compute**, **not** linear write throughput — all writes funnel through one leader + one log ([ADR 008](008-scale-targets.md)).

- **Mitigation path:** write sharding / multi-Raft (deferred #5).
- **v1 guidance:** document clearly; keep commands small; use Redis ([ADR 021](021-actor-state-redis.md)) for high-churn workflow state that does not need consensus.

### R2 — Consensus starvation on shared QUIC listener

Peer, client, and actor traffic share one port ([ADR 010](010-wire-transport.md)). Heavy client/actor payloads could delay Raft heartbeats → spurious elections.

- **v1 safeguard (adopted):** separate QUIC connection for peer RPC (above).
- **Future:** HTTP/3 stream priorities / per-class connections / rate limits.

### R3 — Directory eventual consistency

Actor directory ([ADR 013](013-cross-node-actors.md)) is eventually consistent; brief stale entries after node changes.

- **Mitigation:** TTL + liveness from Raft membership; `ClusterRef` retries after directory update.

### R4 — Stateful actor durability depends on Redis

On crash, actor memory not in Redis ([ADR 021](021-actor-state-redis.md)) is lost.

- **Mitigation:** document write-through pattern; consensus data must use `propose` → `StateMachine`.

### R5 — Observability performance cost

Deep introspection/tracing is costlier than BEAM-native ([ADR 026](026-observability.md)).

- **Mitigation:** metrics + high-level telemetry always-on; per-message tracing opt-in; bounded broadcast (drop, never block).

### R6 — mTLS operational burden

Per-node and per-client certs, manual rotation ([ADR 006](006-security.md), [ADR 024](024-cert-provisioning.md)).

- **Mitigation:** cert script + docs; ACME/step-ca integration is future work.

---

## Consequences

- v1 scope stays focused; deferrals are intentional and documented
- Two structural risks (R1 write ceiling, R2 consensus starvation) are acknowledged; R2 has a cheap v1 mitigation
- No silent gaps for future contributors

## Related

- [008-scale-targets.md](008-scale-targets.md)
- [010-wire-transport.md](010-wire-transport.md)
- [005-read-consistency.md](005-read-consistency.md)
- [007-discovery.md](007-discovery.md)
- [021-actor-state-redis.md](021-actor-state-redis.md)
- [026-observability.md](026-observability.md)
