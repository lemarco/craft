# ADR 027: Future work, deferrals & known risks

**Status:** Accepted  
**Date:** 2026-07-05

## Context

Low-priority items were deferred throughout the design. This ADR records them explicitly, states the **v1 stance**, and captures **known risks** so nothing is silently lost.

## Decision

The table below tracks each item. Several originally-deferred items have since
been implemented (a **cheap safeguard adopted now** — peer connection isolation,
see Risk R2 — plus the post-v1 increments marked **Done**).

### Deferred features

| # | Item | Status | Notes |
|---|------|--------|-------|
| 1 | **Follower reads** | **Done** | etcd-style linearizable reads on followers: `ReadIndexConfirm` → apply barrier → local `StateMachine::query` ([ADR 005](005-read-consistency.md)). Writes still forward to the leader (ADR 003). Lease reads remain leader-only. |
| 2 | **Lease reads** | **Done** | Leader lease reads implemented in `craft-core` and the driver fast-path ([ADR 005](005-read-consistency.md)): a valid, quorum-confirmed lease (`election_timeout_min / 2` ticks) serves `query` with no ReadIndex round-trip; conservative lease bound leaves drift headroom |
| 3 | **Gossip discovery** | **Done** | Bootstrap generalized from a single `JOIN_ADDR` to a resilient **seed set** with peer-book gossip, plus DNS-based discovery (`craft::discovery`) for orchestrated environments ([ADR 007](007-discovery.md)) |
| 4 | **Dev-only JSON wire** | **Done** | Build-time `craft-proto/json-wire` feature swaps the wire codec from `postcard` to human-readable JSON for debugging (`WIRE_CODEC` reports the active format). Never for production |
| 5 | **Write sharding / multi-Raft** | Foundation | Pure routing foundation landed (`craft-core::shard`; [ADR 031](031-write-sharding-multi-raft.md)); full multi-group runtime wiring deferred (see R1) |
| 6 | **K8s / cloud integrations** | **Done** | `deploy/` Dockerfile + Kubernetes StatefulSet/headless-service manifests wired to `/health` `/ready` ([ADR 025](025-health-admin-port.md)), ordinal-derived node ids, and DNS discovery |
| 7 | **QUIC traffic priorities** | **Done** | v1 per-class connection isolation (R2) extended with per-traffic-class token-bucket rate limiting (`craft_net::TrafficPolicy`) so bulk client/actor traffic cannot starve consensus |

### Adopted now (cheap safeguard)

**Peer RPC connection isolation** — see Risk R2. Consensus traffic (`/peer/wire`) uses a **dedicated QUIC connection**, separate from client (`/client/wire`) and actor (`/actor/*`) traffic on the same listener/port. No priority scheduler yet, just connection separation so heartbeats/appends are not head-of-line blocked by bulk client/actor payloads.

---

## Known risks

### R1 — Write throughput ceiling (single Raft log)

Adding VPSes improves **fault tolerance** and **actor compute**, **not** linear write throughput — all writes funnel through one leader + one log ([ADR 008](008-scale-targets.md)).

- **Mitigation path:** write sharding / multi-Raft (#5) — routing foundation landed ([ADR 031](031-write-sharding-multi-raft.md)); runtime wiring deferred.
- **v1 guidance:** document clearly; keep commands small; use Redis ([ADR 021](021-actor-state-redis.md)) for high-churn workflow state that does not need consensus.

### R2 — Consensus starvation on shared QUIC listener

Peer, client, and actor traffic share one port ([ADR 010](010-wire-transport.md)). Heavy client/actor payloads could delay Raft heartbeats → spurious elections.

- **v1 safeguard (adopted):** separate QUIC connection for peer RPC (above).
- **Adopted since:** per-traffic-class token-bucket rate limiting (`craft_net::TrafficPolicy`, opt-in) throttles bulk client/actor sends so consensus RPCs are never starved on the shared socket. QUIC has no cross-connection stream priority, so admission control is the effective lever.

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

- **Mitigation:** cert script + docs; **hot reload + step-ca / cert-manager examples** ([ADR 034](034-cert-automation.md)).

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
