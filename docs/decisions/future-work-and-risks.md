# Future work, deferrals & known risks

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
| 1 | **Follower reads** | **Done** | etcd-style linearizable reads on followers: `ReadIndexConfirm` → apply barrier → local `StateMachine::query` ([read-consistency](read-consistency.md)). Writes still forward to the leader (client-routing). Lease reads remain leader-only. |
| 2 | **Lease reads** | **Done** | Leader lease reads implemented in `craft-core` and the driver fast-path ([read-consistency](read-consistency.md)): a valid, quorum-confirmed lease (`election_timeout_min / 2` ticks) serves `query` with no ReadIndex round-trip; conservative lease bound leaves drift headroom |
| 3 | **Gossip discovery** | **Done** | Bootstrap generalized from a single `JOIN_ADDR` to a resilient **seed set** with peer-book gossip, plus DNS-based discovery (`craft::discovery`) for orchestrated environments ([discovery](discovery.md)) |
| 4 | **Dev-only JSON wire** | **Done** | Build-time `craft-proto/json-wire` feature swaps the wire codec from `postcard` to human-readable JSON for debugging (`WIRE_CODEC` reports the active format). Never for production |
| 5 | **Write sharding / multi-Raft** | Foundation | Pure routing foundation landed (`craft-core::shard`; [write-sharding-multi-raft](write-sharding-multi-raft.md)); full multi-group runtime wiring deferred (see R1) |
| 6 | **K8s / cloud integrations** | **Done** | `deploy/` Dockerfile + Kubernetes StatefulSet/headless-service manifests wired to `/health` `/ready` ([health-admin-port](health-admin-port.md)), ordinal-derived node ids, and DNS discovery |
| 7 | **QUIC traffic priorities** | **Done** | v1 per-class connection isolation (R2) extended with per-traffic-class token-bucket rate limiting (`craft_net::TrafficPolicy`) so bulk client/actor traffic cannot starve consensus |

### Adopted now (cheap safeguard)

**Peer RPC connection isolation** — see Risk R2. Consensus traffic (`/peer/wire`) uses a **dedicated QUIC connection**, separate from client (`/client/wire`) and actor (`/actor/*`) traffic on the same listener/port. No priority scheduler yet, just connection separation so heartbeats/appends are not head-of-line blocked by bulk client/actor payloads.

---

## Known risks

### R1 — Write throughput ceiling (single Raft log)

Adding VPSes improves **fault tolerance** and **actor compute**, **not** linear write throughput — all writes funnel through one leader + one log ([scale-targets](scale-targets.md)).

- **Mitigation path:** write sharding / multi-Raft (#5) — routing foundation landed ([write-sharding-multi-raft](write-sharding-multi-raft.md)); runtime wiring deferred.
- **v1 guidance:** document clearly; keep commands small; use Redis ([actor-state-redis](actor-state-redis.md)) for high-churn workflow state that does not need consensus.

### R2 — Consensus starvation on shared QUIC listener

Peer, client, and actor traffic share one port ([wire-transport](wire-transport.md)). Heavy client/actor payloads could delay Raft heartbeats → spurious elections.

- **v1 safeguard (adopted):** separate QUIC connection for peer RPC (above).
- **Adopted since:** per-traffic-class token-bucket rate limiting (`craft_net::TrafficPolicy`, opt-in) throttles bulk client/actor sends so consensus RPCs are never starved on the shared socket. QUIC has no cross-connection stream priority, so admission control is the effective lever.

### R3 — Directory eventual consistency

Actor directory ([cross-node-actors](cross-node-actors.md)) is eventually consistent; brief stale entries after node changes.

- **Mitigation:** TTL + liveness from Raft membership; `ClusterRef` retries after directory update.

### R4 — Stateful actor durability depends on Redis

On crash, actor memory not in Redis ([actor-state-redis](actor-state-redis.md)) is lost.

- **Mitigation:** document write-through pattern; consensus data must use `propose` → `StateMachine`.

### R5 — Observability performance cost

Deep introspection/tracing is costlier than BEAM-native ([observability](observability.md)).

- **Mitigation:** metrics + high-level telemetry always-on; per-message tracing opt-in; bounded broadcast (drop, never block).

### R6 — mTLS operational burden

Per-node and per-client certs, manual rotation ([security](security.md), [cert-provisioning](cert-provisioning.md)).

- **Mitigation:** cert script + docs; **hot reload + step-ca / cert-manager examples** ([cert-automation](cert-automation.md)).

---

## Consequences

- v1 scope stays focused; deferrals are intentional and documented
- Two structural risks (R1 write ceiling, R2 consensus starvation) are acknowledged; R2 has a cheap v1 mitigation
- No silent gaps for future contributors

## Related

- [scale-targets.md](scale-targets.md)
- [wire-transport.md](wire-transport.md)
- [read-consistency.md](read-consistency.md)
- [discovery.md](discovery.md)
- [actor-state-redis.md](actor-state-redis.md)
- [observability.md](observability.md)
