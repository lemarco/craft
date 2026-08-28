# Liveness signal, distinct from Raft membership

**Status:** Accepted (landed)  
**Date:** 2026-07-06

## Context

Supervisor placement must distinguish:

- **Membership** — committed Raft voters (changes only via `ConfChange`).
- **Liveness** — who acks heartbeats right now (changes on crash/partition without a log entry).

Conflating the two prevented crash-driven worker respawn and accurate auto-worker counts.

## Decision

### Detector (`craft-core`)

The leader records per-peer last ack tick (`last_ack_clock`) from `AppendEntries` responses:

- `reachable(window)` — self plus voters that acked within `window` ticks.
- `reachable_now()` — default window `2 × election_timeout_max`.

Properties: leader-only observation; cleared on step-down; **advisory only** — never affects commit/quorum.

### Threading (`craft-actor`, `craft`)

- `NodeStatus.reachable`, `ClusterState::reachable_nodes()` (defaults to `live_nodes()` for mocks).
- `ClusterFacts` refreshed from leader status in the facts loop.
- Tunable via `ReachabilityConfig` / phi-accrual ([tier2-production-reliability](tier2-production-reliability.md)).

### Crash-driven reconcile (landed)

`ClusterSupervisor` plans against `reachable_nodes()`:

- `manage_auto` tracks reachable node count.
- Fixed groups cap at reachable set.
- Facts-refresher triggers reconcile and directory prune on reachability changes.

`live_nodes()` remains the **committed voter** set; `reachable_nodes()` is the **liveness** view for placement.

## Consequences

**Positive:** Crash signal is free (rides heartbeats), observable, and cannot affect consensus safety.

**Negative:** False positives possible under extreme latency — mitigated by conservative default window and hysteresis tuning.

## Related

- [supervisor-leader](supervisor-leader.md)
- [auto-spawn-on-join](auto-spawn-on-join.md)
- [membership-early](membership-early.md)
- [tier2-production-reliability](tier2-production-reliability.md)
