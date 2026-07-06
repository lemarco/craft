# ADR 032: Liveness signal, distinct from Raft membership

**Status:** Accepted (foundation landed; crash-driven reconcile deferred)
**Date:** 2026-07-06

## Context

The supervisor reconciles actor placement against a [`ClusterState`] whose
`live_nodes()` returned the **committed Raft voter set**. That conflates two
different questions:

- **Membership** — who is *configured* to be in the cluster. Changes only on a
  deliberate `ConfChange` (join/leave, [ADR 016](016-membership.md)).
- **Liveness** — who is *actually reachable right now*. Changes the moment a
  node crashes or partitions, with no log entry.

Because `live_nodes()` tracked membership, a **crashed-but-still-voter** node
was reported as live. Nothing could notice the crash until an operator (or some
external controller) proposed a membership change to remove it. This blocked
crash-driven **auto-migrate / respawn**: the supervisor had no signal that a
worker's host had died, so it could not move that worker elsewhere
([ADR 015](015-managed-groups.md) managed groups, [ADR 018](018-supervision.md)
reconcile). It also meant `manage_auto` (one worker per live node) tracked
*configured* nodes, not *reachable* ones.

Overloading one accessor for both meanings is the layering smell called out in
the v1 review ("stop overloading `NodeStatus.voters` as 'live nodes'").

## Decision

Introduce an explicit **liveness signal derived from the leader's own
heartbeat acks**, separate from committed membership, and thread it through the
stack as a distinct accessor. Land the *signal* now; keep placement behavior
unchanged (still targets committed voters) so this increment is safe and
observable before any crash-driven action is wired on top of it.

### The detector (`craft-core`)

The leader already learns liveness for free: every follower acks its periodic
`AppendEntries`. `RaftNode` now records, per peer, the `logical_clock` tick of
its **last successful ack** (`last_ack_clock`), and exposes:

- `reachable(window)` — itself plus every voter that acked within the last
  `window` logical ticks. A voter silent for longer is treated as
  crashed/partitioned **even though it is still a committed voter**.
- `reachable_now()` — `reachable` with a default window of `2 ×
  election_timeout_max`. A healthy follower acks every heartbeat interval (far
  shorter than an election timeout), so silence this long is strong evidence the
  node is down rather than merely slow, while staying well clear of the
  heartbeat cadence to avoid flapping.

Properties:

- **Leader-only.** Only the leader solicits acks, so only the leader has
  first-hand reachability. A follower has no ack data and conservatively reports
  the full voter set — it defers crash detection to the leader, which is where
  reconcile runs anyway ([ADR 018](018-supervision.md)).
- **Earned per term.** `last_ack_clock` is cleared on `become_leader`, so a
  stale observation from a prior leadership never counts toward the current
  term's liveness.
- **Never affects safety.** Reachability is advisory: it drives *placement*
  decisions, never commit/quorum. Consensus continues to use committed
  membership exclusively.

### Threading it up (`craft-actor`, `craft`)

- `NodeStatus` gains a `reachable: Vec<NodeId>` field (from `reachable_now()`),
  alongside the existing `voters`.
- The `ClusterState` trait gains `reachable_nodes()`, **defaulting to
  `live_nodes()`** so existing/mock implementations keep their prior behavior
  (every committed voter assumed alive).
- `ClusterFacts` (the runtime's `ClusterState`) is refreshed from
  `NodeStatus.reachable` by the same background facts loop that tracks
  leadership and voters.

`live_nodes()` now documents its true meaning — the **placement target**
(committed voters); instances are only ever spawned onto committed voters.
`reachable_nodes()` is the **liveness** view.

## What landed now vs. deferred

| Piece | Status |
|-------|--------|
| Per-peer ack tracking + `reachable`/`reachable_now` (`craft-core`) | **Landed** |
| `NodeStatus.reachable`, `ClusterState::reachable_nodes`, `ClusterFacts` wiring | **Landed** |
| Placement still targets committed voters (no behavior change) | **Landed (intentional)** |
| Supervisor consuming `reachable_nodes()` to migrate/respawn a crashed host's workers | **Deferred** |
| Tuning the detection window / hysteresis under real network jitter | **Deferred** |

The deferred crash-driven reconcile is now *unblocked*: it can read
`reachable_nodes()` and diff it against `live_nodes()` to find crashed voters,
then move their managed workers — without waiting for a `ConfChange`. That step
is intentionally separate so the detector can be observed (via `NodeStatus`)
and its window tuned before it is allowed to trigger migrations.

## Consequences

- **Good:** membership and liveness are no longer conflated; the crash signal
  exists and is observable; the detector is free (rides existing heartbeats) and
  cannot affect consensus safety.
- **Cost:** a small per-peer map on the leader and one extra field on
  `NodeStatus`.
- **Risk:** a too-tight window could flag a slow-but-alive follower
  (false positive). Mitigated by the conservative `2 × election_timeout_max`
  default and by *not* acting on the signal yet — a false positive is currently
  cosmetic (a status field), not a spurious migration.

## Alternatives considered

- **Phi-accrual failure detector.** More precise under variable latency, but
  heavier and unnecessary while the signal is advisory; the ack-recency window
  is adequate and trivially testable. Can be swapped in behind `reachable`
  later.
- **Separate gossip/SWIM liveness plane.** Independent of Raft, but adds a whole
  subsystem and its own failure modes; the leader's heartbeats already carry the
  information for free ([ADR 007](007-discovery.md) keeps gossip scoped to
  address discovery, not liveness).
- **Keep conflating membership and liveness, auto-propose removal on silence.**
  Rejected: turning a transient partition into a committed membership change is
  destructive and races the partitioned node's own view.

[`ClusterState`]: ../../crates/craft-actor/src/supervisor.rs
