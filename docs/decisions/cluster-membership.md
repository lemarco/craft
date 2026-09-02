# Cluster membership & discovery

**Status:** Accepted  
**Date:** 2026-07-05  
**Updated:** 2026-09-02 — learner join default, voter replacement, placement_nodes

## Context

Nodes must find peers, join and leave safely, and (in multi-Raft mode) maintain per-group voter sets. Dynamic `JOIN_ADDR` join with **full joint-consensus membership** is a v1 requirement — not a late add-on.

## Joint-consensus membership

**Implement full Raft membership changes (joint consensus) in early core phases — not deferred.**

| Feature | Required |
|---------|----------|
| Cluster config in replicated log | ✓ |
| Add learner / add voter (joint consensus) | ✓ |
| Remove node (joint consensus) | ✓ |
| Config commit → update peer set on all nodes | ✓ |
| Safe rejection of overlapping membership changes | ✓ |

Membership changes always go through the Raft log — HTTP routes are **entry points only**, not a bypass of consensus.

### Rejected alternatives

| Option | Why rejected |
|--------|--------------|
| Static bootstrap only in v1 | Breaks VPS chain-deploy story |
| Simplified join without joint consensus | Safety debt |
| Hybrid simplified now, fix later | User chose full membership upfront |

## Discovery

**Join-address bootstrap + Raft-persisted membership.**

| Mechanism | Purpose |
|-----------|---------|
| **`JOIN_ADDR` (optional env/CLI)** | First contact for a **new** VPS |
| **Seed mode** | No `JOIN_ADDR` → single-node; **`--allow-join`** to accept joins |
| **Raft cluster config** | Authoritative peer list — joint-consensus changes |
| **Static peer files** | Optional for air-gapped templates only |

### Seed-set discovery

- `TrembitaClusterBuilder::join_seeds` / `trembita::discovery::Seed` — ordered candidate list; tries each in turn.
- `trembita::discovery::resolve_dns_seeds` — ordinal DNS names (`node-0.cluster`, …) → seed set.
- Peer addresses converge via `/cluster/peers` anti-entropy gossip.

Full cloud-metadata auto-discovery beyond a seed set remains out of scope.

## Join RPC

**Dedicated join route** — `POST /raft/v1/cluster/join` (mTLS; target has `--allow-join`).

```rust
pub struct JoinRequest {
    pub protocol_version: u32,
    pub node_id: Option<NodeId>,
    pub advertise_addr: String,
    /// `Learner` (default) for elastic scale-out; `Voter` requires `allow_voter_join`.
    pub role: JoinRole,
}
```

| HTTP | Meaning |
|------|---------|
| `200` | Join accepted; membership change **committed** |
| `403` | `--allow-join` not set |
| `409` | Duplicate `NODE_ID`, version mismatch, or invalid cert |
| `503` | No leader / election in progress |

**Flow:** joiner → target (forward to leader if needed) → leader validates role → joint-consensus ConfChange (voters or learners) → all nodes update config → auto workers spawned ([cluster-elasticity](cluster-elasticity.md#voters-vs-learners-elastic-scale-out)).

## Leave RPC

**Dedicated leave route** — symmetric to join — `POST /raft/v1/cluster/leave` (mTLS; `--allow-leave`).

```rust
pub struct LeaveRequest {
    pub protocol_version: u32,
    pub node_id: NodeId,
}

pub enum LeaveResponse {
    Accepted { leader: NodeId, membership: Membership },
    Redirect { leader: Option<NodeId> },
    Rejected { reason: LeaveRejection },
}
```

| Rejection | When |
|-----------|------|
| `VersionSkew` | `protocol_version` mismatch |
| `LeavesDisabled` | `--allow-leave` not set |
| `NotMember` | `node_id` not in committed voters or learners |
| `LastMember` | Would empty the voter set |

**Facade:** `TrembitaClusterBuilder::allow_leave`, `TrembitaCluster::request_leave`, `TrembitaCluster::leave()`. Actor migration before leave is the caller's job ([cross-node-actors](cross-node-actors.md), [drain-timeout](drain-timeout.md)). `trembita-node` with `TREMBITA_GRACEFUL_LEAVE=1` calls `leave()` on `SIGINT`.

Multi-Raft routes cluster leave to **Meta-Raft** (or group 0 in single-group mode); per-group sync removes the node from shard groups.

## Version skew — hard reject

**Reject join/leave on version mismatch** — fail closed with HTTP **`409 Conflict`**.

| Field | Rule |
|-------|------|
| **`protocol_version`** | Must fall in `[MIN_COMPATIBLE_PROTOCOL_VERSION, PROTOCOL_VERSION]` (rolling N/N−1 wire upgrades) |
| **`app_version`** | Must **exactly equal** cluster's committed app version |

**Rolling upgrades:** adjacent protocol versions may coexist during deploy; app semver still requires exact match everywhere before adding a joiner. Dev: `JoinVersionPolicy::AllowAny` with `insecure-dev` only.

## Liveness vs membership

Two distinct signals:

| Signal | Meaning | Changes via |
|--------|---------|-------------|
| **Membership** | Committed voters + learners | `ConfChange` only |
| **Liveness** | Who acks heartbeats now | Crash/partition (no log entry) |

Leader records per-peer `last_ack_clock` from `AppendEntries` responses:

- `reachable(window)` — self plus **voters** that acked within window (queue replication fan-out).
- `reachable_members_now()` — reachable voters **and** reachable learners (worker placement).
- `reachable_now()` — default `2 × election_timeout_max` (voters only).
- Tunable via `ReachabilityConfig` / phi-accrual ([multi-raft](multi-raft.md#production-reliability)).

`ClusterSupervisor` plans against `placement_nodes()`; `live_nodes()` is the committed voter set. Queue replication uses `reachable_nodes()` (voters only). Advisory only — never affects commit/quorum.

## Voter replacement

When a committed voter stays unreachable beyond `6 ×` the reachability window and at least one learner is caught up, the leader proposes: remove the dead voter, promote the lowest-id eligible learner. Keeps voter count stable without operator action. Disable: `voter_replacement(false)`.

## Per-group membership (multi-Raft)

When `raft_groups > 1`, each group has its **own voter set**:

| Level | Meaning | Changes via |
|-------|---------|-------------|
| **Cluster registry** | Known peers, coordinator metadata | Meta-Raft join/leave (or group 0 in single-group) |
| **Group membership** | Voters for *this* group's log | Per-group `ConfChange` (joint consensus) |

Desired voters: rank nodes by rendezvous weight `group_node_weight(G, node)`, take top **`replication_factor`** (default 3). Planner API in `trembita-core::shard`: `group_voters`, `plan_group_membership_change`, `groups_joining_node_affects`, `groups_leaving_node_affects`. Runtime: `MultiRaftState::sync_group_membership` on membership delta.

Join is two-phase: cluster registry first, then per-group ConfChange; partial failure must be retried.

## Consequences

**Positive:** Safe dynamic join/leave; correct joint consensus; liveness-aware supervisor without consensus risk.

**Negative:** Largest early engineering cost; join is two-phase in multi-Raft; membership tests required before actors depend on join.

## Related

- [cluster-elasticity.md](cluster-elasticity.md) — auto-spawn on join
- [multi-raft.md](multi-raft.md) — write sharding, Meta-Raft
- [client-and-routing.md](client-and-routing.md) — forward pattern for operational RPCs
- [protocol.md](../protocol.md)
