# Per-group Raft membership (multi-Raft)

**Status:** Accepted (Phase 2 runtime wiring landed)
**Date:** 2026-08-27

## Context

[write-sharding-multi-raft](write-sharding-multi-raft.md) landed multi-Raft runtime wiring: N
independent `RaftDriver` instances per physical node, shard routing, keyed
client APIs, and rendezvous-based **group hosting** (`place_group`,
`RaftGroupReconciler`). That unblocks write throughput scaling (R1 in
[future-work-and-risks](future-work-and-risks.md)).

Two gaps remain:

1. **Shared voter set** — every group bootstraps with the same
   `CraftClusterBuilder::members` slice and `MultiRaftState` stores one
   cluster-wide `members: Vec<NodeId>`. Each group replicates as if it were the
   sole cluster Raft group over *all* nodes.
2. **Join only updates group 0** — `ShardedNodeService` routes `/cluster/join`
   to the first group handler. Groups 1..N never receive a `ConfChange` when
   a node joins elastically.

The target model in write-sharding-multi-raft § "Membership & placement" is **joint consensus per
group** ([membership-early](membership-early.md)): each Raft group owns its voter
set; a cluster-level control plane decides which physical nodes replicate which
groups and proposes membership changes accordingly.

This ADR locks the **semantics** and lands the **pure, deterministic planner**
in `craft-core::shard` so runtime wiring (Phase 2+) can be tested in isolation.

## Decision

### Two levels of membership

| Level | Meaning | Changes via | Consumers |
|-------|---------|-------------|-----------|
| **Cluster registry** | Known peers, gossip, actor supervisor placement target | `/cluster/join`, committed voter set of **group 0** (coordinator) until a dedicated meta-group exists | `ClusterSupervisor`, peer directory, join RPC |
| **Group membership** | Voters replicating *this* Raft group's log | Per-group `ConfChange` (joint consensus) | Each `RaftDriver`, group rebalance |

Group 0 remains the **cluster coordinator** for join/peers in the interim: a
node must join the cluster registry before the control plane adds it to any
group's voter set. Phase 2 will fan out group-level changes after cluster join.

### Desired voter set per group

For Raft group `G` and live (committed) cluster nodes `N`:

1. Rank every node in `N` by rendezvous weight `group_node_weight(G, node)`
   (same hash as [`place_group`](../../crates/craft-core/src/shard.rs)), tie-break
   lower `NodeId` first.
2. Take the top **`replication_factor`** nodes (clamped to `[1, |N|]`).
3. Return them **sorted by `NodeId`** — the canonical voter list format used
   everywhere else in craft.

Default **`replication_factor = 3`**: standard Raft quorum fault tolerance.
When `|N| ≤ replication_factor`, every group includes all live nodes (matches
today's shared voter set). When `|N| > replication_factor`, each group replicates
on a **distinct subset** of nodes, spreading replication load and shrinking
per-group quorum work.

`replication_factor = 1` recovers today's **single-host** rendezvous placement:
the voter set is exactly the node returned by `place_group`.

Learners are **out of scope** for this increment; catch-up uses the existing
follower path within the voter set.

### Pure planner API (landed: `craft-core::shard`)

| Function | Role |
|----------|------|
| [`effective_replication_factor`](../../crates/craft-core/src/shard.rs) | Clamp requested RF to live node count |
| [`group_voters`](../../crates/craft-core/src/shard.rs) | Desired voters for one group |
| [`group_membership_assignment`](../../crates/craft-core/src/shard.rs) | Full `group → voters` map |
| [`plan_group_membership_change`](../../crates/craft-core/src/shard.rs) | Diff committed vs desired (`add` / `remove`) |
| [`groups_joining_node_affects`](../../crates/craft-core/src/shard.rs) | Groups that should add a node after cluster join |
| [`groups_leaving_node_affects`](../../crates/craft-core/src/shard.rs) | Groups that should remove a departed node |

The control plane (Phase 2) calls these on membership events:

```
cluster join commits (group 0)
  → groups_joining_node_affects(new_node, …)
  → for each group: plan_group_membership_change → propose ConfChange

cluster leave / node removed from group 0
  → groups_leaving_node_affects(departed, …)
  → per-group remove ConfChange
```

Adopt/retire from [`plan_node_group_rebalance`](../../crates/craft-core/src/shard.rs)
(write-sharding-multi-raft) and group membership changes are **orthogonal but coordinated**:

- **Adopt** — this physical node should run a `RaftDriver` for the group
  (local process).
- **Add voter** — this node joins the group's Raft configuration (may be on a
  different physical host).

Phase 2 will pair adopt with add-voter on the joining host and retire with
remove-voter on the departing host. Cross-node group migration RPC is landed
([write-sharding-multi-raft](write-sharding-multi-raft.md)) to move a group's *state* before
removing its last local replica safely; per-group ConfChange fan-out remains
deferred.

## What landed now vs. deferred

| Piece | Status |
|-------|--------|
| ADR semantics + planner functions (`craft-core::shard`) | **Landed (Phase 1)** |
| Unit tests: RF=1 ≡ `place_group`, join/leave group subsets, diff planner | **Landed** |
| Per-group `ConfChange` on join/leave (runtime fan-out) | **Landed (Phase 2)** |
| `MultiRaftState.members` → per-group map | **Landed** — `replication_factor` + `group_voters` at spawn |
| Builder `replication_factor` option | **Landed** — `group_replication_factor` |
| Cross-node group migration RPC before retire | **Landed (write-sharding-multi-raft)** |
| Learners / asymmetric replication | **Deferred** |

## Consequences

**Positive**

- Names per-group membership concretely; no silent "shared voter set" gap.
- Planner is pure, deterministic, and testable — same pattern as write-sharding-multi-raft routing.
- `replication_factor` knob subsumes today's extremes: RF=|N| (legacy) and RF=1
  (single-host rendezvous).

**Negative**

- Cluster join + per-group ConfChange is a **two-phase** bootstrap; Phase 2 must
  handle partial failure (node in group 0 but not yet in group 7).
- RF &lt; |N| means a node can be in the cluster registry but not a voter for
  every group — clients must route keyed writes to the correct group's leader,
  not any cluster leader.

**Risk**

- Shrinking RF when nodes leave can require simultaneous changes across many
  groups. Phase 2 should serialize one ConfChange per group (Raft already
  refuses overlapping changes per group).

## Alternatives considered

- **Keep shared voter set, fan-out join to all groups only.** Fixes the join
  bug but not replication load / fault isolation; rejected as insufficient for
  R1 scaling goals.
- **Dedicated meta-Raft group for all membership.** Cleaner long term, but heavy
  for this increment; group 0 as coordinator is sufficient until meta-group ADR.
- **Phi-accrual or SWIM for group membership.** Rejected — membership stays
  log-authoritative ([membership-early](membership-early.md), [liveness-vs-membership](liveness-vs-membership.md)).

## Related

- [write-sharding-multi-raft](write-sharding-multi-raft.md) — multi-Raft runtime, deferred items
- [membership-early](membership-early.md) — joint consensus
- [supervisor-leader](supervisor-leader.md) — leader-owned control plane
- [liveness-vs-membership](liveness-vs-membership.md) — liveness vs committed voters
