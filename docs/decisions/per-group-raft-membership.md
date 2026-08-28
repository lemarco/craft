# Per-group Raft membership (multi-Raft)

**Status:** Accepted (landed)  
**Date:** 2026-08-27

## Context

Multi-Raft runtime ([write-sharding-multi-raft](write-sharding-multi-raft.md)) hosts N independent Raft groups per physical node. Each group needs its **own voter set** rather than sharing the cluster-wide bootstrap list. Cluster join must fan out membership changes to affected groups.

## Decision

### Two levels of membership

| Level | Meaning | Changes via |
|-------|---------|-------------|
| **Cluster registry** | Known peers, supervisor placement | Group 0 join/leave |
| **Group membership** | Voters for *this* group's log | Per-group `ConfChange` (joint consensus) |

Group 0 is the **cluster coordinator** until a meta-Raft group exists (deferred).

### Desired voter set per group

For group `G` and live nodes `N`:

1. Rank nodes by rendezvous weight `group_node_weight(G, node)`.
2. Take top **`replication_factor`** nodes (default 3, clamped to `|N|`).
3. Return sorted by `NodeId`.

`CraftClusterBuilder::group_replication_factor` configures RF. Per-group learners: `group_learner_factor` ([tier1-multi-raft-advances](tier1-multi-raft-advances.md)).

### Planner API (`craft-core::shard`)

| Function | Role |
|----------|------|
| `group_voters` | Desired voters for one group |
| `plan_group_membership_change` | Diff committed vs desired |
| `groups_joining_node_affects` | Groups to update after cluster join |
| `groups_leaving_node_affects` | Groups to update after leave |

### Runtime

- `MultiRaftState::sync_group_membership` on membership delta.
- Adopt/retire (group hosting) is orthogonal to add/remove voter — coordinated on join/leave and rebalance.
- Cross-node state move before retire: group migration RPC ([write-sharding-multi-raft](write-sharding-multi-raft.md)).

## Consequences

**Positive:** Replication load spreads across subsets when `RF < |N|`; planner is pure and testable.

**Negative:** Join is two-phase (group 0 then per-group ConfChange); partial failure must be retried; clients route keyed writes to the correct group leader.

## Related

- [write-sharding-multi-raft](write-sharding-multi-raft.md)
- [membership-early](membership-early.md)
- [supervisor-leader](supervisor-leader.md)
- [liveness-vs-membership](liveness-vs-membership.md)
- [tier2-multi-raft-architecture](tier2-multi-raft-architecture.md)
