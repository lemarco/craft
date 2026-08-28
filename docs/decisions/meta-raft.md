# Meta-Raft coordinator group

**Status:** Accepted (landed)  
**Date:** 2026-08-28

## Context

In multi-Raft mode, group 0 previously served **dual roles**: cluster coordinator (join/leave, dynamic catalog, saga journal metadata) and a regular user Raft group with its own state machine. On large clusters, coordinator traffic (membership changes, catalog expansions, saga upserts) competes with keyed user writes on the same log and leader.

[Tier 2](tier2-multi-raft-architecture.md) deferred a separate meta-Raft group while group 0 sufficed for small deployments.

## Decision

Introduce a dedicated **Meta-Raft** coordinator group when `raft_groups > 1`:

| Concern | Before | After (multi-Raft) |
|---------|--------|---------------------|
| Cluster registry (join/leave) | Group 0 membership | Meta-Raft membership |
| Dynamic catalog | Group 0 log (`EntryPayload::Catalog`) | Meta-Raft log |
| Saga journal | Group 0 log (`EntryPayload::SagaJournal`) | Meta-Raft log |
| User state machine | Group 0 | Group 0 (unchanged) |

### Reserved group id

- `META_RAFT_GROUP_ID = u32::MAX` (`craft_core::shard`)
- Storage: `group-meta.redb` under `data_dir`
- **Not** in the user catalog or keyed shard routing
- Hosted on **every** live node; cluster RPCs (`/cluster/join`, `/cluster/leave`, `/cluster/catalog/add`) route to Meta-Raft

### Single-group clusters

When `raft_groups == 1`, behavior is unchanged: group 0 remains coordinator + user SM (no Meta-Raft spawn).

### User catalog

Catalog validation requires contiguous user ids `0..=max` only. Group 0 is a normal data group in multi-Raft mode; per-group membership sync applies to it like any other shard group ([per-group-raft-membership](per-group-raft-membership.md)).

## Consequences

**Positive:** Coordinator metadata is isolated from user write throughput on group 0; join/leave and catalog expansion no longer contend with keyed application traffic.

**Negative:** Multi-Raft nodes host one extra Raft group; operators must back up `group-meta.redb` alongside user group files.

## Related

- [tier2-multi-raft-architecture](tier2-multi-raft-architecture.md)
- [per-group-raft-membership](per-group-raft-membership.md)
- [cross-shard-transactions](cross-shard-transactions.md)
- [status.md](../status.md)
