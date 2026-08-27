# ADR 031: Write sharding / multi-Raft

**Status:** Accepted (runtime wiring landed)
**Date:** 2026-07-06

## Context

v1 runs a **single Raft group**: every write funnels through one leader and one
log. Adding nodes improves fault tolerance and actor compute but **not** linear
write throughput — the write-throughput ceiling recorded as **risk R1** in
[ADR 027](027-future-work-and-risks.md) and the "primary future write-scaling
path" (deferred item #5).

The scaling answer, as in Spanner/CockroachDB/TiKV, is to partition the keyspace
across **multiple independent Raft groups** ("multi-Raft" / write sharding).
Each group replicates its own shard of state and elects its own leader, so write
capacity scales roughly linearly with the number of groups (bounded by hardware
and cross-shard coordination).

## Decision

Adopt a **fixed-shard, rendezvous-placed, multi-group** design. Land the pure
routing foundation now (this ADR + `craft-core::shard`); stage the runtime,
storage, and client changes behind it.

### Routing model (landed: `craft-core::shard`)

| Step | Mechanism | Property |
|------|-----------|----------|
| key → shard | `ShardRouter::shard_for` (stable FNV-1a mod `shard_count`) | Every node agrees; stable across restarts |
| shard → group | `place_shard` — rendezvous (highest-random-weight) hashing | Adding/removing a group moves only ~`1/N` shards, never churns others |
| assignment | `shard_assignment` | Deterministic full map for a group set |

- **Shard count is fixed** for a cluster's life (repartitioning/split-merge is
  out of scope — pick a count comfortably larger than the expected group count,
  e.g. 256).
- **Groups are elastic**; rendezvous hashing keeps shard movement minimal when
  the group set changes.

### Runtime model (landed: `craft-actor::sharded`)

```
                       ┌── RaftGroup 0 (shards {0,4,8,…})  → driver + log + SM slice
 client write (key) ─▶ ShardRouter ─▶ group ─▶ RaftGroup 1 (shards {1,5,9,…})
                       └── RaftGroup 2 (shards {2,6,10,…}) …
```

- Each node may host a replica of **several** groups (one `RaftDriver` per group).
  `craft-actor::spawn_multi_raft_node` wires *N* drivers on one physical node;
  `ShardedNodeService` demuxes client and peer traffic by group.
- `GroupTransport` wraps peer RPCs in `GroupPeerEnvelope` so a single UDP
  socket carries multiple Raft groups.
- `GroupMemoryStorage` provides per-group in-memory isolation for tests;
  production redb namespacing remains deferred.
- **Client routing:** `ClientRequest::ProposeKeyed` / `QueryKeyed` carry a shard
  key; [`ShardedNodeService`] resolves `key → shard → group` and forwards to
  the correct group's [`NodeService`]. Ungrouped `Propose`/`Query` still hit
  group 0 (single-group default unchanged).
- **Rebalancing control plane** (leader-owned placement on join/leave) remains
  deferred.

### Cross-shard writes

- v1-multi-Raft: **single-shard writes only** (each `propose` touches one
  group). Multi-key/cross-shard atomicity (2PC over Raft groups) is a **separate
  future ADR** — deliberately not promised here.

### Membership & placement

- Group membership reuses joint consensus ([ADR 016](016-membership-early.md))
  **per group**. A cluster-level control plane (an extension of the leader-owned
  supervisor, [ADR 018](018-supervisor-leader.md)) decides which physical nodes
  host which groups' replicas and rebalances on join/leave — reusing the
  rendezvous placement so rebalancing is minimal.

## What landed now

- `craft-core::shard`: `ShardRouter`, `ShardId`, `RaftGroupId`, `place_shard`,
  `shard_assignment` — pure, deterministic, unit-tested (including the
  minimal-churn property of rendezvous hashing). This is the shared vocabulary
  every later layer (client, runtime, control plane) routes against.
- `craft-actor::sharded`: `ShardedNodeService`, `spawn_multi_raft_node`,
  `GroupTransport`, keyed client wire types (`ProposeKeyed`/`QueryKeyed`).
- `craft-client`: `RemoteClient::propose_keyed` / `query_keyed` and matching
  `TypedClient` helpers.

## What is deferred (and why it is safe to defer)

- `CraftClusterBuilder` integration (multi-group QUIC/local start), per-group
  redb namespacing, cross-node multi-group replication layout, and a
  rebalancing control plane. Single-group remains the default via
  `CraftClusterBuilder`; opt into multi-Raft with `spawn_multi_raft_node` or
  a custom `ShardedNodeService` until the builder path lands.

## Consequences

**Positive**

- Names the write-scaling architecture concretely; no silent gap for R1.
- The routing math is proven and testable in isolation, decoupled from the hard
  runtime work.
- Rendezvous placement gives cheap elasticity (minimal shard movement).

**Negative**

- Full multi-Raft is still future work; write throughput is unchanged until the
  runtime wiring lands.
- Fixed shard count trades repartitioning flexibility for routing simplicity.
- Cross-shard atomicity remains unsolved (explicitly out of scope).

## Related

- [ADR 027](027-future-work-and-risks.md) — R1 write ceiling, deferral #5
- [ADR 008](008-scale-targets.md) — scale targets
- [ADR 003](003-client-routing.md) — client routing (gains a shard step)
- [ADR 016](016-membership-early.md) — per-group membership
- [ADR 018](018-supervisor-leader.md) — leader-owned control plane
