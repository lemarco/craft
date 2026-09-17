# Write scaling — multi-Raft and small commands

**Problem (R1):** every `propose` on one Raft group serializes through that group's leader. Adding VPS capacity runs more workers and replicas; it does not remove the **per-group** write ceiling.

**Answer:** partition the keyspace across **multiple Raft groups**, keep log entries small, and put bulk/async work in `JobQueue` or cap store — not in the consensus log.

## When you hit the ceiling

Symptoms:

- Leader CPU or append latency grows while business throughput is flat
- Single hot key or tenant dominates one group
- You are tempted to put large payloads or job rows in `propose`

## Mitigation checklist

1. **Enable multi-Raft** — [`spawn_multi_raft_node`](../../crates/trembita-runtime/src/sharded.rs), `ShardedNodeService`, Meta-Raft coordinator ([multi-raft ADR](../decisions/multi-raft.md)).
2. **Add groups** — `TrembitaCluster::add_raft_groups(n)` expands the catalog; rebalance migrates group hosting across nodes.
3. **Route by key** — `propose_keyed`, `query_keyed`, `propose_keyed_batch` (sequential batch, partial errors surfaced).
4. **Stable shards** — `activate_shards`, `switch_to_stable_shards` when moving from modulus to catalog-backed placement.
5. **Keep commands small** — SM commands carry decisions; bytes live in queue, cap store, or your DB ([state-placement](state-placement.md)).

## Ops introspection

- `GET /introspect/raft-groups` — groups hosted on this node
- Rebalance tracing: `TREMBITA_LOG_REBALANCE=1` ([cargo-shell-safety](../../.cursor/rules/cargo-shell-safety.mdc))

After rebalance, cross-node capability delivery retries directory visibility automatically when using product [`CapManifest`](../../crates/trembita/src/capability/manifest.rs) ([structural-limits § R3](structural-limits.md#r3--actor-directory-is-eventually-consistent)).

## Cross-shard writes

When one user action touches multiple shards:

- Prefer **`run_saga`** with compensators ([workflows](workflows.md))
- Use **`propose_cross_shard_2pc`** only when you need atomic prepare/commit across a **small** set of groups (≤3); enable **`durable_cross_shard_2pc(true)`** if the coordinator must survive restart

Neither replaces **global serializable isolation** — see [structural-limits](structural-limits.md#cross-shard-writes--no-global-serializable-isolation).

## Related

- [cluster-elasticity § scale targets](../decisions/cluster-elasticity.md#scale-targets)
- [job-queue](../decisions/job-queue.md) · [external-backlog](../decisions/external-backlog.md)
- [future-work-and-risks § R1](../decisions/future-work-and-risks.md#r1--write-throughput-ceiling-per-raft-group)
