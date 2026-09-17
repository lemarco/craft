# Structural limits & consistency — product cheat sheet

Trembita ships mitigations for known platform limits (**R1–R6** in [future-work-and-risks](../decisions/future-work-and-risks.md)). This page maps **what you feel in app code** to **what to use instead**.

## R1 — Per-group write ceiling

One Raft group = one leader log. More VPS nodes improve fault tolerance and compute; they do **not** multiply writes through a **single** group.

| Do | API / pattern |
|----|----------------|
| Partition hot keys | Multi-Raft — [`ShardRouter`](../decisions/multi-raft.md), `add_raft_groups`, keyed `propose` / `query` ([`TrembitaApp`](../../crates/trembita/src/app/runtime.rs) / [`OpCtx::propose_keyed`](../../crates/trembita/src/capability/consensus.rs)) |
| Keep commands small | Domain SM commands; bulk bytes in `JobQueue`, cap store, or app Postgres |
| Respect wire limit | [`MAX_RAFT_COMMAND_BYTES`](../../crates/trembita-proto/src/client.rs) (512 KiB) — client + leader reject oversize (`CommandTooLarge`) |
| Scale backlog throughput | `JobQueue` sharding, `ExternalBacklog`, not Raft log rows |

Guide: [Write scaling](write-scaling.md).

## R3 — Actor directory is eventually consistent

After join, scale, or **Raft group rebalance**, the merged directory can lag briefly → `NoTarget` on cross-node deliver.

| Mitigation | When |
|------------|------|
| [`DirectoryPolicy::ReadYourWrites`](../../crates/trembita-runtime/src/directory_policy.rs) | Brief retry before `NoTarget` (default for [`CapManifest`](../../crates/trembita/src/capability/manifest.rs) apps) |
| [`TrembitaAppBuilder::directory_retry`](../../crates/trembita/src/app/builder.rs) | Tune attempts/backoff after heavy rebalance |
| [`ActorSession`](../../crates/trembita-runtime/src/session.rs) TTL + re-open | Sticky workflows after migration |
| Anti-entropy + liveness | Automatic; ops: drain before leave |

Advanced: [`TrembitaCluster::publish_directory_visible`](../../crates/trembita-assembly/src/cluster_handle/cluster.rs) after local spawn.

## R4 — Handler RAM without cap store

In-memory group state and session fields are **lost on process crash** unless written elsewhere.

| Need | Use |
|------|-----|
| Idempotency / step markers | `OpCtx::require_store()` → [`trembita::capstore`](../../crates/trembita/src/capstore.rs) (requires `TREMBITA_DATA_DIR`) |
| Authoritative balances / audit facts | `propose` / `query` on your Raft SM (Postgres via app layer + outbox, not cap store alone) |
| Fire-and-forget side effects | `Route::Queued` + store marker before ack |

`trembita doctor` errors when a capability file calls `store_get` / `store_set` without `require_store`.

## Read paths — not interchangeable

| Path | Consistency | Use for |
|------|-------------|---------|
| **`client.query` / SM `query`** | **Linearizable** (ReadIndex / lease / follower reads) | Authoritative replicated state |
| **`ask` / capability inline reply** | Fast, local to the serving instance | Handler logic, cached group state |
| **`ask_linearizable`** | Directory **visibility** only (RYW retries) — **not** SM-linearizable |

**Non-goal:** linearizable actor `ask`. Use Raft `query` for truth.

Details: [client-and-routing § read consistency](../decisions/client-and-routing.md#read-consistency).

## Cross-shard writes — no global serializable isolation

Multi-Raft shards commit independently. Cross-shard patterns:

| API | Guarantee | Non-goal |
|-----|-----------|----------|
| `propose_keyed_batch` | Sequential per call; partial failure surfaced | Atomic all-shards |
| `run_saga` / `resume_saga` | All steps commit **or** compensators run; durable journal | Global serializable snapshot |
| `propose_cross_shard_2pc` (+ optional `durable_cross_shard_2pc`) | Prepare/commit across ≤3 groups when all prepare | Spanner-style timestamps |

**Non-goal:** global serializable isolation across shards — design compensating sagas and idempotent steps instead.

Guides: [workflows](workflows.md) · [multi-raft § cross-shard](../decisions/multi-raft.md#cross-shard-transactions) · [state-placement](state-placement.md).

## Related

- [status.md § scope boundaries](../status.md#scope-boundaries)
- [future-work-and-risks](../decisions/future-work-and-risks.md)
- [capabilities](capabilities.md) · [stateful-workers](stateful-workers.md)
