# Project status

**Current-state index** for the crafty workspace. Feature rationale lives in [decisions/](decisions/); test inventory in [testing-coverage.md](testing-coverage.md).

| | |
|---|---|
| **Version** | `0.2.0` (pre-1.0 — API may change on minor bumps) |
| **MSRV** | 1.90 |
| **Maturity** | Advanced prototype — full test pyramid, E2E/chaos, published on [crates.io](https://crates.io/crates/crafty) |

---

## Shipped capabilities

### Consensus & storage

- Pure Raft FSM (`crafty-core`) — election, replication, joint-consensus membership, snapshots, compaction
- ReadIndex linearizable reads, leader lease-read fast path, follower linearizable reads
- Durable log via redb (`crafty-storage`); per-group `group-<id>.redb` with `CraftyClusterBuilder::data_dir`

### Network & security

- HTTP/3 / QUIC + mTLS (`crafty-net`); dedicated peer connection per traffic class
- Opt-in per-class token-bucket rate limiting (`TrafficPolicy` / `RateLimiter`)
- mTLS hot reload (`PemSecurity`, `CertReloadHandle`, step-ca example)
- Dev-only JSON wire (`crafty/json-wire` feature)

### Cluster operations

- Dynamic join via seed set + DNS discovery (`crafty::discovery`, `join_seeds`)
- Cluster leave RPC (`CraftyCluster::leave`, `CRAFTY_ALLOW_LEAVE`)
- Health/admin HTTP (`:8080`) with optional TLS
- Reachability signal distinct from membership; crash-driven supervisor reconcile against `reachable_nodes()`
- Phi-accrual / tunable reachability (`ReachabilityConfig`)
- `crafty-ops` snapshot backup/restore; rolling wire N/N−1 compatibility

### Actors

- Cross-node actors, auto-spawn on join, one worker per VPS (production)
- Consistent-hash ring keyed routing, sticky `ActorSession`, per-group drain override (`CRAFTY_DRAIN_TIMEOUT`)
- Optional `DirectoryPolicy::ReadYourWrites` and `ask_linearizable` (directory visibility, not Raft-linearizable actor state)
- **Durable mailbox spool** — redb outbox/inbox for cross-node `/actor/deliver` (`.durable_mailbox(true)` + `data_dir`)
- **Durable actor workflow store** — `RedbActorStateStore` + voter replication; auto with `.data_dir()` ([actor-state-store](decisions/actor-state-store.md))
- **Product API** — [`CraftyApp`](../crates/crafty/src/app.rs), [getting-started.md](getting-started.md)
- Redis-backed `ActorStateStore` (`crafty-store-redis`); actor migration RPC
- **`JobQueue`** — `InMemoryJobQueue`, `RedbJobQueue`, sharded streams, priority/delayed enqueue, leader `QueueService` with parallel sync voter replication + replicate auth, `ClusterJobQueue`, worker + membership autoscale, Meta-Raft autoscale policy ([job-queue](decisions/job-queue.md))
- **Job queue E2E** — `./e2e/queue.sh` (QUIC/mTLS, enqueue → follower worker → leader failover); `crafty-node` env `CRAFTY_DATA_DIR` + `CRAFTY_JOB_QUEUE`
- **Examples** — `job_queue_worker` (follower `ClusterJobQueue` + failover), `job_queue_cluster` (sharding, dedup, autoscale)

### Multi-Raft write scaling

| Layer | API / component |
|-------|-----------------|
| Routing | `ShardRouter`, `StableShardRouter` (default), rendezvous `place_shard` / `place_group` |
| Runtime | `ShardedNodeService`, `spawn_multi_raft_node`, Meta-Raft coordinator, keyed `ProposeKeyed` / `QueryKeyed` |
| Tier 1 | Per-group learners, `expand_shard_count`, `propose_keyed_batch`, `/introspect/raft-groups` |
| Tier 2 | Dynamic catalog (`add_raft_groups`), stable shard activation (`activate_shards`, `switch_to_stable_shards`), `catalog_version` |
| Meta-Raft | Dedicated coordinator group for join/leave, catalog, saga journal (multi-Raft only) |
| Rebalance | `RaftGroupReconciler`, cross-node group migration RPC (`/cluster/group/migrate`) |
| Membership | Per-group voter sets (`group_replication_factor`, `sync_group_membership`) |

### Cross-shard writes

| API | Guarantee |
|-----|-----------|
| `propose_keyed_batch` | Sequential; partial failure surfaced (`BatchError::Partial`) |
| `run_saga` / `resume_saga` | All steps commit or compensators run; journal in Redis and/or Meta-Raft log (`CompositeSagaJournal`) |
| `propose_cross_shard_2pc` (opt-in) | Atomic commit if all groups ack prepare (≤3 groups; in-memory prepare by default) |
| `durable_cross_shard_2pc(true)` | Same as 2PC; prepare/abort persisted in each group's Raft log (survives leader restart) |
| `resume_cross_shard_2pc` | Client coordinator: journal resume + commit-first probe for durable server prepares |

Global serializable isolation across shards is **not** a goal — see [cross-shard-transactions](decisions/cross-shard-transactions.md).

---

## Intentionally not shipped

| Item | Notes | ADR |
|------|-------|-----|
| **Linearizable actor `ask`** | Use Raft `query` for SM data; `ask` stays fast/local | [read-consistency](decisions/read-consistency.md) |
| **PostgreSQL `ActorStateStore`** | redb is the product default; Redis optional | [actor-state-store](decisions/actor-state-store.md) |
| **Redis Cluster auto-discovery** | Single Redis URL per node | [actor-state-redis](decisions/actor-state-redis.md) |
| **Jepsen / Antithesis validation** | Aspirational before 1.0 stability claim | [testing-strategy](decisions/testing-strategy.md) |
| **`loom` concurrency tests** | On-demand only | [testing-strategy](decisions/testing-strategy.md) |

---

## Release & ops (process, not missing code)

- **crates.io / docs.rs publish** — published v0.2.0 — see [CHANGELOG.md](../CHANGELOG.md)
- **Public API docs** — `missing_docs = "warn"` on published crates; CI allows pre-1.0 (`-A missing_docs`). Audit: `./scripts/docs-missing-audit.sh`
- **Real-world soak** — scenario harness in `benchmarks/` (`soak`, `soak_queue`, `soak_multi_raft`, `soak_actor_store`, `soak_saga`, `soak_session`); scheduled CI `bench` job (60–120s budgets); long-running production soak is operator responsibility
- **Heavy integration tests** — Redis/docker tests gated `#[ignore]` in fast CI; scheduled heavy lane

---

## Known structural limits

Documented in [future-work-and-risks](decisions/future-work-and-risks.md):

| Risk | Summary |
|------|---------|
| **R1** | Single Raft group still has a per-group write ceiling; mitigation is **multi-Raft + add groups**, not bigger VPS count alone |
| **R2** | Shared QUIC listener — mitigated by peer connection isolation + optional rate limiting |
| **R3** | Actor directory is eventually consistent — mitigated by TTL, RYW policy, anti-entropy |
| **R4** | Actor memory without Redis is lost on crash — use write-through to `ActorStateStore` |
| **R5** | Deep tracing has a performance cost — metrics on by default, tracing opt-in |
| **R6** | mTLS ops burden — mitigated by hot reload + step-ca example |

---

## Product scenarios (guides + backlog)

Four application patterns on one runtime — **no mandatory Redis or Kubernetes**:

| Scenario | Guide | Runtime |
|----------|-------|---------|
| Background jobs | [scenarios/background-jobs.md](scenarios/background-jobs.md) | ✅ `RedbJobQueue` |
| Stateful workers | [scenarios/stateful-workers.md](scenarios/stateful-workers.md) | ✅ `RedbActorStateStore` |
| Real-time / session | [scenarios/realtime-sessions.md](scenarios/realtime-sessions.md) | ✅ `ActorSession` |
| Workflows | [scenarios/workflows.md](scenarios/workflows.md) | ✅ Meta-Raft saga journal |
| Product API | [getting-started.md](getting-started.md) | ✅ `CraftyApp` |

Decision: [decisions/product-scenarios.md](decisions/product-scenarios.md) · Backlog: [backlog.md](backlog.md)

---

## Where to read next

| Doc | Purpose |
|-----|---------|
| [scenarios/README.md](scenarios/README.md) | Product scenario index |
| [backlog.md](backlog.md) | Planned work (0.2.x → 1.0) |
| [architecture.md](architecture.md) | Crate graph, data flows |
| [decisions/](decisions/) | Design decision records |
| [testing-coverage.md](testing-coverage.md) | Test inventory |
| [CHANGELOG.md](../CHANGELOG.md) | Version history |
| [releasing.md](releasing.md) | Publish workflow |
