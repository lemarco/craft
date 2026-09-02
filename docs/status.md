# Project status

**Current-state index** for the trembita workspace. Feature rationale lives in [decisions/](decisions/); test inventory in [testing-coverage.md](testing-coverage.md).

| | |
|---|---|
| **Version** | `0.1.0` |
| **MSRV** | 1.90 |
| **Distribution** | Published on [crates.io](https://crates.io/crates/trembita) — full test pyramid, E2E/chaos |

---

## At a glance

**Product scenarios** (no mandatory Redis or Kubernetes):

| Scenario | Guide | Status |
|----------|-------|--------|
| Background jobs | [scenarios/background-jobs.md](scenarios/background-jobs.md) | ✅ queue, DLQ, cron, external backlog |
| Event topics | [scenarios/event-topics.md](scenarios/event-topics.md) | ✅ pub/sub, named subscriptions |
| Stateful workers | [scenarios/stateful-workers.md](scenarios/stateful-workers.md) | ✅ `RedbActorStateStore`, migration |
| Real-time / session | [scenarios/realtime-sessions.md](scenarios/realtime-sessions.md) | ✅ `ActorSession`, gateway WS |
| Workflows | [scenarios/workflows.md](scenarios/workflows.md) | ✅ Meta-Raft saga journal |
| Product API | [getting-started.md](getting-started.md) | ✅ `TrembitaApp` + gateway |

**Platform core:** pure Raft FSM, HTTP/3/mTLS, redb persistence, cross-node actors, multi-Raft sharding, cross-shard saga/2PC, self-update coordinator, E2E/chaos.

**Not goals:** linearizable actor `ask`, global cross-shard serializable isolation. **Optional work:** [backlog.md](backlog.md#open-work).

Details below ↓

---

## Shipped capabilities (detail)

### Consensus & storage

- Pure Raft FSM (`trembita-core`) — election, replication, joint-consensus membership, snapshots, compaction
- ReadIndex linearizable reads, leader lease-read fast path, follower linearizable reads
- Durable log via redb (`trembita-storage`); per-group `group-<id>.redb` with `TrembitaClusterBuilder::data_dir`

### Network & security

- HTTP/3 / QUIC + mTLS (`trembita-net`); dedicated peer connection per traffic class
- Opt-in per-class token-bucket rate limiting (`TrafficPolicy` / `RateLimiter`)
- mTLS hot reload (`PemSecurity`, `CertReloadHandle`, step-ca example)
- Dev-only JSON wire (`trembita/json-wire` feature)

### Cluster operations

- Dynamic join via seed set + DNS discovery (`trembita::discovery`, `join_seeds`)
- Cluster leave RPC (`TrembitaCluster::leave`, `TREMBITA_ALLOW_LEAVE`)
- Health/admin HTTP (`:8080`) with optional TLS
- Reachability signal distinct from membership; crash-driven supervisor reconcile against `reachable_nodes()`
- Phi-accrual / tunable reachability (`ReachabilityConfig`)
- `trembita-ops` snapshot backup/restore; rolling wire N/N−1 compatibility
- **Self-update coordinator** — `trembita_core::upgrade` reference SM, leader reconcile + local executor (`trembita::upgrade`), HTTP `GET/POST /cluster/upgrade*` ([upgrade-coordinator](decisions/upgrade-coordinator.md), [examples/self-update](../examples/self-update/))

### Actors

- Cross-node actors, auto-spawn on join, one worker per VPS (production)
- Consistent-hash ring keyed routing, sticky `ActorSession`, per-group drain override (`TREMBITA_DRAIN_TIMEOUT`)
- Optional `DirectoryPolicy::ReadYourWrites` and `ask_linearizable` (directory visibility, not Raft-linearizable actor state)
- **Durable mailbox spool** — redb outbox/inbox for cross-node `/actor/deliver` (`.durable_mailbox(true)` + `data_dir`)
- **Durable actor workflow store** — `RedbActorStateStore` + voter replication; auto with `.data_dir()` ([actor-state-store](decisions/actor-state-store.md))
- **Actor store TTL + GC** — per-key TTL on `set`/`set_with_ttl`; periodic leader GC ticker replicates expired-key deletes to voters
- **Product API** — [`TrembitaApp`](../crates/trembita/src/app.rs), [getting-started.md](getting-started.md)
- Redis-backed `ActorStateStore` (`trembita-store-redis`); actor migration RPC

**Job queue** ([job-queue](decisions/job-queue.md)): `RedbJobQueue`, batch enqueue/ack, prefetch, DLQ, cron, `ClusterJobQueue`, `#[trembita::consumer]`, autoscale; **`ExternalBacklog`** ([external-backlog](decisions/external-backlog.md), [`trembita-backlog-postgres`](../crates/trembita-backlog-postgres/)); **`ScheduleSource`** ([schedule-source](decisions/schedule-source.md)).

**Event topics** ([event-topics](decisions/event-topics.md)): durable pub/sub, named subscriptions, voter replication; [`TopicOpts`](../crates/trembita/src/topic_opts.rs), [`.topics()`](../crates/trembita/src/app.rs).

**Gateway & HTTP** ([`trembita-http`](../crates/trembita-http/README.md), [gateway-identity](decisions/gateway-identity.md)): separate listener, opt-in `/jobs/*`, `/actors/*`, `/workflows/*`, bearer auth, custom Axum/WebSocket, `HostRouter`.

**Workload governor** ([workload-governor](decisions/workload-governor.md)): per-node compute tokens + consumer tuning from gateway load and queue depth.

**Consumer DX** — `#[consumer_json]`, `ConsumerOpts::on_app`, `IdempotencyOpts::retain_for`, graceful drain, workflow step helpers.

**E2E & showcases** — `./e2e/queue.sh`, gateway/idempotency scripts; `examples/background-jobs`, `stateful-workers`, `realtime`, `workflows`.

### Multi-Raft write scaling

| Layer | API / component |
|-------|-----------------|
| Routing | `ShardRouter`, `StableShardRouter` (default), rendezvous `place_shard` / `place_group` |
| Runtime | `ShardedNodeService`, `spawn_multi_raft_node`, Meta-Raft coordinator, keyed `ProposeKeyed` / `QueryKeyed` |
| Modulus routing | Per-group learners, `expand_shard_count`, `propose_keyed_batch`, `/introspect/raft-groups` |
| Stable shards & catalog | Dynamic catalog (`add_raft_groups`), stable shard activation (`activate_shards`, `switch_to_stable_shards`), `catalog_version` |
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

Global serializable isolation across shards is **not** a goal — see [multi-raft § cross-shard transactions](decisions/multi-raft.md#cross-shard-transactions).

---

## Scope boundaries

Capabilities we deliberately do **not** provide — not missing work:

| Item | Notes | ADR |
|------|-------|-----|
| **Linearizable actor `ask`** | Use Raft `query` for SM data; `ask` stays fast/local | [client-and-routing § read consistency](decisions/client-and-routing.md#read-consistency) |
| **PostgreSQL `ActorStateStore`** | redb is the product default; Redis optional | [actor-state-store](decisions/actor-state-store.md) |
| **Redis Cluster auto-discovery** | Single Redis URL per node | [actor-state-redis](decisions/actor-state-redis.md) |
| **Jepsen / Antithesis validation** | Optional external validation; in-tree sim + E2E cover correctness today | [testing-strategy](decisions/testing-strategy.md) |
| **`loom` concurrency tests** | On-demand only | [testing-strategy](decisions/testing-strategy.md) |

---

## Release & ops (process, not missing code)

- **crates.io / docs.rs publish** — v0.1.0 (see [CHANGELOG.md](../CHANGELOG.md))
- **Public API docs** — `missing_docs = "deny"` on published crates; `publish = false` crates exempt via crate lint override. Audit: `./scripts/docs-missing-audit.sh`
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

## Where to read next

| Doc | Purpose |
|-----|---------|
| [examples/README.md](../examples/README.md) | Product showcases (local + QUIC cluster) |
| [scenarios/README.md](scenarios/README.md) | Product scenario index |
| [backlog.md](backlog.md) | Open work + shipped epic archive |
| [../CONTRIBUTING.md](../CONTRIBUTING.md) | Contributor guide (humans) |
| [architecture.md](architecture.md) | Crate graph, data flows |
| [decisions/](decisions/) | Design decision records |
| [testing-coverage.md](testing-coverage.md) | Test inventory |
| [CHANGELOG.md](../CHANGELOG.md) | Version history |
| [releasing.md](releasing.md) | Publish workflow |
