# Project status

**Current-state index** for the trembita workspace. Feature rationale lives in [decisions/](decisions/); test inventory in [testing-coverage.md](testing-coverage.md).

| | |
|---|---|
| **Stability** | Pre-**1.0** (APIs and env surface evolve — see [decisions/](decisions/)) |
| **crates.io** | [`0.6.1`](https://crates.io/crates/trembita) · [release notes](releases/0.6.1.md) |
| **MSRV** | 1.94 |
| **Distribution** | Published on [crates.io](https://crates.io/crates/trembita) — full test pyramid, E2E/chaos |

---

## At a glance

**Product scenarios** (embedded redb, no mandatory Redis):

| Scenario | Guide | Status |
|----------|-------|--------|
| Background jobs | [scenarios/background-jobs.md](scenarios/background-jobs.md) | ✅ queue, DLQ, cron, external backlog |
| Event topics | [scenarios/event-topics.md](scenarios/event-topics.md) | ✅ pub/sub, named subscriptions |
| Stateful workers | [scenarios/stateful-workers.md](scenarios/stateful-workers.md) | ✅ capabilities + `RedbActorStateStore`; **`TREMBITA_MIGRATE_DEMO`** = advanced `UserActor` lab only |
| Capabilities (typed ops) | [scenarios/capabilities.md](scenarios/capabilities.md) · [structural-limits](scenarios/structural-limits.md) | ✅ `OpCtx` store/deps/ingress + [`consensus` helpers](../crates/trembita/src/capability/consensus.rs); RYW directory; doctor store guard; **auto group scale** (B-28) + **founder scale map / doctor foot-guns** (B-31 — [capability-dx § Founder scale](decisions/capability-dx.md#founder-scale-model-b-31)) |
| Real-time / session | [scenarios/realtime-sessions.md](scenarios/realtime-sessions.md) | ✅ `ActorSession`, gateway WS; **cluster session cookies** ([gateway-cluster-auth](decisions/gateway-cluster-auth.md)) |
| Workflows | [scenarios/workflows.md](scenarios/workflows.md) | ✅ Meta-Raft saga journal |
| Product API | [getting-started.md](getting-started.md) | ✅ `capabilities/` + **`async` [`#[cap_handler]`](../crates/trembita-macros/src/lib.rs)** + gateway [`cap_*`](../crates/trembita/src/gateway/cap_handlers.rs); `/actors/*` off by default ([`WorkerOpts::http_cast`](../crates/trembita/src/worker_opts.rs) advanced) |

**Platform core:** pure Raft FSM, HTTP/3/mTLS, redb persistence, supervised runtime (capability hosts + optional advanced `UserActor`), multi-Raft sharding, cross-shard saga/2PC, self-update coordinator, E2E/chaos. Product surface: [product-terminology](decisions/product-terminology.md).

**Not goals:** linearizable actor `ask`, global cross-shard serializable isolation. **Optional work:** [backlog.md](backlog.md#open-work).

Details below ↓

---

## Product scale wave (B-28–B-32)

Shipped backlog items for **«add VPS + same binary»** — capability compute, gateway sessions, ingress health checks, founder-safe manifests, and coordination-scale queues / multi-Raft on the **product** API.

| Id | What shipped | Read first | Automated regression |
|----|--------------|------------|----------------------|
| **B-28** | Auto **[`resolved_scale`](../crates/trembita/src/capability/group.rs)** on cap groups (marker → **PerNode**, shared RAM / session → **Fixed**) | [capabilities § Group scale](scenarios/capabilities.md#group-scale-b-28) · [capability-dx § B-28](decisions/capability-dx.md#group-scale-b-28) | [`cap_scale.rs`](../crates/trembita/src/integration/cap_scale.rs), [`group.rs`](../crates/trembita/src/capability/group.rs) unit tables |
| **B-29** | **Cluster session cookies** — [`ClusterSessionSecret`](../crates/trembita/src/gateway/cluster_session.rs), optional cap-store registry; same secret on all gateway nodes | [gateway-cluster-auth](decisions/gateway-cluster-auth.md) · [realtime § B-29](scenarios/realtime-sessions.md#cluster-session-cookies-b-29) | [`gateway_cluster_session.rs`](../crates/trembita/tests/gateway_cluster_session.rs), [`cluster_session.rs`](../crates/trembita/src/gateway/cluster_session.rs) |
| **B-30** | **Ingress / LB** recipe — liveness **`GET /health`** vs pool **`GET /ready`** on unified `TREMBITA_LISTEN` | [ops/ingress-lb.md](ops/ingress-lb.md) | [`ingress_lb_ops.rs`](../crates/trembita/tests/ingress_lb_ops.rs), [`ingress_lb.rs`](../crates/trembita/src/integration/ingress_lb.rs) |
| **B-31** | **Founder scale** vocabulary + **`trembita doctor`** foot-guns (`.instances(1)` + queued, session without `.per_node()`) | [capability-dx § Founder scale](decisions/capability-dx.md#founder-scale-model-b-31) · [product-terminology](decisions/product-terminology.md) | [`doctor.rs`](../crates/trembita-cli/src/scaffold/doctor.rs), [`cap_scale_doctor.rs`](../crates/trembita-cli/tests/cap_scale_doctor.rs) |
| **B-32** | **Coordination scale** — sharded / auto-shard job queue + multi-Raft via [`QueueOpts` / `JobOpts`](../crates/trembita/src/queue_opts.rs), [`TrembitaConfigure`](../crates/trembita/src/configure.rs), **`TREMBITA_*`** | [capabilities § Coordination scale](scenarios/capabilities.md#coordination-scale-b-32) · [env.md](env.md) | [`product_coordination_scale.rs`](../crates/trembita/tests/product_coordination_scale.rs), [`env_config.rs`](../crates/trembita-assembly/src/env_config.rs) `b32_*` |

Full test commands: [testing-coverage § B-28–B-32](testing-coverage.md#shipped-backlog-b-28b32). Planning snapshot: [archive/backlog-wave-b28-b32.md](archive/backlog-wave-b28-b32.md).

---

## Shipped capabilities (detail)

### Consensus & storage

- Pure Raft FSM (`trembita-core`) — election, replication, joint-consensus membership, snapshots, compaction
- ReadIndex linearizable reads, leader lease-read fast path, follower linearizable reads
- Durable log via redb (`trembita-storage`); per-group `group-<id>.redb` with `TREMBITA_DATA_DIR` / [`TrembitaConfigure::with_data_dir`](../crates/trembita/src/configure.rs)

### Network & security

- HTTP/3 / QUIC + mTLS (`trembita-net`); dedicated peer connection per traffic class
- Opt-in per-class token-bucket rate limiting (`TrafficPolicy` / `RateLimiter`)
- mTLS hot reload (`PemSecurity`, `CertReloadHandle`, step-ca example)
- Dev-only JSON wire (`trembita/json-wire` feature)

### Cluster operations

- Dynamic join via seed set + DNS discovery (`trembita::discovery`, `join_seeds`)
- Cluster leave RPC (`TrembitaCluster::leave`, `TREMBITA_ALLOW_LEAVE`)
- Unified ops + product HTTP on **`TREMBITA_LISTEN`** (same port as QUIC; [`TrembitaApp::from_env`](../crates/trembita/src/app/runtime.rs) default surfaces) — [env.md](env.md), [unified-listener](decisions/unified-listener.md)
- Reachability signal distinct from membership; crash-driven supervisor reconcile against `reachable_nodes()`
- Phi-accrual / tunable reachability (`ReachabilityConfig`)
- `trembita-ops` snapshot backup/restore; rolling wire N/N−1 compatibility
- **Self-update coordinator** — `trembita_core::upgrade` reference SM, leader reconcile + local executor (`trembita::upgrade`), HTTP `GET/POST /cluster/upgrade*` ([upgrade-coordinator](decisions/upgrade-coordinator.md), [examples/self-update](../examples/self-update/))

### Capabilities (product)

- [`CapManifest`](../crates/trembita/src/capability/manifest.rs), `#[cap_handler]`, routes (`Inline`, `Queued`, `Session`, …), gateway `cap_*`, `OpCtx` store/deps/ingress — [capability-dx](decisions/capability-dx.md), [getting-started.md](getting-started.md)
- Default gateway **without** `/actors/*` ([product-terminology](decisions/product-terminology.md))

### Runtime placement (internal + advanced)

- Supervised workers (capability **`CapHost`**, optional app **`UserActor`**), cross-node deliver, auto-spawn on join, one instance/VPS typical in production
- Consistent-hash ring, sticky `ActorSession`, per-group drain (`TREMBITA_DRAIN_TIMEOUT`); **`CapManifest` apps default `DirectoryPolicy::ReadYourWrites`** (`TrembitaAppBuilder`); `ask_linearizable` (directory visibility only, not SM-linearizable reads)
- **Cap store** — `RedbCapStateStore` + voter replication; auto with `.data_dir()` ([actor-state-store](decisions/actor-state-store.md)); TTL/GC; optional Redis (`redis-store`) and Postgres (`capstore-postgres`, transactional CAS); migration RPC for advanced workers
- **Durable mailbox spool** — assembly-only [`durable_mailbox`](../crates/trembita-assembly/src/builder/cluster/config.rs) + `/actor/deliver` wire (not `TrembitaApp` today)

**Job queue** ([job-queue](decisions/job-queue.md)): `RedbJobQueue`, batch enqueue/ack, prefetch, DLQ, cron, `ClusterJobQueue`, `#[trembita::consumer]`, autoscale; sharded / auto-shard queues via **product** [`QueueOpts` / `JobOpts`](../crates/trembita/src/queue_opts.rs) and **`TREMBITA_JOB_QUEUE_*`** env (B-32); assembly **`job_queue_sharded`** / **`job_queue_auto_shard`** unchanged for embedders; **`ExternalBacklog`** ([external-backlog](decisions/external-backlog.md), facade feature `external-backlog`); **`ScheduleSource`** ([schedule-source](decisions/schedule-source.md)).

**Event topics** ([event-topics](decisions/event-topics.md)): durable pub/sub, named subscriptions, voter replication; [`TopicOpts`](../crates/trembita/src/topic_opts.rs), [`.topics()`](../crates/trembita/src/app/mod.rs); **`EventOutboxSource`** ([event-outbox](decisions/event-outbox.md)) for transactional outbox drain.

**Gateway & HTTP** ([unified-listener](decisions/unified-listener.md), [gateway-routing-v2](decisions/gateway-routing-v2.md), [gateway-identity](decisions/gateway-identity.md)): one TCP bind on `TREMBITA_LISTEN`; default ops/jobs surfaces from env boot or explicit `.surfaces()`; `AuthMode` on route tables; native `Gateway`/`RouteTable` + hyper WebSocket. Cluster-only: [`spawn_cluster_ops_http`](../crates/trembita/src/gateway/cluster_ops.rs). Env: [env.md](env.md). **Ops:** VPS ingress/LB recipe [ops/ingress-lb.md](ops/ingress-lb.md) (B-30).

**Workload governor** ([workload-governor](decisions/workload-governor.md)): per-node compute tokens + consumer tuning from gateway connections, **in-flight HTTP**, **consumer in-flight**, and queue depth; `ConsumerTune::max_in_flight` caps concurrent handlers across consumer instances; subprocess load via [`compute_cost`](../crates/trembita/src/job_opts.rs) + optional [`ExternalLoad`](decisions/external-load.md) ([external-load](decisions/external-load.md)).

**Consumer DX** — `#[consumer_json]`, `ConsumerOpts::on_app`, `IdempotencyOpts::retain_for`, graceful drain, workflow step helpers.

**E2E & showcases** — `./e2e/queue.sh`, gateway/idempotency scripts; `examples/background-jobs`, `stateful-workers`, `realtime`, `workflows`.

### Multi-Raft write scaling

| Layer | API / component |
|-------|-----------------|
| Routing | `ShardRouter`, `StableShardRouter` (default), rendezvous `place_shard` / `place_group` |
| Runtime | `ShardedNodeService`, `spawn_multi_raft_node`, Meta-Raft coordinator, keyed `ProposeKeyed` / `QueryKeyed` |
| Modulus routing | Per-group learners, `expand_shard_count`, `propose_keyed_batch`, `/introspect/raft-groups` |
| Stable shards & catalog | Dynamic catalog (`add_raft_groups`), stable shard activation (`activate_shards`, `switch_to_stable_shards`), `catalog_version`; product boot via **`TrembitaConfigure::with_coordination_raft_groups`** / **`TREMBITA_RAFT_*`** (B-32, `EmptyStateMachine` apps) |
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

- **crates.io / docs.rs publish** — periodic `0.x` snapshots; full semver history from **1.0** ([CHANGELOG.md](../CHANGELOG.md))
- **Public API docs** — `missing_docs = "deny"` on published crates; `publish = false` crates exempt via crate lint override. Audit: `./scripts/docs-missing-audit.sh`
- **Real-world soak** — scenario harness in `benchmarks/` (`soak`, `soak_queue`, `soak_multi_raft`, `soak_actor_store`, `soak_saga`, `soak_session`); scheduled CI `bench` job (60–120s budgets); long-running production soak is operator responsibility
- **Heavy integration tests** — Redis/docker tests gated `#[ignore]` in fast CI; scheduled heavy lane

---

## Known structural limits

Documented in [future-work-and-risks](decisions/future-work-and-risks.md):

| Risk | Summary |
|------|---------|
| **R1** | Single Raft group still has a per-group write ceiling; mitigation is **multi-Raft + add groups**, not bigger VPS count alone — [write-scaling](scenarios/write-scaling.md) |
| **R2** | Shared QUIC listener — mitigated by peer connection isolation + optional rate limiting |
| **R3** | Actor directory is eventually consistent — mitigated by TTL, RYW (default for capabilities), anti-entropy — [structural-limits](scenarios/structural-limits.md#r3--actor-directory-is-eventually-consistent) |
| **R4** | Handler RAM without cap store is lost on crash — `OpCtx::require_store`, domain in SM via `propose` — [structural-limits](scenarios/structural-limits.md#r4--handler-ram-without-cap-store) |
| **R5** | Deep tracing has a performance cost — metrics on by default, tracing opt-in |
| **R6** | mTLS ops burden — mitigated by hot reload + step-ca example |

---

## Where to read next

| Doc | Purpose |
|-----|---------|
| [examples/README.md](../examples/README.md) | Product showcases (local + QUIC cluster) |
| [scenarios/README.md](scenarios/README.md) | Product scenario index |
| [backlog.md](backlog.md) | Open work |
| [status § B-28–B-32](#product-scale-wave-b-28b32) | Shipped product scale wave |
| [../CONTRIBUTING.md](../CONTRIBUTING.md) | Contributor guide (humans) |
| [architecture.md](architecture.md) | Crate graph, data flows |
| [decisions/](decisions/) | Design decision records |
| [testing-coverage.md](testing-coverage.md) | Test inventory |
| [CHANGELOG.md](../CHANGELOG.md) | Changelog policy (detailed history from **1.0**) |
| [releasing.md](releasing.md) | Publish workflow |
