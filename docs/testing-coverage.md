# Testing coverage matrix

Living inventory of what the trembita test suite covers, where gaps remain, and
which CI lane exercises each layer. Update this file when adding tests or
closing a gap.

**Strategy:** [testing-strategy](decisions/testing-strategy.md)  
**Feature status:** [status.md](status.md)  
**Last audit:** 2026-08-31

Legend: **✅** covered · **⚠️** partial · **❌** missing · **🔒** scheduled / `#[ignore]` only

---

## Test pyramid (current)

| Layer | Scope | Location | Count (approx.) | Fast CI |
|-------|-------|----------|-----------------|---------|
| Unit | Pure functions, small modules | `#[cfg(test)]` in `src/` | ~70 | ✅ |
| Integration | Crate boundaries, async runtime | `crates/*/tests/` | ~260 | ✅ |
| Property | Raft safety under fault schedules | `trembita-sim/tests/safety.rs` (+ proptest) | 250+ seeds | ✅ |
| Compile-fail | Macro misuse → good errors | `trembita-actor/tests/compile_fail.rs` | 3+ | ✅ |
| Deterministic sim | Whole cluster, virtual clock | `trembita-sim` harness + scenarios | 27 tests | ✅ |
| Linearizability | Client-visible histories | `trembita-sim/tests/linearizability.rs` | 2 | ✅ |
| Doctests | Public API examples | `cargo test --doc` | — | ✅ |
| Redis integration | Real `ActorStateStore` | `trembita-store-redis/tests/{redis,tls}.rs` | 10 | 🔒 nightly |
| E2E | Real processes, QUIC, mTLS, chaos | `e2e/run.sh`, `e2e/leave.sh`, `e2e/queue.sh`, `e2e/chaos.sh`, `e2e/cert_renew.sh`, `e2e/linearizability.sh` | 6 scenarios | 🔒 nightly |
| Fuzz | Wire decode never panics | `trembita-fuzz` | 1 target | 🔒 nightly |
| Bench / soak | Throughput, long-run sim | `benchmarks/` | — | 🔒 nightly |
| Examples | Product showcases (standalone crates) | [`examples/`](../examples/README.md) | 4 apps | `./scripts/check-examples.sh` |

---

## Per-crate inventory

| Crate | Unit (`src/`) | Integration (`tests/`) | Total | Primary focus |
|-------|:-------------:|:----------------------:|:-----:|---------------|
| `trembita-core` | 30 | 81 | **111** | Pure Raft FSM: election, replication, membership, snapshots, ReadIndex |
| `trembita-actor` | 10 | 99 | **109** | `RaftDriver`, runtime, registry, placement, supervision, migration, trybuild |
| `trembita-net` | 12 | 32 | **44** | Wire framing, `LocalNetwork`, TLS handshake, loopback QUIC, protocol compat |
| `trembita-sim` | 8 | 22 | **30** | Safety/liveness under faults, linearizability, actor scenarios, multi-Raft |
| `trembita-dashboard` | 8 | **8** | **16** | Admin HTTP, admin TLS, metrics, telemetry |
| `trembita` (facade) | 2 | **24** | **26** | `TrembitaCluster` builder, multi-Raft, keyed client, live QUIC cluster, reachability reconcile, actor store resume, graceful leave, admin TLS, DNS discovery |
| `trembita-storage` | 0 | 7 | **7** | Store contract (Memory + Redb), namespaced groups, reopen |
| `trembita-proto` | 7 | 0 | **7** | Encode/decode roundtrips, protocol compat band |
| `trembita-store-redis` | 0 | 10 (7 `redis` + 3 `tls`, `#[ignore]` except 2 fast) | **10** | Redis CAS/TTL, dual conn, idempotent worker, reconnect, `rediss://` |
| `trembita-client` | 1 | **8** | **9** | Remote client propose/query, follower forward, failover, retry policy, keyed batch |
| `trembita-ops` | 0 | 2 | **2** | Snapshot export/import, object-store push/pull |
| `trembita-macros` | — | via trybuild in `trembita-actor` | — | Compile-pass/fail |
| `trembita-node` | **10** | 0 | **0** | *(E2E smoke only)* |

Count tests locally:

```sh
cargo test --workspace --all-features --lib --tests -- --list | rg ': test$' | wc -l
```

---

## Coverage by functional area

### Consensus (`trembita-core` + driver)

| Area | Unit | Integration | Sim | E2E | Status |
|------|:----:|:-----------:|:---:|:---:|--------|
| Leader election (Pre-Vote) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Log replication / conflict truncate | ✅ | ✅ | ✅ | — | ✅ |
| Joint-consensus membership | ✅ | ✅ | ✅ | — | ✅ |
| ReadIndex linearizable reads | ✅ | ✅ | ✅ | — | ✅ |
| Follower reads (ReadIndexConfirm + local query) | ✅ | ✅ | — | — | ✅ |
| Lease reads | ✅ | ✅ | — | — | ✅ |
| Snapshots + log compaction | ✅ | ✅ | ✅ | — | ✅ |
| Auto-compaction policy (`CompactionPolicy`, runtime) | ✅ | ✅ `runtime`, `auto_compaction` | — | — | ✅ |
| `take_persist` / `restore` (core) | ✅ | ✅ | — | — | ✅ |
| Write sharding / multi-Raft routing | ✅ | ✅ `multi_raft` | ✅ | — | ✅ |
| Dynamic catalog expansion (pure planner) | ✅ `shard` | — | — | — | ✅ |
| Stable virtual shards (pure planner) | ✅ `shard` | — | — | — | ✅ |
| Dynamic catalog expansion (runtime) | — | ✅ `multi_raft` | — | — | ✅ |
| Stable shard routing (runtime) | — | ✅ `multi_raft`, `sharded` | — | — | ✅ |
| Cross-shard atomic transactions | — | ✅ `saga`, `two_phase`, `trembita-client` | ✅ `two_phase_journal`, `two_phase_client_journal` | ✅ `trembita-sim/tests/two_phase` | ✅ |
| Durable 2PC log entries (`EntryPayload::TwoPhasePrepare/Abort`) | ✅ `two_phase_journal` | ✅ `driver`, `runtime`, `two_phase` | — | ✅ `trembita-sim/tests/two_phase` | ✅ |
| 2PC client journal Meta-Raft (`EntryPayload::TwoPhaseJournal`) | ✅ `two_phase_client_journal` | ✅ `driver`, `runtime`, `two_phase` | — | — | ✅ |
| Saga journal Meta-Raft metadata (`EntryPayload::SagaJournal`) | ✅ `saga_journal` | ✅ `driver`, `runtime`, `saga` | — | — | ✅ |
| Meta-Raft coordinator (multi-Raft join/catalog/saga isolation) | ✅ `shard` | ✅ `sharded` | — | — | ✅ |
| Per-group membership planner (`group_voters`, join/leave affects) | ✅ | ✅ | — | — | ✅ |
| Per-group membership runtime sync on cluster join | — | ✅ | — | — | ✅ |
| Per-group learners (`group_learners`, membership sync, rebalance hosting) | ✅ | ✅ `group_rebalance` | ✅ `learners` | — | ✅ |
| Operator shard expansion (`expand_shard_count`, modulus routing) | ✅ | ✅ `multi_raft` | — | — | ✅ |
| Stable shard activation (`activate_shards`) | ✅ | ✅ `multi_raft`, `sharded` | — | — | ✅ |
| Cluster leave RPC (`/cluster/leave`, `TrembitaCluster::leave`) | — | ✅ `runtime`, `multi_raft` | — | — | ✅ |
| Leader-side reachability / hysteresis / phi-accrual | ✅ | ✅ | — | — | ✅ |
| Wire protocol N/N−1 compat band | ✅ | ✅ | — | — | ✅ |
| Rolling upgrade coordinator (`trembita_core::upgrade`, leader-last grant) | ✅ | ✅ `upgrade`, `upgrade_coordinator` | — | — | ✅ |
| Admin HTTPS (server TLS) | ✅ | ✅ `admin`, `facade` | — | 🔒 nightly | ✅ |
| Snapshot backup CLI (`trembita-ops`) | — | ✅ | — | — | ✅ |
| External linearizability (Jepsen-lite) | — | — | ✅ | ✅ `linearizability.sh` | ✅ |
| Malformed persistence payloads | — | ✅ driver | — | — | ✅ |

| Area | Unit | Integration | Sim | E2E | Status |
|------|:----:|:-----------:|:---:|:---:|--------|
| Store contract (Memory ≡ Redb) | — | ✅ | — | — | ✅ |
| Redb reopen after "crash" | — | ✅ | — | — | ✅ |
| Namespaced multi-group layout | — | ✅ | — | — | ✅ |
| `RaftDriver` restart + replay | — | ✅ driver | — | — | ✅ |
| **`TrembitaCluster` + `data_dir` restart** | — | ✅ `persistence` | — | — | ✅ |
| Snapshot survives facade restart | — | ✅ `persistence` | — | — | ✅ |
| Backend error injection | — | ✅ driver | — | — | ✅ |

### Transport (`trembita-net`)

| Area | Unit | Integration | Sim | E2E | Status |
|------|:----:|:-----------:|:---:|:---:|--------|
| Wire encode/decode + size guard | — | ✅ (16) | — | — | ✅ |
| `LocalNetwork` (in-process) | — | ✅ | ✅ | — | ✅ |
| mTLS mutual auth (loopback) | — | ✅ | — | ✅ | ✅ |
| PEM hot reload (loopback QUIC) | — | ✅ `cert_reload` | — | ✅ | ✅ |
| PEM hot reload (docker-compose, SIGHUP + poll) | — | — | — | ✅ `cert_renew` | ✅ |
| Live HTTP/3 QUIC (loopback) | ✅ | ✅ `dev-certs` | — | ✅ | ✅ |
| Connection pool + backoff | ✅ | ✅ `quic` | — | — | ✅ |
| Partition / drop injection (net layer) | — | ✅ detach | ✅ sim | ✅ chaos | ✅ |

### Actor runtime (`trembita-actor`)

| Area | Unit | Integration | Sim | E2E | Status |
|------|:----:|:-----------:|:---:|:---:|--------|
| Registry / messaging | ✅ | ✅ | — | — | ✅ |
| Keyed routing (consistent hash ring) | ✅ `ring` | ✅ `messaging`, `directory` | — | — | ✅ |
| Sticky session / actor lease | ✅ `session` | ✅ `messaging` | — | — | ✅ |
| `ask_linearizable` (directory retry) | — | ✅ `messaging` | — | — | ✅ |
| Directory `ReadYourWrites` policy | ✅ `directory_policy` | ✅ `messaging` | — | — | ✅ |
| Per-group drain timeout override | — | ✅ `migration` | — | — | ✅ |
| Placement / supervisor | — | ✅ | ✅ `rebalance_churn` | — | ✅ |
| Crash-driven auto-respawn (reachable ≠ membership) | — | ✅ | ✅ | — | ✅ |
| Cross-node spawn / migration | — | ✅ | ✅ `actor_scenarios` | — | ✅ |
| Group rebalance / sharded runtime | — | ✅ | — | — | ✅ |
| Raft group migration bundle + respawn | ✅ storage | ✅ `group_migrate` | — | — | ✅ |
| Group migrate RPC (facade wire) | — | ✅ `multi_raft` | — | — | ✅ |
| Runtime fatal-error path | — | ✅ `runtime` | — | — | ✅ |
| Actor state store (in-memory) | ✅ | ✅ | — | — | ✅ |
| Actor state store TTL + GC (`RedbActorStateStore`) | ✅ `redb_store` | ✅ `store` | — | — | ✅ |
| **Job queue (`JobQueue`, in-memory + redb)** | ✅ `queue`, `redb_queue` | ✅ `queue` | — | — | ✅ |
| **Job queue wire + cluster client** | ✅ `queue_service` | ✅ `queue` (facade, autoscale) | — | — | ✅ |
| **Job queue voter replication (sync `/queue/replicate`)** | ✅ `redb_queue` (`apply_replicate`) | ✅ `queue` (all voters redb, follower lease after shutdown) | — | — | ✅ |
| **Job queue sharded streams + priority/delayed + membership autoscale** | ✅ `sharded_queue`, `queue` | ✅ `queue` | — | — | ✅ |
| **Job queue replicate auth + parallel replicate** | ✅ `queue_service` | ✅ `queue` (`queue_replicate_rejects_non_leader_caller`) | — | — | ✅ |
| **Meta-Raft queue autoscale policy** | ✅ `queue_autoscale_policy` (core) | ✅ `queue` (autoscale tests) | — | — | ✅ |
| **RedbJobQueue ack-driven compaction** | ✅ `redb_queue` | ✅ `queue` | — | — | ✅ |
| **Queue throughput (batch enqueue/ack + leader prefetch)** | ✅ `redb_queue`, `queue_prefetch` | ✅ `queue`, `queue_throughput` | — | — | ✅ |
| **Event topics (`EventTopic`, pub/sub + named subscriptions)** | ✅ `topic`, `redb_topic` | — | — | — | ✅ |
| **Event topic wire + voter replication (`/topic/replicate`)** | ✅ `topic_service` | — | — | — | ✅ |
| **Event topic leader failover (replicated cursors)** | — | ✅ `topic_failover` | — | — | ✅ |
| **Transactional event outbox drainer (`EventOutboxSource`)** | ✅ `event_outbox` | ✅ `event_outbox` (facade) | — | — | ✅ |
| **Leader task primitive (`LeaderSession`, `run_leader_loop`, `on_leader`)** | ✅ `leader_task` | ✅ `leader_task` | — | — | ✅ |
| **External backlog (feeder + settle outbox drainer + autoscale depth)** | ✅ `external_backlog`, `backlog_settle_outbox` | ✅ `external_backlog` (facade) | — | — | ✅ |
| **Workload governor (compute tokens + consumer tune + actor ask)** | ✅ `compute_token`, `workload`, `messaging` | ✅ `workload_governor` | — | — | ✅ |
| **Job queue E2E (QUIC enqueue → follower lease/ack → leader failover)** | — | — | — | ✅ `e2e/queue.sh` | ✅ |
| **Durable mailbox outbox/inbox** | ✅ `mailbox_spool` | ✅ `mailbox_spool` (wire) | — | — | ✅ |
| Actor state store resume + idempotency (facade) | — | ✅ `actor_store_resume` | — | — | ✅ |
| Actor state store (Redis) | — | 🔒 ignore | — | — | 🔒 |
| Actor state store (`rediss://` + private CA) | — | 🔒 ignore | — | — | 🔒 |

### Client (`trembita-client`)

| Area | Unit | Integration | Sim | E2E | Status |
|------|:----:|:-----------:|:---:|:---:|--------|
| Propose + query (any node) | — | ✅ | — | — | ✅ |
| Follower-only target: write forwards, read local | — | ✅ | — | — | ✅ |
| Failover (detached node) | — | ✅ | — | — | ✅ |
| `NoTargets` | — | ✅ `retry` | — | — | ✅ |
| `NotLeader` hint follow (explicit) | — | ✅ `retry` | — | — | ✅ |
| Max attempts / timeout exhaustion | — | ✅ `retry` | — | — | ✅ |
| Keyed propose/query (multi-Raft) | — | ✅ `client_keyed` | — | — | ✅ |
| Cross-shard keyed batch (`propose_keyed_batch`, partial failure) | ✅ `batch` | ✅ `multi_raft` | — | — | ✅ |

### Facade & reference binary

| Area | Unit | Integration | Sim | E2E | Status |
|------|:----:|:-----------:|:---:|:---:|--------|
| `TrembitaCluster` local 3-node | — | ✅ | — | — | ✅ |
| Admin / observability HTTP | — | ✅ | — | ✅ `run.sh` | ✅ |
| Gateway introspection (`/introspect/*` on product router) | ✅ `introspect_routes` | ✅ `gateway_introspect_http` | — | — | ✅ |
| Multi-Raft introspection (`/introspect/raft-groups`) | — | ✅ `admin`, `multi_raft` | — | — | ✅ |
| Actors + auto-spawn | — | ✅ | ✅ `auto_spawn` | — | ✅ |
| Multi-Raft file layout (`data_dir`) | — | ✅ `persistence` + `multi_raft` | — | — | ✅ |
| DNS discovery | ✅ | ✅ `discovery` | — | ✅ | ✅ |
| **`trembita-node` env parsing** | ✅ `config` | — | — | ✅ implicit | ✅ |
| **`trembita-node` drain timeout (`TREMBITA_DRAIN_TIMEOUT`)** | ✅ `config` | — | — | ✅ implicit | ✅ |
| **`trembita-node` graceful leave on shutdown** | ✅ `config` | ✅ `graceful_leave` | — | ✅ `leave.sh` | ✅ |

### Macros & wire

| Area | Unit | Integration | Sim | E2E | Status |
|------|:----:|:-----------:|:---:|:---:|--------|
| `StateMachine` / `actor` attribute | — | ✅ trybuild | — | — | ✅ |
| Proto roundtrip | ✅ | — | — | — | ✅ |
| Wire decode fuzz | — | — | — | — | ✅ (nightly) |
| Join/leave group 1 membership sync | — | ✅ `multi_raft` | — | — | ✅ |
| Multi-Raft follower partition | — | ✅ `multi_raft` | — | — | ✅ |

---

## CI lane mapping

| Job | When | What runs |
|-----|------|-----------|
| `fast` | Every MR / push | fmt, clippy `-D warnings`, nextest (all non-ignored tests), doctests, doc |
| `msrv` | Every MR / push | `cargo check` on Rust 1.90 |
| `e2e` | Scheduled | `e2e/run.sh` + `e2e/leave.sh` + `e2e/chaos.sh` + `e2e/cert_renew.sh` + docker phase of `e2e/linearizability.sh` |
| `linearizability-sim` | Scheduled | trembita-sim linearizability + read_index seed sweep (`e2e/linearizability.sh`) |
| `store-redis` | Scheduled | `cargo test -p trembita-store-redis -- --ignored` |
| `bench` | Scheduled | criterion (`append`/`apply`/`deliver`/`queue`) + 120s `soak` + 60s `soak_multi_raft` + 60s `soak_queue` + 60s `soak_actor_store` + 60s `soak_saga` + 60s `soak_session` |
| `fuzz` | Scheduled | `cargo-fuzz` wire_decode in `crates/trembita-fuzz` |

Local hooks mirror the fast lane: `lefthook` pre-commit (fmt, shellcheck, clippy —
skipped when no matching staged files) and pre-push (clippy → tests → doctests → doc
→ publish dry-run → MSRV; optional release via `--tags release`). See
`scripts/quality-gate-*.sh`.

---

## Known gaps (prioritized)

Track open gaps here; move rows to **Closed gaps** when fixed.

| Priority | Gap | Suggested test location | Effort |
|----------|-----|-------------------------|--------|

### Closed gaps

| Closed | What | Where |
|--------|------|-------|
| 2026-08 | Scenario soak per product path (actor store, saga resume, session restart) | `benchmarks/soak_{actor_store,saga,session}.rs`, `.gitlab-ci.yml` `bench` |
| 2026-08 | Gateway identity, `SessionHandle`, WS + HTTP E2E, gateway drain | `trembita/src/gateway/`, `trembita/tests/{gateway_identity,gateway_ws,gateway_http}.rs`, `examples/{realtime,stateful-workers}/`, `docs/decisions/gateway-identity.md` |
| 2026-09 | Introspect API on product gateway (`IntrospectApi`, `with_introspect_api`, `AuthFn`) | `trembita-http/src/introspect_routes.rs`, `trembita/tests/gateway_introspect_http.rs`, `docs/decisions/introspect-api.md` |
| 2026-09 | Gateway virtual-host dispatch (`HostRouter`, strict default, loopback dev fallback) | `trembita-http/src/host_router.rs` (unit), `trembita-http/README.md`, `trembita/src/gateway/mod.rs` |
| 2026-09 | Durable event topics (`EventTopic`, min-cursor compaction, retention discard, voter replication) | `trembita-actor/src/{topic,redb_topic,topic_service}.rs`, `trembita-actor/tests/topic_failover.rs`, `trembita/src/topic_opts.rs`, `docs/decisions/event-topics.md` |
| 2026-09 | Dynamic schedule source (`ScheduleSource`, diff reconcile, leader replication) | `trembita-actor/src/schedule_source.rs`, `trembita-actor/tests/schedule_source.rs`, `trembita/tests/schedule_source.rs`, `docs/decisions/schedule-source.md` |
| 2026-08 | Transport + facade gaps: QUIC backoff, DNS discovery, queue compaction, auto-spawn sim, admin/leave E2E | `trembita-net/tests/quic.rs`, `trembita/tests/{discovery,queue}.rs`, `trembita-sim/tests/{auto_spawn,actor_scenarios}.rs`, `e2e/{run,leave}.sh` |
| 2026-08 | Runtime fatal-error observable path (`status()` → `None`, `Stopped`) | `trembita-actor/tests/runtime.rs` |
| 2026-08 | Multi-Raft sim: shard routing + independent group safety | `trembita-sim/tests/multi_raft.rs` |
| 2026-08 | Group rebalance planner + sharded adopt/retire runtime | `trembita-actor/tests/group_rebalance.rs`, `sharded.rs` |
| 2026-08 | Malformed persistence + backend error injection at driver | `trembita-actor/tests/driver.rs` |
| 2026-08 | Cluster leave RPC + `TrembitaCluster::leave()` facade | `trembita-actor/tests/runtime.rs`, `trembita/tests/multi_raft.rs`, `docs/decisions/cluster-membership.md#leave-rpc` |
| 2026-08 | Injectable Tokio clock in integration tests (`trembita-test-support::clock`, `start_paused`) | `trembita-test-support`, `trembita/tests/*`, `trembita-actor/tests/*`, `trembita-client/tests/cluster.rs` |
| 2026-08 | Snapshot survives facade restart (`compact` + `data_dir`) | `trembita/tests/persistence.rs` |
| 2026-08 | 3-node majority survives one member restart | `trembita/tests/persistence.rs` |
| 2026-08 | Shared KV fixtures + harness helpers (dedupe ~8 copies) | `trembita-test-support` (`Kv`, `TrackedKv`, `find_keys_for_two_groups`, cluster polling) |
| 2026-08 | Stable shard router runtime (`StableShardRouter`, `activate_shards`, builder default) | `trembita-actor/sharded`, `trembita/tests/multi_raft.rs`, `trembita-actor/tests/sharded.rs` |
| 2026-08 | Linearizability E2E phase 2 (QUIC `trembita-e2e-client` + external checker) | `crates/trembita-e2e-client`, `e2e/linearizability.sh`, `e2e/docker-compose.yml` |
| 2026-08 | Hardening: graceful leave integration, admin HTTPS E2E | `trembita/tests/graceful_leave.rs`, `trembita/tests/facade.rs`, `trembita-dashboard/tests/admin.rs` |
| 2026-08 | Wire decode fuzz (`cargo-fuzz` wire_decode, scheduled CI) | `crates/trembita-fuzz/`, `.gitlab-ci.yml` `fuzz` job |
| 2026-08 | Multi-Raft modulus routing: learners planner, shard expansion, keyed batch, `/introspect/raft-groups` | `trembita-core`, `trembita-client`, `trembita-dashboard`, `trembita/tests/multi_raft.rs`, `docs/decisions/multi-raft.md#modulus-routing--keyed-batch` |
| 2026-08 | Client retry edge cases (`NoTargets`, timeout, `NotLeader`, unreachable) | `trembita-client/tests/retry.rs` |
| 2026-08 | Keyed client routing (multi-Raft propose/query) | `trembita/tests/client_keyed.rs` |
| 2026-08 | Stable shards & catalog: `catalog_version`, `switch_to_stable_shards`, saga hardening | `trembita-core/shard.rs`, `trembita/src/cluster.rs`, `trembita-client/src/saga.rs`, `trembita/tests/{multi_raft,saga}.rs` |
| 2026-08 | Cross-shard saga coordinator (`run_saga`, `StoreSagaJournal`) | `trembita-client/src/saga.rs`, `trembita/src/saga.rs`, `trembita/tests/saga.rs`, `docs/decisions/multi-raft.md#cross-shard-transactions` |
| 2026-08 | Saga hardening v2: group 0 journal fallback + coordinator restart resume | `trembita-proto/saga_journal`, `trembita-core/node`, `trembita-actor/runtime`, `trembita/src/saga.rs`, `trembita/tests/saga.rs` |
| 2026-08 | Saga journal layered tests (proto/core/driver/runtime/facade) | `trembita-proto`, `trembita-core/tests/saga_journal.rs`, `trembita-actor/tests/{driver,runtime}.rs`, `trembita/tests/saga.rs` |
| 2026-08 | Durable 2PC prepare timeout GC + `resume_cross_shard_2pc` | `trembita-actor/runtime`, `trembita-client/two_phase`, `trembita/tests/two_phase.rs` |
| 2026-08 | 2PC facade + client journal + metrics + sim partition test | `trembita/src/two_phase.rs`, `trembita-proto/two_phase_journal`, `trembita-sim/tests/two_phase.rs`, `trembita/tests/two_phase.rs` |
| 2026-08 | Product showcases (4 standalone examples + cluster scripts) | `examples/{background-jobs,stateful-workers,realtime,workflows}/`, `dev/cluster-common.sh`, `./scripts/check-examples.sh` |
| 2026-08 | Reference KV in `trembita_core::kv` (single source, re-exported by facade) | `trembita-core/src/kv.rs`, `trembita-test-support` `TrackedKv` only |
| 2026-08 | Durable cross-shard 2PC (`durable_cross_shard_2pc`, `EntryPayload::TwoPhasePrepare/Abort`) | `trembita-proto/src/two_phase.rs`, `trembita-core/tests/two_phase_journal.rs`, `trembita/tests/two_phase.rs` |
| 2026-08 | Dynamic catalog runtime (`add_raft_groups`, `/cluster/catalog/add`) | `trembita-proto/catalog`, `trembita-actor/runtime`, `trembita/tests/multi_raft.rs`, `docs/decisions/multi-raft.md` |
| 2026-08 | Multi-Raft architecture ADR + pure planners | `trembita-core/src/shard.rs`, `docs/decisions/multi-raft.md` |
| 2026-08 | Actor routing: ring, session, drain override, `ask_linearizable`, directory RYW | `trembita-actor` (`ring`, `session`, `directory_policy`), `trembita-actor/tests/{messaging,migration}.rs`, `docs/decisions/actor-routing.md` |
| 2026-08 | `trembita-node` env parsing unit tests | `trembita-node/src/config.rs` (`#[cfg(test)]`) |
| 2026-08 | Actor store resume + idempotency after unreachable node | `trembita/tests/actor_store_resume.rs` |
| 2026-08 | Redis dual connection, idempotent worker, reconnect | `trembita-store-redis/tests/redis.rs` |
| 2026-08 | Redis TLS (`rediss://`) with private CA | `trembita-store-redis/tests/tls.rs` |
| 2026-08 | Facade `data_dir` stop → restart → state | `trembita/tests/persistence.rs` |
| 2026-07 | Pure Raft FSM + property sim | `trembita-core`, `trembita-sim` |
| 2026-07 | Store contract Memory ≡ Redb + reopen | `trembita-storage/tests/storage.rs` |
| 2026-07 | Driver-level restart + replay | `trembita-actor/tests/driver.rs` |
| 2026-07 | E2E election + failover + chaos | `e2e/` |
| 2026-08 | E2E PEM hot reload (SIGHUP + poll) | `e2e/cert_renew.sh` |
| 2026-07 | Macro compile-fail suite | `trembita-actor/tests/compile_fail.rs` |
| 2026-07 | Loopback QUIC + mTLS integration | `trembita-net/tests/quic.rs`, `trembita/tests/quic.rs` |

---

## Testability patterns (for contributors)

When adding features, prefer these hooks — they are already used across the codebase:

| Port | Trait / type | Production | Test double |
|------|--------------|------------|-------------|
| Storage | `RaftStorage` | `RedbStorage` | `MemoryStorage`, `NullStorage` |
| Transport | `Transport` | `QuicTransport` | `LocalNetwork` |
| Actor workflow state | `ActorStateStore` | Redis | `InMemoryStore` |
| Admin views | `Observer` | `TrembitaObserver` | `Fake` (dashboard tests) |
| Consensus | `RaftNode` (pure) | — | Direct tick/deliver in tests |

**Regression rule (testing-strategy):** every fixed timing/partition bug gets a test at
the lowest layer that reproduces it — usually a seeded `trembita-sim` case.

---

## Maintenance

1. After adding or removing tests, refresh the **Total** column in [Per-crate inventory](#per-crate-inventory).
2. When closing a gap, update [Known gaps](#known-gaps-prioritized) → [Closed gaps](#closed-gaps).
3. Keep [testing-strategy](decisions/testing-strategy.md) as the *strategy*; this file is the *inventory*.
4. Optional: per-crate counts via `cargo test -p <crate> --lib --tests -- --list | rg ': test$' | wc -l`.
