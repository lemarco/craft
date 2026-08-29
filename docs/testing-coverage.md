# Testing coverage matrix

Living inventory of what the crafty test suite covers, where gaps remain, and
which CI lane exercises each layer. Update this file when adding tests or
closing a gap.

**Strategy:** [testing-strategy](decisions/testing-strategy.md)  
**Feature status:** [status.md](status.md)  
**Last audit:** 2026-08-28

Legend: **✅** covered · **⚠️** partial · **❌** missing · **🔒** scheduled / `#[ignore]` only

---

## Test pyramid (current)

| Layer | Scope | Location | Count (approx.) | Fast CI |
|-------|-------|----------|-----------------|---------|
| Unit | Pure functions, small modules | `#[cfg(test)]` in `src/` | ~70 | ✅ |
| Integration | Crate boundaries, async runtime | `crates/*/tests/` | ~260 | ✅ |
| Property | Raft safety under fault schedules | `crafty-sim/tests/safety.rs` (+ proptest) | 250+ seeds | ✅ |
| Compile-fail | Macro misuse → good errors | `crafty-actor/tests/compile_fail.rs` | 3+ | ✅ |
| Deterministic sim | Whole cluster, virtual clock | `crafty-sim` harness + scenarios | 27 tests | ✅ |
| Linearizability | Client-visible histories | `crafty-sim/tests/linearizability.rs` | 2 | ✅ |
| Doctests | Public API examples | `cargo test --doc` | — | ✅ |
| Redis integration | Real `ActorStateStore` | `crafty-store-redis/tests/{redis,tls}.rs` | 10 | 🔒 nightly |
| E2E | Real processes, QUIC, mTLS, chaos | `e2e/run.sh`, `e2e/leave.sh`, `e2e/queue.sh`, `e2e/chaos.sh`, `e2e/cert_renew.sh`, `e2e/linearizability.sh` | 6 scenarios | 🔒 nightly |
| Fuzz | Wire decode never panics | `crafty-fuzz` | 1 target | 🔒 nightly |
| Bench / soak | Throughput, long-run sim | `benchmarks/` | — | 🔒 nightly |

---

## Per-crate inventory

| Crate | Unit (`src/`) | Integration (`tests/`) | Total | Primary focus |
|-------|:-------------:|:----------------------:|:-----:|---------------|
| `crafty-core` | 30 | 81 | **111** | Pure Raft FSM: election, replication, membership, snapshots, ReadIndex |
| `crafty-actor` | 10 | 99 | **109** | `RaftDriver`, runtime, registry, placement, supervision, migration, trybuild |
| `crafty-net` | 12 | 32 | **44** | Wire framing, `LocalNetwork`, TLS handshake, loopback QUIC, protocol compat |
| `crafty-sim` | 8 | 22 | **30** | Safety/liveness under faults, linearizability, actor scenarios, multi-Raft |
| `crafty-dashboard` | 8 | **8** | **16** | Admin HTTP, admin TLS, metrics, telemetry |
| `crafty` (facade) | 2 | **24** | **26** | `CraftyCluster` builder, multi-Raft, keyed client, live QUIC cluster, reachability reconcile, actor store resume, graceful leave, admin TLS, DNS discovery |
| `crafty-storage` | 0 | 7 | **7** | Store contract (Memory + Redb), namespaced groups, reopen |
| `crafty-proto` | 7 | 0 | **7** | Encode/decode roundtrips, protocol compat band |
| `crafty-store-redis` | 0 | 10 (7 `redis` + 3 `tls`, `#[ignore]` except 2 fast) | **10** | Redis CAS/TTL, dual conn, idempotent worker, reconnect, `rediss://` |
| `crafty-client` | 1 | **8** | **9** | Remote client propose/query, follower forward, failover, retry policy, keyed batch |
| `crafty-ops` | 0 | 2 | **2** | Snapshot export/import, object-store push/pull |
| `crafty-macros` | — | via trybuild in `crafty-actor` | — | Compile-pass/fail |
| `crafty-node` | **10** | 0 | **0** | *(E2E smoke only)* |

Count tests locally:

```sh
cargo test --workspace --all-features --lib --tests -- --list | rg ': test$' | wc -l
```

---

## Coverage by functional area

### Consensus (`crafty-core` + driver)

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
| Tier 2 catalog expansion (pure planner) | ✅ `shard` | — | — | — | ✅ |
| Tier 2 stable virtual shards (pure planner) | ✅ `shard` | — | — | — | ✅ |
| Dynamic catalog expansion (runtime) | — | ✅ `multi_raft` | — | — | ✅ |
| Stable shard routing (runtime) | — | ✅ `multi_raft`, `sharded` | — | — | ✅ |
| Cross-shard atomic transactions | — | ✅ `saga`, `two_phase`, `crafty-client` | ✅ `two_phase_journal`, `two_phase_client_journal` | ✅ `crafty-sim/tests/two_phase` | ✅ |
| Durable 2PC log entries (`EntryPayload::TwoPhasePrepare/Abort`) | ✅ `two_phase_journal` | ✅ `driver`, `runtime`, `two_phase` | — | ✅ `crafty-sim/tests/two_phase` | ✅ |
| 2PC client journal Meta-Raft (`EntryPayload::TwoPhaseJournal`) | ✅ `two_phase_client_journal` | ✅ `driver`, `runtime`, `two_phase` | — | — | ✅ |
| Saga journal Meta-Raft metadata (`EntryPayload::SagaJournal`) | ✅ `saga_journal` | ✅ `driver`, `runtime`, `saga` | — | — | ✅ |
| Meta-Raft coordinator (multi-Raft join/catalog/saga isolation) | ✅ `shard` | ✅ `sharded` | — | — | ✅ |
| Per-group membership planner (`group_voters`, join/leave affects) | ✅ | ✅ | — | — | ✅ |
| Per-group membership runtime sync on cluster join | — | ✅ | — | — | ✅ |
| Per-group learners (`group_learners`, membership sync, rebalance hosting) | ✅ | ✅ `group_rebalance` | ✅ `learners` | — | ✅ |
| Operator shard expansion (`expand_shard_count`, Tier 1 modulus) | ✅ | ✅ `multi_raft` | — | — | ✅ |
| Stable shard activation (`activate_shards`, Tier 2) | ✅ | ✅ `multi_raft`, `sharded` | — | — | ✅ |
| Cluster leave RPC (`/cluster/leave`, `CraftyCluster::leave`) | — | ✅ `runtime`, `multi_raft` | — | — | ✅ |
| Leader-side reachability / hysteresis / phi-accrual | ✅ | ✅ | — | — | ✅ |
| Wire protocol N/N−1 compat band | ✅ | ✅ | — | — | ✅ |
| Admin HTTPS (server TLS) | ✅ | ✅ `admin`, `facade` | — | 🔒 nightly | ✅ |
| Snapshot backup CLI (`crafty-ops`) | — | ✅ | — | — | ✅ |
| External linearizability (Jepsen-lite) | — | — | ✅ | ✅ `linearizability.sh` | ✅ |
| Malformed persistence payloads | — | ✅ driver | — | — | ✅ |

| Area | Unit | Integration | Sim | E2E | Status |
|------|:----:|:-----------:|:---:|:---:|--------|
| Store contract (Memory ≡ Redb) | — | ✅ | — | — | ✅ |
| Redb reopen after "crash" | — | ✅ | — | — | ✅ |
| Namespaced multi-group layout | — | ✅ | — | — | ✅ |
| `RaftDriver` restart + replay | — | ✅ driver | — | — | ✅ |
| **`CraftyCluster` + `data_dir` restart** | — | ✅ `persistence` | — | — | ✅ |
| Snapshot survives facade restart | — | ✅ `persistence` | — | — | ✅ |
| Backend error injection | — | ✅ driver | — | — | ✅ |

### Transport (`crafty-net`)

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

### Actor runtime (`crafty-actor`)

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
| **Job queue (`JobQueue`, in-memory + redb)** | ✅ `queue`, `redb_queue` | ✅ `queue` | — | — | ✅ |
| **Job queue wire + cluster client** | ✅ `queue_service` | ✅ `queue` (facade, autoscale) | — | — | ✅ |
| **Job queue voter replication (sync `/queue/replicate`)** | ✅ `redb_queue` (`apply_replicate`) | ✅ `queue` (all voters redb, follower lease after shutdown) | — | — | ✅ |
| **Job queue sharded streams + priority/delayed + membership autoscale** | ✅ `sharded_queue`, `queue` | ✅ `queue` | — | — | ✅ |
| **Job queue replicate auth + parallel replicate** | ✅ `queue_service` | ✅ `queue` (`queue_replicate_rejects_non_leader_caller`) | — | — | ✅ |
| **Meta-Raft queue autoscale policy** | ✅ `queue_autoscale_policy` (core) | ✅ `queue` (autoscale tests) | — | — | ✅ |
| **RedbJobQueue ack-driven compaction** | ✅ `redb_queue` | ✅ `queue` | — | — | ✅ |
| **Queue throughput (batch enqueue/ack + leader prefetch)** | ✅ `redb_queue`, `queue_prefetch` | ✅ `queue`, `queue_throughput` | — | — | ✅ |
| **Job queue E2E (QUIC enqueue → follower lease/ack → leader failover)** | — | — | — | ✅ `e2e/queue.sh` | ✅ |
| **Durable mailbox outbox/inbox** | ✅ `mailbox_spool` | ✅ `mailbox_spool` (wire) | — | — | ✅ |
| Actor state store resume + idempotency (facade) | — | ✅ `actor_store_resume` | — | — | ✅ |
| Actor state store (Redis) | — | 🔒 ignore | — | — | 🔒 |
| Actor state store (`rediss://` + private CA) | — | 🔒 ignore | — | — | 🔒 |

### Client (`crafty-client`)

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
| `CraftyCluster` local 3-node | — | ✅ | — | — | ✅ |
| Admin / observability HTTP | — | ✅ | — | ✅ `run.sh` | ✅ |
| Multi-Raft introspection (`/introspect/raft-groups`) | — | ✅ `admin`, `multi_raft` | — | — | ✅ |
| Actors + auto-spawn | — | ✅ | ✅ `auto_spawn` | — | ✅ |
| Multi-Raft file layout (`data_dir`) | — | ✅ `persistence` + `multi_raft` | — | — | ✅ |
| DNS discovery | ✅ | ✅ `discovery` | — | ✅ | ✅ |
| **`crafty-node` env parsing** | ✅ `config` | — | — | ✅ implicit | ✅ |
| **`crafty-node` drain timeout (`CRAFTY_DRAIN_TIMEOUT`)** | ✅ `config` | — | — | ✅ implicit | ✅ |
| **`crafty-node` graceful leave on shutdown** | ✅ `config` | ✅ `graceful_leave` | — | ✅ `leave.sh` | ✅ |

### Macros & wire

| Area | Unit | Integration | Sim | E2E | Status |
|------|:----:|:-----------:|:---:|:---:|--------|
| `StateMachine` / `remote_actor` derive | — | ✅ trybuild | — | — | ✅ |
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
| `linearizability-sim` | Scheduled | crafty-sim linearizability + read_index seed sweep (`e2e/linearizability.sh`) |
| `store-redis` | Scheduled | `cargo test -p crafty-store-redis -- --ignored` |
| `bench` | Scheduled | criterion (`append`/`apply`/`deliver`/`queue`) + 120s `soak` + 60s `soak_multi_raft` + 60s `soak_queue` + 60s `soak_actor_store` + 60s `soak_saga` + 60s `soak_session` |
| `fuzz` | Scheduled | `cargo-fuzz` wire_decode in `crates/crafty-fuzz` |

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
| 2026-08 | Transport + facade gaps: QUIC backoff, DNS discovery, queue compaction, auto-spawn sim, admin/leave E2E | `crafty-net/tests/quic.rs`, `crafty/tests/{discovery,queue}.rs`, `crafty-sim/tests/{auto_spawn,actor_scenarios}.rs`, `e2e/{run,leave}.sh` |
| 2026-08 | Runtime fatal-error observable path (`status()` → `None`, `Stopped`) | `crafty-actor/tests/runtime.rs` |
| 2026-08 | Multi-Raft sim: shard routing + independent group safety | `crafty-sim/tests/multi_raft.rs` |
| 2026-08 | Group rebalance planner + sharded adopt/retire runtime | `crafty-actor/tests/group_rebalance.rs`, `sharded.rs` |
| 2026-08 | Malformed persistence + backend error injection at driver | `crafty-actor/tests/driver.rs` |
| 2026-08 | Cluster leave RPC + `CraftyCluster::leave()` facade | `crafty-actor/tests/runtime.rs`, `crafty/tests/multi_raft.rs`, `docs/decisions/cluster-membership.md#leave-rpc` |
| 2026-08 | Injectable Tokio clock in integration tests (`crafty-test-support::clock`, `start_paused`) | `crafty-test-support`, `crafty/tests/*`, `crafty-actor/tests/*`, `crafty-client/tests/cluster.rs` |
| 2026-08 | Snapshot survives facade restart (`compact` + `data_dir`) | `crafty/tests/persistence.rs` |
| 2026-08 | 3-node majority survives one member restart | `crafty/tests/persistence.rs` |
| 2026-08 | Shared KV fixtures + harness helpers (dedupe ~8 copies) | `crafty-test-support` (`Kv`, `TrackedKv`, `find_keys_for_two_groups`, cluster polling) |
| 2026-08 | Stable shard router runtime (`StableShardRouter`, `activate_shards`, builder default) | `crafty-actor/sharded`, `crafty/tests/multi_raft.rs`, `crafty-actor/tests/sharded.rs` |
| 2026-08 | Linearizability E2E phase 2 (QUIC `crafty-e2e-client` + external checker) | `crates/crafty-e2e-client`, `e2e/linearizability.sh`, `e2e/docker-compose.yml` |
| 2026-08 | Hardening: graceful leave integration, admin HTTPS E2E | `crafty/tests/graceful_leave.rs`, `crafty/tests/facade.rs`, `crafty-dashboard/tests/admin.rs` |
| 2026-08 | Wire decode fuzz (`cargo-fuzz` wire_decode, scheduled CI) | `crates/crafty-fuzz/`, `.gitlab-ci.yml` `fuzz` job |
| 2026-08 | Tier 1 multi-Raft: learners planner, shard expansion, keyed batch, `/introspect/raft-groups` | `crafty-core`, `crafty-client`, `crafty-dashboard`, `crafty/tests/multi_raft.rs`, `docs/decisions/multi-raft.md#tier-1-advances-landed` |
| 2026-08 | Client retry edge cases (`NoTargets`, timeout, `NotLeader`, unreachable) | `crafty-client/tests/retry.rs` |
| 2026-08 | Keyed client routing (multi-Raft propose/query) | `crafty/tests/client_keyed.rs` |
| 2026-08 | Tier 2 tails: `catalog_version`, `switch_to_stable_shards`, saga hardening | `crafty-core/shard.rs`, `crafty/src/cluster.rs`, `crafty-client/src/saga.rs`, `crafty/tests/{multi_raft,saga}.rs` |
| 2026-08 | Tier 2 Phase 4: cross-shard saga coordinator (`run_saga`, `StoreSagaJournal`) | `crafty-client/src/saga.rs`, `crafty/src/saga.rs`, `crafty/tests/saga.rs`, `docs/decisions/multi-raft.md#cross-shard-transactions` |
| 2026-08 | Saga hardening v2: group 0 journal fallback + coordinator restart resume | `crafty-proto/saga_journal`, `crafty-core/node`, `crafty-actor/runtime`, `crafty/src/saga.rs`, `crafty/tests/saga.rs` |
| 2026-08 | Saga journal layered tests (proto/core/driver/runtime/facade) | `crafty-proto`, `crafty-core/tests/saga_journal.rs`, `crafty-actor/tests/{driver,runtime}.rs`, `crafty/tests/saga.rs` |
| 2026-08 | Durable 2PC prepare timeout GC + `resume_cross_shard_2pc` | `crafty-actor/runtime`, `crafty-client/two_phase`, `crafty/tests/two_phase.rs` |
| 2026-08 | 2PC facade + client journal + metrics + sim partition test | `crafty/src/two_phase.rs`, `crafty-proto/two_phase_journal`, `crafty-sim/tests/two_phase.rs`, `examples/cross_shard_2pc.rs` |
| 2026-08 | Durable cross-shard 2PC (`durable_cross_shard_2pc`, `EntryPayload::TwoPhasePrepare/Abort`) | `crafty-proto/src/two_phase.rs`, `crafty-core/tests/two_phase_journal.rs`, `crafty/tests/two_phase.rs` |
| 2026-08 | Tier 2 Phase 2: dynamic catalog runtime (`add_raft_groups`, `/cluster/catalog/add`) | `crafty-proto/catalog`, `crafty-actor/runtime`, `crafty/tests/multi_raft.rs`, `docs/decisions/multi-raft.md` |
| 2026-08 | Tier 2 multi-Raft architecture ADR + Phase 1 pure planners | `crafty-core/src/shard.rs`, `docs/decisions/multi-raft.md` |
| 2026-08 | Actor routing Tier 3: ring, session, drain override, `ask_linearizable`, directory RYW | `crafty-actor` (`ring`, `session`, `directory_policy`), `crafty-actor/tests/{messaging,migration}.rs`, `docs/decisions/actor-routing-tier3.md` |
| 2026-08 | `crafty-node` env parsing unit tests | `crafty-node/src/config.rs` (`#[cfg(test)]`) |
| 2026-08 | Actor store resume + idempotency after unreachable node | `crafty/tests/actor_store_resume.rs` |
| 2026-08 | Redis dual connection, idempotent worker, reconnect | `crafty-store-redis/tests/redis.rs` |
| 2026-08 | Redis TLS (`rediss://`) with private CA | `crafty-store-redis/tests/tls.rs` |
| 2026-08 | Facade `data_dir` stop → restart → state | `crafty/tests/persistence.rs` |
| 2026-07 | Pure Raft FSM + property sim | `crafty-core`, `crafty-sim` |
| 2026-07 | Store contract Memory ≡ Redb + reopen | `crafty-storage/tests/storage.rs` |
| 2026-07 | Driver-level restart + replay | `crafty-actor/tests/driver.rs` |
| 2026-07 | E2E election + failover + chaos | `e2e/` |
| 2026-08 | E2E PEM hot reload (SIGHUP + poll) | `e2e/cert_renew.sh` |
| 2026-07 | Macro compile-fail suite | `crafty-actor/tests/compile_fail.rs` |
| 2026-07 | Loopback QUIC + mTLS integration | `crafty-net/tests/quic.rs`, `crafty/tests/quic.rs` |

---

## Testability patterns (for contributors)

When adding features, prefer these hooks — they are already used across the codebase:

| Port | Trait / type | Production | Test double |
|------|--------------|------------|-------------|
| Storage | `RaftStorage` | `RedbStorage` | `MemoryStorage`, `NullStorage` |
| Transport | `Transport` | `QuicTransport` | `LocalNetwork` |
| Actor workflow state | `ActorStateStore` | Redis | `InMemoryStore` |
| Admin views | `Observer` | `CraftyObserver` | `Fake` (dashboard tests) |
| Consensus | `RaftNode` (pure) | — | Direct tick/deliver in tests |

**Regression rule (testing-strategy):** every fixed timing/partition bug gets a test at
the lowest layer that reproduces it — usually a seeded `crafty-sim` case.

---

## Maintenance

1. After adding or removing tests, refresh the **Total** column in [Per-crate inventory](#per-crate-inventory).
2. When closing a gap, update [Known gaps](#known-gaps-prioritized) → [Closed gaps](#closed-gaps).
3. Keep [testing-strategy](decisions/testing-strategy.md) as the *strategy*; this file is the *inventory*.
4. Optional: per-crate counts via `cargo test -p <crate> --lib --tests -- --list | rg ': test$' | wc -l`.
