# Testing coverage matrix

Living inventory of what the craft test suite covers, where gaps remain, and
which CI lane exercises each layer. Update this file when adding tests or
closing a gap.

**Strategy (why we test this way):** [testing-strategy — Testing strategy](decisions/testing-strategy.md)  
**Implementation status:** [backlog.md — Track T](backlog.md)  
**Last audit:** 2026-08-27 · **~337** test functions (`cargo test --workspace --lib --tests --all-features`)

Legend: **✅** covered · **⚠️** partial · **❌** missing · **🔒** scheduled / `#[ignore]` only

---

## Test pyramid (current)

| Layer | Scope | Location | Count (approx.) | Fast CI |
|-------|-------|----------|-----------------|---------|
| Unit | Pure functions, small modules | `#[cfg(test)]` in `src/` | ~70 | ✅ |
| Integration | Crate boundaries, async runtime | `crates/*/tests/` | ~260 | ✅ |
| Property | Raft safety under fault schedules | `craft-sim/tests/safety.rs` (+ proptest) | 250+ seeds | ✅ |
| Compile-fail | Macro misuse → good errors | `craft-actor/tests/compile_fail.rs` | 3+ | ✅ |
| Deterministic sim | Whole cluster, virtual clock | `craft-sim` harness + scenarios | 24 tests | ✅ |
| Linearizability | Client-visible histories | `craft-sim/tests/linearizability.rs` | 2 | ✅ |
| Doctests | Public API examples | `cargo test --doc` | — | ✅ |
| Redis integration | Real `ActorStateStore` | `craft-store-redis/tests/{redis,tls}.rs` | 10 | 🔒 nightly |
| E2E | Real processes, QUIC, mTLS, chaos | `e2e/run.sh`, `e2e/chaos.sh`, `e2e/cert_renew.sh`, `e2e/linearizability.sh` | 4 scenarios | 🔒 nightly |
| Fuzz | Wire decode never panics | `craft-fuzz` | 1 target | 🔒 nightly |
| Bench / soak | Throughput, long-run sim | `benchmarks/` | — | 🔒 nightly |

---

## Per-crate inventory

| Crate | Unit (`src/`) | Integration (`tests/`) | Total | Primary focus |
|-------|:-------------:|:----------------------:|:-----:|---------------|
| `craft-core` | 30 | 81 | **111** | Pure Raft FSM: election, replication, membership, snapshots, ReadIndex |
| `craft-actor` | 10 | 99 | **109** | `RaftDriver`, runtime, registry, placement, supervision, migration, trybuild |
| `craft-net` | 11 | 31 | **42** | Wire framing, `LocalNetwork`, TLS handshake, loopback QUIC, protocol compat |
| `craft-sim` | 8 | 19 | **27** | Safety/liveness under faults, linearizability, actor scenarios, multi-Raft |
| `craft-dashboard` | 8 | **8** | **16** | Admin HTTP, admin TLS, metrics, telemetry |
| `craft` (facade) | 2 | **21** | **23** | `CraftCluster` builder, multi-Raft, keyed client, live QUIC cluster, reachability reconcile, actor store resume, graceful leave, admin TLS |
| `craft-storage` | 0 | 7 | **7** | Store contract (Memory + Redb), namespaced groups, reopen |
| `craft-proto` | 7 | 0 | **7** | Encode/decode roundtrips, protocol compat band |
| `craft-store-redis` | 0 | 10 (7 `redis` + 3 `tls`, `#[ignore]` except 2 fast) | **10** | Redis CAS/TTL, dual conn, idempotent worker, reconnect, `rediss://` |
| `craft-client` | 1 | **8** | **9** | Remote client propose/query, follower forward, failover, retry policy, keyed batch |
| `craft-ops` | 0 | 2 | **2** | Snapshot export/import, object-store push/pull |
| `craft-macros` | — | via trybuild in `craft-actor` | — | Compile-pass/fail |
| `craft-node` | **10** | 0 | **0** | *(E2E smoke only)* |

Count tests locally:

```sh
cargo test --workspace --all-features --lib --tests -- --list | rg ': test$' | wc -l
```

---

## Coverage by functional area

### Consensus (`craft-core` + driver)

| Area | Unit | Integration | Sim | E2E | Status |
|------|:----:|:-----------:|:---:|:---:|--------|
| Leader election (Pre-Vote) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Log replication / conflict truncate | ✅ | ✅ | ✅ | — | ✅ |
| Joint-consensus membership | ✅ | ✅ | ✅ | — | ✅ |
| ReadIndex linearizable reads | ✅ | ✅ | ✅ | — | ✅ |
| Follower reads (ReadIndexConfirm + local query) | ✅ | ✅ | — | — | ✅ |
| Lease reads | ✅ | ✅ | — | — | ✅ |
| Snapshots + log compaction | ✅ | ✅ | ✅ | — | ✅ |
| `take_persist` / `restore` (core) | ✅ | ✅ | — | — | ✅ |
| Write sharding / multi-Raft routing | ⚠️ | ✅ `multi_raft` | ✅ | — | ✅ |
| Tier 2 catalog expansion (pure planner) | ✅ `shard` | — | — | — | ⚠️ |
| Tier 2 stable virtual shards (pure planner) | ✅ `shard` | — | — | — | ✅ |
| Dynamic catalog expansion (runtime) | — | ✅ `multi_raft` | — | — | ✅ |
| Stable shard routing (runtime) | — | ✅ `multi_raft`, `sharded` | — | — | ✅ |
| Cross-shard atomic transactions | — | ✅ `saga`, `craft-client` | — | — | ✅ |
| Per-group membership planner (`group_voters`, join/leave affects) | ✅ | ✅ | — | — | ✅ |
| Per-group membership runtime sync on cluster join | — | ✅ | — | — | ✅ |
| Per-group learners (`group_learners`, membership sync) | ✅ | — | — | — | ✅ |
| Operator shard expansion (`expand_shard_count`, Tier 1 modulus) | ✅ | ✅ `multi_raft` | — | — | ✅ |
| Stable shard activation (`activate_shards`, Tier 2) | ✅ | ✅ `multi_raft`, `sharded` | — | — | — |
| Cluster leave RPC (`/cluster/leave`, `CraftCluster::leave`) | — | ✅ `runtime`, `multi_raft` | — | — | ✅ |
| Leader-side reachability / hysteresis / phi-accrual | ✅ | ✅ | — | — | ✅ |
| Wire protocol N/N−1 compat band | ✅ | ✅ | — | — | ✅ |
| Admin HTTPS (server TLS) | ✅ | ✅ `admin`, `facade` | — | 🔒 nightly | ✅ |
| Snapshot backup CLI (`craft-ops`) | — | ✅ | — | — | ✅ |
| External linearizability (Jepsen-lite) | — | — | ✅ | ✅ `linearizability.sh` | ✅ |
| Malformed persistence payloads | — | ✅ driver | — | — | ✅ | (`craft-storage` + B4)

| Area | Unit | Integration | Sim | E2E | Status |
|------|:----:|:-----------:|:---:|:---:|--------|
| Store contract (Memory ≡ Redb) | — | ✅ | — | — | ✅ |
| Redb reopen after "crash" | — | ✅ | — | — | ✅ |
| Namespaced multi-group layout | — | ✅ | — | — | ✅ |
| `RaftDriver` restart + replay | — | ✅ driver | — | — | ✅ |
| **`CraftCluster` + `data_dir` restart** | — | ✅ `persistence` | — | — | ✅ |
| Snapshot survives facade restart | — | ✅ `persistence` | — | — | ✅ |
| Backend error injection | — | ✅ driver | — | — | ✅ |

### Transport (`craft-net`)

| Area | Unit | Integration | Sim | E2E | Status |
|------|:----:|:-----------:|:---:|:---:|--------|
| Wire encode/decode + size guard | — | ✅ (16) | — | — | ✅ |
| `LocalNetwork` (in-process) | — | ✅ | ✅ | — | ✅ |
| mTLS mutual auth (loopback) | — | ✅ | — | ✅ | ✅ |
| PEM hot reload (loopback QUIC) | — | ✅ `cert_reload` | — | ✅ | — |
| PEM hot reload (docker-compose, SIGHUP + poll) | — | — | — | ✅ `cert_renew` | — |
| Live HTTP/3 QUIC (loopback) | ⚠️ | ✅ `dev-certs` | — | ✅ | ⚠️ |
| Connection pool + backoff | ✅ | ⚠️ | — | — | ⚠️ |
| Partition / drop injection (net layer) | — | ⚠️ detach | ✅ sim | ✅ chaos | ✅ |

### Actor runtime (`craft-actor`)

| Area | Unit | Integration | Sim | E2E | Status |
|------|:----:|:-----------:|:---:|:---:|--------|
| Registry / messaging | ✅ | ✅ | — | — | ✅ |
| Keyed routing (consistent hash ring) | ✅ `ring` | ✅ `messaging`, `directory` | — | — | ✅ |
| Sticky session / actor lease | ✅ `session` | ✅ `messaging` | — | — | ✅ |
| `ask_linearizable` (directory retry) | — | ✅ `messaging` | — | — | ✅ |
| Directory `ReadYourWrites` policy | ✅ `directory_policy` | ✅ `messaging` | — | — | ✅ |
| Per-group drain timeout override | — | ✅ `migration` | — | — | ✅ |
| Placement / supervisor | — | ✅ | ⚠️ | — | ✅ |
| Crash-driven auto-respawn (reachable ≠ membership) | — | ✅ | ✅ | — | ✅ |
| Cross-node spawn / migration | — | ✅ | ⚠️ | — | ✅ |
| Group rebalance / sharded runtime | — | ✅ | — | — | ✅ |
| Raft group migration bundle + respawn | ✅ storage | ✅ `group_migrate` | — | — | ✅ |
| Group migrate RPC (facade wire) | — | ✅ `multi_raft` | — | — | ✅ |
| Runtime fatal-error path | — | ✅ `runtime` | — | — | ✅ |
| Actor state store (in-memory) | ✅ | ✅ | — | — | ✅ |
| Actor state store resume + idempotency (facade) | — | ✅ `actor_store_resume` | — | — | ✅ |
| Actor state store (Redis) | — | 🔒 ignore | — | — | 🔒 |
| Actor state store (`rediss://` + private CA) | — | 🔒 ignore | — | — | 🔒 |

### Client (`craft-client`)

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
| `CraftCluster` local 3-node | — | ✅ | — | — | ✅ |
| Admin / observability HTTP | — | ✅ | — | ⚠️ | ✅ |
| Multi-Raft introspection (`/introspect/raft-groups`) | — | ✅ `admin`, `multi_raft` | — | — | ✅ |
| Actors + auto-spawn | — | ✅ | ⚠️ | — | ✅ |
| Multi-Raft file layout (`data_dir`) | — | ✅ `persistence` + `multi_raft` | — | — | ✅ |
| DNS discovery | ⚠️ 2 unit | ❌ | — | ⚠️ K8s | ⚠️ |
| **`craft-node` env parsing** | ✅ `config` | — | — | ✅ implicit | ✅ |
| **`craft-node` drain timeout (`CRAFT_DRAIN_TIMEOUT`)** | ✅ `config` | — | — | ✅ implicit | ✅ |
| **`craft-node` graceful leave on shutdown** | ✅ `config` | ✅ `graceful_leave` | — | ⚠️ manual | ✅ |

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
| `msrv` | Every MR / push | `cargo check` on Rust 1.98 |
| `e2e` | Scheduled | `e2e/run.sh` + `e2e/chaos.sh` + `e2e/cert_renew.sh` + docker phase of `e2e/linearizability.sh` |
| `linearizability-sim` | Scheduled | craft-sim linearizability + read_index seed sweep (`e2e/linearizability.sh`) |
| `store-redis` | Scheduled | `cargo test -p craft-store-redis -- --ignored` |
| `bench` | Scheduled | criterion + 120s soak + 60s `soak_multi_raft` |
| `fuzz` | Scheduled | `cargo-fuzz` wire_decode in `crates/craft-fuzz` |

Local hooks mirror the fast lane: `lefthook` pre-commit (fmt, clippy, check) and
pre-push (check → tests → doctests → release build). See `scripts/quality-gate-*.sh`.

---

## Known gaps (prioritized)

Track open gaps here; move rows to **Closed gaps** when fixed.

| Priority | Gap | Suggested test location | Effort |
|----------|-----|-------------------------|--------|

### Closed gaps

| Closed | What | Where |
|--------|------|-------|
| 2026-08 | Runtime fatal-error observable path (`status()` → `None`, `Stopped`) | `craft-actor/tests/runtime.rs` |
| 2026-08 | Multi-Raft sim: shard routing + independent group safety | `craft-sim/tests/multi_raft.rs` |
| 2026-08 | Group rebalance planner + sharded adopt/retire runtime | `craft-actor/tests/group_rebalance.rs`, `sharded.rs` |
| 2026-08 | Malformed persistence + backend error injection at driver | `craft-actor/tests/driver.rs` |
| 2026-08 | Cluster leave RPC + `CraftCluster::leave()` facade | `craft-actor/tests/runtime.rs`, `craft/tests/multi_raft.rs`, `docs/decisions/leave-rpc.md` |
| 2026-08 | Injectable Tokio clock in integration tests (`craft-test-support::clock`, `start_paused`) | `craft-test-support`, `craft/tests/*`, `craft-actor/tests/*`, `craft-client/tests/cluster.rs` |
| 2026-08 | Snapshot survives facade restart (`compact` + `data_dir`) | `craft/tests/persistence.rs` |
| 2026-08 | 3-node majority survives one member restart | `craft/tests/persistence.rs` |
| 2026-08 | Shared KV fixtures + harness helpers (dedupe ~8 copies) | `craft-test-support` (`Kv`, `TrackedKv`, `find_keys_for_two_groups`, cluster polling) |
| 2026-08 | Stable shard router runtime (`StableShardRouter`, `activate_shards`, builder default) | `craft-actor/sharded`, `craft/tests/multi_raft.rs`, `craft-actor/tests/sharded.rs` |
| 2026-08 | Linearizability E2E phase 2 (QUIC `craft-e2e-client` + external checker) | `crates/craft-e2e-client`, `e2e/linearizability.sh`, `e2e/docker-compose.yml` |
| 2026-08 | Hardening: graceful leave integration, admin HTTPS E2E | `craft/tests/graceful_leave.rs`, `craft/tests/facade.rs`, `craft-dashboard/tests/admin.rs` |
| 2026-08 | Wire decode fuzz (`cargo-fuzz` wire_decode, scheduled CI) | `crates/craft-fuzz/`, `.gitlab-ci.yml` `fuzz` job |
| 2026-08 | Tier 1 multi-Raft: learners planner, shard expansion, keyed batch, `/introspect/raft-groups` | `craft-core`, `craft-client`, `craft-dashboard`, `craft/tests/multi_raft.rs`, `docs/decisions/tier1-multi-raft-advances.md` |
| 2026-08 | Client retry edge cases (`NoTargets`, timeout, `NotLeader`, unreachable) | `craft-client/tests/retry.rs` |
| 2026-08 | Keyed client routing (multi-Raft propose/query) | `craft/tests/client_keyed.rs` |
| 2026-08 | Tier 2 Phase 4: cross-shard saga coordinator (`run_saga`, `StoreSagaJournal`) | `craft-client/src/saga.rs`, `craft/src/saga.rs`, `craft/tests/saga.rs`, `docs/decisions/cross-shard-transactions.md` |
| 2026-08 | Tier 2 Phase 2: dynamic catalog runtime (`add_raft_groups`, `/cluster/catalog/add`) | `craft-proto/catalog`, `craft-actor/runtime`, `craft/tests/multi_raft.rs`, `docs/decisions/tier2-multi-raft-architecture.md` |
| 2026-08 | Tier 2 multi-Raft architecture ADR + Phase 1 pure planners | `craft-core/src/shard.rs`, `docs/decisions/tier2-multi-raft-architecture.md` |
| 2026-08 | Actor routing Tier 3: ring, session, drain override, `ask_linearizable`, directory RYW | `craft-actor` (`ring`, `session`, `directory_policy`), `craft-actor/tests/{messaging,migration}.rs`, `docs/decisions/actor-routing-tier3.md` |
| 2026-08 | `craft-node` env parsing unit tests | `craft-node/src/config.rs` (`#[cfg(test)]`) |
| 2026-08 | Actor store resume + idempotency after unreachable node | `craft/tests/actor_store_resume.rs` |
| 2026-08 | Redis dual connection, idempotent worker, reconnect | `craft-store-redis/tests/redis.rs` |
| 2026-08 | Redis TLS (`rediss://`) with private CA | `craft-store-redis/tests/tls.rs` |
| 2026-08 | Facade `data_dir` stop → restart → state | `craft/tests/persistence.rs` |
| 2026-07 | Pure Raft FSM + property sim | `craft-core`, `craft-sim` |
| 2026-07 | Store contract Memory ≡ Redb + reopen | `craft-storage/tests/storage.rs` |
| 2026-07 | Driver-level restart + replay | `craft-actor/tests/driver.rs` |
| 2026-07 | E2E election + failover + chaos | `e2e/` |
| 2026-08 | E2E PEM hot reload (SIGHUP + poll) | `e2e/cert_renew.sh` |
| 2026-07 | Macro compile-fail suite | `craft-actor/tests/compile_fail.rs` |
| 2026-07 | Loopback QUIC + mTLS integration | `craft-net/tests/quic.rs`, `craft/tests/quic.rs` |

---

## Testability patterns (for contributors)

When adding features, prefer these hooks — they are already used across the codebase:

| Port | Trait / type | Production | Test double |
|------|--------------|------------|-------------|
| Storage | `RaftStorage` | `RedbStorage` | `MemoryStorage`, `NullStorage` |
| Transport | `Transport` | `QuicTransport` | `LocalNetwork` |
| Actor workflow state | `ActorStateStore` | Redis | `InMemoryStore` |
| Admin views | `Observer` | `CraftObserver` | `Fake` (dashboard tests) |
| Consensus | `RaftNode` (pure) | — | Direct tick/deliver in tests |

**Regression rule (testing-strategy):** every fixed timing/partition bug gets a test at
the lowest layer that reproduces it — usually a seeded `craft-sim` case.

---

## Maintenance

1. After adding or removing tests, refresh the **Total** column in [Per-crate inventory](#per-crate-inventory).
2. When closing a gap, update [Known gaps](#known-gaps-prioritized) → [Closed gaps](#closed-gaps).
3. Keep [testing-strategy](decisions/testing-strategy.md) as the *strategy*; this file is the *inventory*.
4. Optional: per-crate counts via `cargo test -p <crate> --lib --tests -- --list | rg ': test$' | wc -l`.
