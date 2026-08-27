# Testing coverage matrix

Living inventory of what the craft test suite covers, where gaps remain, and
which CI lane exercises each layer. Update this file when adding tests or
closing a gap.

**Strategy (why we test this way):** [ADR 029 — Testing strategy](decisions/029-testing-strategy.md)  
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
| E2E | Real processes, QUIC, mTLS, chaos | `e2e/run.sh`, `e2e/chaos.sh`, `e2e/cert_renew.sh` | 3 scenarios | 🔒 nightly |
| Fuzz | Wire decode never panics | `craft-fuzz` | 1 target | 🔒 nightly |
| Bench / soak | Throughput, long-run sim | `benchmarks/` | — | 🔒 nightly |

---

## Per-crate inventory

| Crate | Unit (`src/`) | Integration (`tests/`) | Total | Primary focus |
|-------|:-------------:|:----------------------:|:-----:|---------------|
| `craft-core` | 30 | 81 | **111** | Pure Raft FSM: election, replication, membership, snapshots, ReadIndex |
| `craft-actor` | 10 | 99 | **109** | `RaftDriver`, runtime, registry, placement, supervision, migration, trybuild |
| `craft-net` | 11 | 29 | **40** | Wire framing, `LocalNetwork`, TLS handshake, loopback QUIC |
| `craft-sim` | 7 | 17 | **24** | Safety/liveness under faults, linearizability, actor scenarios |
| `craft-dashboard` | 6 | 7 | **13** | Admin HTTP, metrics, telemetry |
| `craft` (facade) | 2 | 15 | **17** | `CraftCluster` builder, multi-Raft, live QUIC cluster, reachability reconcile, actor store resume |
| `craft-storage` | 0 | 7 | **7** | Store contract (Memory + Redb), namespaced groups, reopen |
| `craft-proto` | 6 | 0 | **6** | Encode/decode roundtrips |
| `craft-store-redis` | 0 | 10 (7 `redis` + 3 `tls`, `#[ignore]` except 2 fast) | **10** | Redis CAS/TTL, dual conn, idempotent worker, reconnect, `rediss://` |
| `craft-client` | 0 | 3 | **3** | Remote client propose/query, follower forward, failover |
| `craft-macros` | — | via trybuild in `craft-actor` | — | Compile-pass/fail |
| `craft-node` | 0 | 0 | **0** | *(E2E smoke only)* |

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
| Write sharding / multi-Raft routing | ⚠️ | ⚠️ | ❌ | — | ⚠️ |
| Per-group membership planner (`group_voters`, join/leave affects) | ✅ | ✅ | — | — | ✅ |
| Per-group membership runtime sync on cluster join | — | ✅ | — | — | ✅ |
| Malformed persistence payloads | ❌ | ❌ | — | — | ❌ |

### Persistence (`craft-storage` + B4)

| Area | Unit | Integration | Sim | E2E | Status |
|------|:----:|:-----------:|:---:|:---:|--------|
| Store contract (Memory ≡ Redb) | — | ✅ | — | — | ✅ |
| Redb reopen after "crash" | — | ✅ | — | — | ✅ |
| Namespaced multi-group layout | — | ✅ | — | — | ✅ |
| `RaftDriver` restart + replay | — | ✅ driver | — | — | ✅ |
| **`CraftCluster` + `data_dir` restart** | — | ✅ `persistence` | — | — | ✅ |
| Snapshot survives facade restart | — | ❌ | — | — | ❌ **gap** |
| Backend error injection | ❌ | ❌ | — | — | ❌ |

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
| Placement / supervisor | — | ✅ | ⚠️ | — | ✅ |
| Crash-driven auto-respawn (reachable ≠ membership) | — | ✅ | ✅ | — | ✅ |
| Cross-node spawn / migration | — | ✅ | ⚠️ | — | ✅ |
| Group rebalance / sharded runtime | — | ⚠️ (1) | — | — | ⚠️ |
| Raft group migration bundle + respawn | ✅ storage | ✅ `group_migrate` | — | — | ✅ |
| Group migrate RPC (facade wire) | — | ✅ `multi_raft` | — | — | ✅ |
| Runtime fatal-error path | — | ❌ | — | — | ❌ |
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
| `NoTargets` | — | ❌ | — | — | ❌ **gap** |
| `NotLeader` hint follow (explicit) | — | ❌ | — | — | ❌ **gap** |
| Max attempts / timeout exhaustion | — | ❌ | — | — | ❌ **gap** |
| Keyed propose/query (multi-Raft) | — | ❌ | — | — | ❌ **gap** |

### Facade & reference binary

| Area | Unit | Integration | Sim | E2E | Status |
|------|:----:|:-----------:|:---:|:---:|--------|
| `CraftCluster` local 3-node | — | ✅ | — | — | ✅ |
| Admin / observability HTTP | — | ✅ | — | ⚠️ | ✅ |
| Actors + auto-spawn | — | ✅ | ⚠️ | — | ✅ |
| Multi-Raft file layout (`data_dir`) | — | ✅ `persistence` + `multi_raft` | — | — | ✅ |
| DNS discovery | ⚠️ 2 unit | ❌ | — | ⚠️ K8s | ⚠️ |
| **`craft-node` env parsing** | — | ❌ | — | ✅ implicit | ❌ **gap** |

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
| `e2e` | Scheduled | `e2e/run.sh` + `e2e/chaos.sh` + `e2e/cert_renew.sh` (Docker, real QUIC + mTLS) |
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
| **P1** | Client retry edge cases (`NoTargets`, timeout, `NotLeader`) | `craft-client/tests/cluster.rs` | S |
| **P1** | Keyed client routing (multi-Raft) | `craft-client/tests/` or `craft/tests/` | M |
| **P1** | `craft-node` env parsing unit tests | extract `config.rs` + `#[cfg(test)]` | S |
| **P2** | Snapshot survives facade restart | `craft/tests/persistence.rs` | M |
| **P2** | 3-node majority survives one member restart | `craft/tests/persistence.rs` | M |
| **P2** | Injectable clock for runtime integration (less `sleep`) | refactor + test updates | L |
| **P2** | Wire decode fuzz (`cargo-fuzz`) | `craft-fuzz/` (T4) | M |
| **P2** | Runtime fatal-error observable path | `craft-actor/tests/runtime.rs` | S |

### Closed gaps

| Closed | What | Where |
|--------|------|-------|
| 2026-08 | Shared KV fixtures + harness helpers (dedupe ~8 copies) | `craft-test-support` (`Kv`, `TrackedKv`, `find_keys_for_two_groups`, cluster polling) |
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

**Regression rule (ADR 029):** every fixed timing/partition bug gets a test at
the lowest layer that reproduces it — usually a seeded `craft-sim` case.

---

## Maintenance

1. After adding or removing tests, refresh the **Total** column in [Per-crate inventory](#per-crate-inventory).
2. When closing a gap, update [Known gaps](#known-gaps-prioritized) → [Closed gaps](#closed-gaps).
3. Keep [ADR 029](decisions/029-testing-strategy.md) as the *strategy*; this file is the *inventory*.
4. Optional: per-crate counts via `cargo test -p <crate> --lib --tests -- --list | rg ': test$' | wc -l`.
