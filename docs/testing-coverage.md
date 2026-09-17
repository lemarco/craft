# Testing coverage matrix

Living inventory of what the trembita test suite covers, where gaps remain, and
which CI lane exercises each layer. Update this file when adding tests or
closing a gap.

**Strategy:** [testing-strategy](decisions/testing-strategy.md)  
**Feature status:** [status.md](status.md)  
**Product docs:** [product-terminology](decisions/product-terminology.md) (capabilities vs runtime actors)  
**Last audit:** 2026-09-17 (B-28–B-32 wave index)

Legend: **✅** covered · **⚠️** partial · **❌** missing · **🔒** scheduled / `#[ignore]` only

---

## Shipped backlog B-28–B-32

Product scale wave — quick regression commands (detail in scenario pages):

| Id | Focus | Primary tests | Fast command |
|----|-------|---------------|--------------|
| B-28 | Auto cap group `resolved_scale` | `capability/group.rs`, `integration/cap_scale.rs` | `./scripts/test-fast.sh -p trembita --lib auto_scale_marker_state_spawns_one_host_per_node` |
| B-29 | Cluster session cookies | `gateway/cluster_session.rs`, `tests/gateway_cluster_session.rs` | `./scripts/test-fast.sh -p trembita --test gateway_cluster_session` |
| B-30 | Ingress `/health` vs `/ready` | `trembita-http`, `trembita-dashboard`, `ingress_lb*.rs` | `./scripts/test-fast.sh -p trembita --test ingress_lb_ops` |
| B-31 | Founder scale doctor | `trembita-cli/.../doctor.rs`, `tests/cap_scale_doctor.rs` | `./scripts/test-fast.sh -p trembita-cli --test cap_scale_doctor` |
| B-32 | Product coordination scale | `configure` / `queue_opts` / `job_opts`, `env_config`, `product_coordination_scale.rs` | `./scripts/test-fast.sh -p trembita --all-features --lib b32_` |

Index: [status § Product scale wave](status.md#product-scale-wave-b-28b32) · [capabilities scenarios](scenarios/capabilities.md).

---

## Test pyramid (current)

| Layer | Scope | Location | Count (approx.) | Fast CI |
|-------|-------|----------|-----------------|---------|
| Unit | Pure functions, small modules | `#[cfg(test)]` in `src/` | ~70 | ✅ |
| Integration | Crate boundaries, async runtime | `crates/*/tests/` | ~260 | ✅ |
| Property | Raft safety under fault schedules | `trembita-sim/tests/safety.rs` (+ proptest) | 250+ seeds | ✅ |
| Compile-fail | Macro misuse → good errors | `trembita-runtime/tests/compile_fail.rs` | 3+ | ✅ |
| Deterministic sim | Whole cluster, virtual clock | `trembita-sim` harness + scenarios | 27 tests | ✅ |
| Linearizability | Client-visible histories | `trembita-sim/tests/linearizability.rs` | 2 | ✅ |
| Doctests | Public API examples | `cargo test --doc` | — | ✅ |
| Redis integration | Real `CapStateStore` | `trembita-store-redis/tests/{redis,tls}.rs` | 10 | 🔒 nightly |
| Postgres cap store | Real `CapStateStore` CAS | `trembita-capstore-postgres/tests/postgres.rs` | 4 | 🔒 nightly |
| E2E | Real processes, QUIC, mTLS, chaos | `e2e/run.sh`, `e2e/leave.sh`, `e2e/queue.sh`, `e2e/chaos.sh`, `e2e/cert_renew.sh`, `e2e/linearizability.sh` | 6 scenarios | 🔒 nightly |
| Fuzz | Wire decode never panics | `trembita-fuzz` | 1 target | 🔒 nightly |
| Bench / soak | Throughput, long-run sim | `benchmarks/` | — | 🔒 nightly |
| Examples | Product showcases + WS lab crates | [`examples/`](../examples/README.md) | 8 crates | `./scripts/check-examples.sh` |

---

## Per-crate inventory

| Crate | Unit (`src/`) | Integration (`tests/`) | Total | Primary focus |
|-------|:-------------:|:----------------------:|:-----:|---------------|
| `trembita-core` | 30 | 81 | **111** | Pure Raft FSM: election, replication, membership, snapshots, ReadIndex |
| `trembita-runtime` | 10 | 99 | **109** | `RaftDriver`, runtime, registry, placement, supervision, migration, trybuild |
| `trembita-net` | 12 | 32 | **44** | Wire framing, `LocalNetwork`, TLS handshake, loopback QUIC, protocol compat |
| `trembita-sim` | 8 | 22 | **30** | Safety/liveness under faults, linearizability, actor scenarios, multi-Raft |
| `trembita-dashboard` | **14** | **8** | **22** | Ops HTTP handlers (`/introspect/*`, dashboard), ops TLS, metrics, telemetry, LB readiness unit |
| `trembita` (facade) | **10** | **30** | **40** | `TrembitaCluster` builder, multi-Raft, keyed client, live QUIC cluster, reachability reconcile, actor store resume, gateway ops TLS, DNS discovery, B-30 ingress LB, B-32 coordination scale (unit tables + env + product boot) |
| `trembita-storage` | 0 | 7 | **7** | Store contract (Memory + Redb), namespaced groups, reopen |
| `trembita-proto` | 7 | 0 | **7** | Encode/decode roundtrips, protocol compat band |
| `trembita-store-redis` | 0 | 10 (7 `redis` + 3 `tls`, `#[ignore]` except 2 fast) | **10** | Redis CAS/TTL, dual conn, idempotent worker, reconnect, `rediss://` |
| `trembita-capstore-postgres` | 0 | 4 (`#[ignore]`, `docker-tests`) | **4** | SQL CAS, concurrency claim, idempotent worker, TTL |
| `trembita-client` | 1 | **8** | **9** | Remote client propose/query, follower forward, failover, retry policy, keyed batch |
| `trembita-ops` | 0 | 2 | **2** | Snapshot export/import, object-store push/pull |
| `trembita-macros` | — | via trybuild in `trembita-runtime` | — | Compile-pass/fail |
| `trembita-tools` | **10** | 0 | **10** | `trembita-node` env parsing (`node/config.rs`); E2E smoke via binary |
| `trembita-cli` | **13** | **16** | **29** | `new` scaffold, read-only `doctor`, B-31 cap scale foot-guns; `dev` registry (debug CLI only) |

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
| Ops HTTP TLS (server TLS on unified bind) | ✅ | ✅ `admin`, `facade` | — | 🔒 nightly | ✅ |
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

### Actor runtime (`trembita-runtime`)

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
| **Workload governor (connections + HTTP/consumer in-flight + `max_in_flight` + actor ask)** | ✅ `compute_token`, `workload`, `messaging`, `workload::decide_*` | ✅ `workload_governor` | — | — | ✅ |
| **Queue auto-shard coordinator + lazy follower open** | ✅ `queue_auto_shard`, `queue_service/auto_shard` | — | — | — | — |
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

### Framework CLI (`trembita-cli`)

Published binary **`trembita`** ([`crates/trembita-cli`](../crates/trembita-cli/)) — release install exposes **`new`** and **`doctor`** only; **`dev`** is compiled in debug builds of the CLI (repo contributors). Fast CI: `./scripts/test-fast.sh -p trembita-cli`.

| Command | Behavior under test | Tests |
|---------|---------------------|-------|
| **`new`** | Layout incl. `manifest.rs`, `.without_actors_api()` in `app.rs`, `manifest` marker regions (`jobs`/`topics`/`workers`/`capabilities`), path dep canonicalization, `cargo check`-clean templates (`serde`, `trembita::RouteTable`) | `tests/scaffold.rs`, `scaffold/render.rs`, `scaffold/features.rs` |
| **`doctor`** | manifest ↔ consumers/actors/workflows; `// trembita:topics` / `// trembita:workers` markers; duplicate ids; `.manifest()` in `app.rs`; no inline capabilities in `app.rs`; missing `mod` declarations (reports only); cap store API without `OpCtx::require_store` (R4); **B-31** cap scale foot-guns (`.instances(1)`+queued without `key`, session without `.per_node()`) | `scaffold/doctor.rs`, `tests/add_doctor.rs` |
| **`doctor --preflight`** | deploy env / gateway ops hints | `scaffold/doctor.rs` (unit scenarios) |
| **`dev *`** (debug CLI) | showcase registry, workspace root, `trigger.sh` path | `tests/dev.rs`; full `dev up` 🔒 manual / `TREMBITA_DEV_INTEGRATION` |
| **Project discover** | walk parents; requires `app.rs` + `manifest.rs` | `tests/add_doctor.rs`, `scaffold/project.rs` |
| **Marker names** | `// trembita:*` region constants + consumer type naming | `scaffold/markers.rs` |

Facade registry type: [`AppManifest`](../crates/trembita/src/app/manifest.rs) unit test in `trembita` crate (`manifest_chains_into_builder`).

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
| **`trembita-node` `TREMBITA_PEERS` → `EnvOverrides` / `into_app_config`** | ✅ `config` | — | — | ✅ implicit | ✅ |
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
| `msrv` | Every MR / push | `cargo check` on Rust 1.94 |
| `e2e` | Scheduled | `e2e/run.sh` + `e2e/leave.sh` + `e2e/chaos.sh` + `e2e/cert_renew.sh` + docker phase of `e2e/linearizability.sh` |
| `linearizability-sim` | Scheduled | trembita-sim linearizability + read_index seed sweep (`e2e/linearizability.sh`) |
| `store-redis` | Scheduled | `cargo test -p trembita-store-redis --features docker-tests -- --ignored` |
| `capstore-postgres` | Scheduled | `cargo test -p trembita-capstore-postgres --features docker-tests -- --ignored` |
| `bench` | Scheduled | criterion (`append`/`apply`/`deliver`/`queue`) + 120s `soak` + 60s `soak_multi_raft` + 60s `soak_queue` + 60s `soak_actor_store` + 60s `soak_saga` + 60s `soak_session` |
| `fuzz` | Scheduled | `cargo-fuzz` wire_decode in `crates/trembita-fuzz` |

Local hooks mirror the fast lane: `lefthook` pre-commit (fmt, shellcheck, clippy —
skipped when no matching staged files) and pre-push (clippy → tests → doctests → doc
→ publish dry-run → MSRV; optional release via `--tags release`). See
`scripts/quality-gate-*.sh`.

---

## Known gaps (prioritized)

Track open gaps here; append fixed rows to [archive/testing-closed-gaps.md](archive/testing-closed-gaps.md).

| Priority | Gap | Suggested test location | Effort |
|----------|-----|-------------------------|--------|
| Medium | Actor store redb contract at crate level | `trembita-capstore/tests/redb_contract.rs` | S |
| Low | `doctor --preflight` on full synthetic `deploy/` tree | `tests/add_doctor.rs` or `scaffold/doctor.rs` | S |
| Low | `trembita dev up` multi-node smoke in CI (debug CLI) | `tests/dev.rs` + job label `run-heavy` | M |

**Closed gaps (historical):** [archive/testing-closed-gaps.md](archive/testing-closed-gaps.md) — append new rows there when fixing a gap.

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
2. When closing a gap, update [Known gaps](#known-gaps-prioritized) and append a row to [archive/testing-closed-gaps.md](archive/testing-closed-gaps.md).
3. Keep [testing-strategy](decisions/testing-strategy.md) as the *strategy*; this file is the *inventory*.
4. Optional: per-crate counts via `cargo test -p <crate> --lib --tests -- --list | rg ': test$' | wc -l`.
