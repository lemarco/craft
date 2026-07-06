# craft — Full-featured backlog

Implementation backlog derived from ADRs 001–028. Organized by **epics**, with a **dependency graph** and **parallel tracks** so independent work can proceed simultaneously.

Legend: **[P]** parallelizable within its wave · **→** depends on · effort **S/M/L/XL**.

---

## Dependency graph (crates)

```mermaid
flowchart TB
    proto[craft-proto]
    core[craft-core]
    storage[craft-storage]
    net[craft-net]
    macros[craft-macros]
    actor[craft-actor]
    client[craft-client]
    redis[craft-store-redis]
    dash[craft-dashboard]
    sim[craft-sim]
    facade[craft]
    node[craft-node]

    proto --> core
    proto --> storage
    proto --> net
    proto --> macros
    core --> actor
    storage --> actor
    net --> actor
    macros --> actor
    net --> client
    proto --> client
    actor --> redis
    actor --> dash
    net --> dash
    core --> sim
    net --> sim
    actor --> facade
    client --> facade
    redis --> facade
    facade --> node
```

**Critical path:** `proto → core → actor → facade → node`.  
**Parallel opportunity:** once `proto` is stable, `core`, `storage`, `net`, `macros` proceed **in parallel**.

---

## Waves & parallel tracks

### Wave 0 — Foundations (mostly sequential)

| ID | Task | Effort | Deps | Status |
|----|------|--------|------|--------|
| W0.1 | Cargo workspace, `craft-*` crate stubs, compile | S | — | ✅ done |
| W0.2 | CI: fmt, clippy -D warnings, test, MSRV, (publish dry-run at release) ([ADR 028](decisions/028-library-and-publishing.md)) | M | W0.1 | ✅ done (fast + nightly lanes) |
| W0.3 | `rustfmt.toml`, `clippy.toml`, license files (MIT/Apache) | S | W0.1 | ✅ done |
| W0.4 | `craft-proto`: `NodeId/Term/LogIndex`, `LogEntry`, RPC enums, client + join + actor wire types, `postcard` codec ([ADR 011](decisions/011-serialization.md)) | L | W0.1 | ✅ done (+ roundtrip tests) |

`craft-proto` (W0.4) is the gate for all parallel tracks. **Wave 0 complete** — `cargo build/fmt/clippy -D warnings/test` all green; the parallel tracks (A/B/C/D) can now begin.

---

### Wave 1 — Parallel core tracks (after `craft-proto`)

Four **independent tracks** — different owners can work simultaneously.

#### Track A — Consensus core (`craft-core`) — critical path
| ID | Task | Effort | Status |
|----|------|--------|--------|
| A1 | Pure FSM scaffolding: effects-as-data `Output`, roles, config, deterministic RNG, `Log` | M | ✅ done |
| A2 | Leader election (randomized timeout, vote rules, up-to-date check) | L | ✅ done |
| A3 | Log replication (prevLog matching, conflict truncation, commit index, backtracking, Figure-8 term rule) | L | ✅ done |
| A4 | **Joint-consensus membership** (add/remove, C_old,new) ([ADR 016](decisions/016-membership-early.md)) | XL | **done** |
| A5 | ReadIndex linearizable reads ([ADR 005](decisions/005-read-consistency.md)) | M | **done** |
| A6 | Snapshot install + log compaction | L | **done** |
| A7 | Unit + `proptest` for safety invariants | L | ✅ done (34 core tests + sim suite) |

**Track A complete:** `craft-core` implements the full single-Raft-group FSM as a pure, I/O-free state machine (ADR 030): leader election, log replication, **joint-consensus membership (A4)**, **ReadIndex linearizable reads (A5)**, and **snapshot install + log compaction (A6)**. Elections use **Pre-Vote** (Raft thesis §9.6) so isolated/removed nodes cannot disrupt a live leader by inflating terms. The FSM is modeled with value objects — `LogId (term,index)`, `Round`, and a `Configuration` value object that owns quorum arithmetic — rather than scattered primitives. Covered by 75 unit/rule tests plus a `craft-sim` deterministic harness (Track I1–I4 partial) running liveness, grow/shrink membership (including removing the current leader), linearizable-read, snapshot catch-up, and 250 randomized fault schedules asserting election safety, commit agreement, and monotonic application.

> **Note (A6):** the log carries a snapshot boundary `(snapshot_index, snapshot_term)`; `compact(up_to, data)` discards the applied prefix, and a leader ships `InstallSnapshot` (carrying the configuration, since config entries may be compacted away) to followers whose next index falls below the boundary. A follower installs the snapshot, emits `LoadSnapshot` for the runtime to reset its state machine, and resumes replication. Chunked transfer (`offset`/`done`) is wired in the protocol but sent single-chunk for now.

> **Note (A4):** membership uses the standard two-phase joint consensus — a change appends a transitional `C_old,new` requiring majorities in *both* voter sets, then the leader appends the final `C_new`; a leader removed from `C_new` steps down once it commits. New nodes join as followers and catch up before/while being promoted.

> **Note (A5):** ReadIndex captures the leader's commit index, confirms leadership via a heartbeat `Round` acked by a quorum, and only serves the read once an entry of the current term is committed and applied through the read index. Loss of leadership fails pending reads (`ReadFailed`) so the client retries against the new leader.

#### Track B — Persistence (`craft-storage`) **[P]**
| ID | Task | Effort | Status |
|----|------|--------|--------|
| B1 | `LogStore`/`HardStateStore`/`SnapshotStore` traits | M | **done** |
| B2 | In-memory impl (tests) | S | **done** |
| B3 | `redb` impl + crash-recovery tests | L | **done** |
| B4 | Storage integration into the driver + restart recovery | M | **done** |

> **Track B progress (B1–B3):** `craft-storage` defines three storage ports — `HardStateStore` (term + vote), `LogStore` (append-only with suffix-truncate and prefix-purge), and `SnapshotStore` — plus value types `HardState`, `SnapshotMeta`, and `Snapshot`. Two adapters implement them: an in-memory `MemoryStorage` test double and a crash-safe `RedbStorage` backed by a single `redb` file (log entries keyed by index + a metadata table for hard state, snapshot, and the purge boundary, each write committed in its own transaction). A single store-contract test suite runs against **both** backends (so the simulator's in-memory double provably matches production), and a dedicated test reopens a `redb` file across "process lifetimes" to prove hard-state, compaction, and snapshot durability. `redb` is pinned to `2.x` to hold the workspace MSRV at 1.85 (ADR 028).

> **B4 done — durability & restart recovery.** The core (`RaftNode`) now tracks a durability watermark and exposes `take_persist() -> Option<Persist>`: the exact hard-state (`term`/`voted_for`) and log delta (`truncate_from` + appended entries) accumulated since the previous call, produced by routing every log mutation through instrumented helpers. `RaftDriver` owns a `Box<dyn RaftStorage>` (the new `craft-storage::RaftStorage` supertrait bundling all three ports) and persists this delta **synchronously at the top of `drain`, before surfacing any effect** — so a follower never ack's an unflushed entry and a node never reveals an unrecorded vote (Raft §5.1–§5.3), and because a commit is reported to its client only after that fsync, a non-durable commit is never acknowledged. On restart, `RaftNode::restore` / `RaftDriver::recover` rebuild the node from the stored hard state + log as a follower with `commit_index`/`last_applied` reset to 0; the state machine is rebuilt by replaying the recovered log as the node re-establishes a commit index (no snapshot ⇒ full-log replay). Nodes that opt out of durability use `RaftDriver::new`, backed by a no-op `NullStorage`. Tested at both layers: core `take_persist`/`restore` unit tests (election, plain propose, follower conflict-truncation, restore round-trip) and driver-level tests over a shared `MemoryStorage` proving writes are persisted as they commit and that state (incl. last-write-wins values and an advanced term) survives a simulated crash + recovery. **Deferred:** snapshot durability (compaction / `InstallSnapshot` persistence via `SnapshotStore` + `purge_prefix`) rides with the snapshot-shipping work.

#### Track C — Transport (`craft-net`) **[P]**
| ID | Task | Effort | Status |
|----|------|--------|--------|
| C1 | `Transport` trait + address/peer map | M | **done** |
| C2 | QUIC/HTTP/3 server via `quinn`+`h3` ([ADR 010](decisions/010-wire-transport.md)) | L | **done** |
| C3 | rustls mTLS peers; **dedicated peer connection** ([ADR 027](decisions/027-future-work-and-risks.md) R2) | L | **done** (mTLS handshake; dedicated peer connection = C5) |
| C4 | Route dispatch: `/peer/wire`, `/client/wire`, `/cluster/join`, `/actor/*` | M | **done** |
| C5 | Peer connection pool + reconnect/backoff | M | partial (per-peer connection cache + evict-on-error; backoff pending) |
| C6 | `postcard` framing over HTTP/3 bodies | S | **done** |

> **Track C progress (C1, C4, C6):** `craft-net` now owns the transport-agnostic wire contract, split into four pure/near-pure, fully-tested modules. `route` defines the fixed `/raft/v1/*` route table as a `Route` enum with `path`/`from_path`/`method` and a per-route `TrafficClass` (peer consensus is isolated from client/actor/cluster traffic for the dedicated-connection requirement of ADR 027 R2). `wire` frames `postcard` bodies of `craft-proto` types with the `application/x-postcard` content-type, a protocol-version check (missing header ⇒ v1), and a 16 MiB body-size guard that rejects oversized inputs *before* allocating on decode. `peer` provides the `PeerDirectory` (`NodeId → SocketAddr` book) and an IPv6-safe HTTPS route-URL builder. `transport` defines the **`Transport`/`RequestHandler` ports** (object-safe boxed-future signatures so the runtime can hold an `Arc<dyn Transport>`) plus typed `send_peer_rpc`/`send_client_request` helpers and an in-memory `LocalNetwork` switch (with `attach`/`detach` to model crash/partition) — the same abstraction ADR 010 says the simulator and QUIC stack must share. Covered by 22 tests (16 wire/route/peer + 6 async transport: peer/client round-trips, unreachable + detach/partition, `Arc<dyn Transport>`, and 50 concurrent sends).
>
> **mTLS (C3 core):** the `tls` module builds the `quinn` server/client configs for mutual auth (ADR 006) — `server_config` requires every incoming peer/client to present a cert chaining to the cluster CA (`WebPkiClientVerifier`), `client_config` presents this node's cert and trusts the CA, both over the `craft/1` ALPN and the explicitly-selected `ring` crypto provider (so embedding never fights the host over the process-default provider). Under the default-on `dev-certs` feature, `ClusterCa` mints a self-signed CA and per-node identities whose Common Name / DNS SAN binds a cert to its `NodeId` (`craft-node-<id>`); production instead feeds operator certs to `NodeIdentity::from_der`. Three tests run a **real loopback `quinn` handshake**: mutual auth succeeds (both ends see the peer cert), a client signed by a foreign CA is rejected, and a server-name mismatch fails.
>
> **Live HTTP/3 transport (C2):** the `quic` module wires the mTLS configs into real endpoints. `QuicServer` runs the accept loop, turns each authenticated QUIC connection into an `h3` server connection, drains the request body (enforcing the 16 MiB cap → `413`), dispatches the path to a `RequestHandler` (unknown route → `404`, handler error → `500`), and writes the `postcard` response. `QuicTransport` implements the `Transport` port over an `h3` client: it resolves a peer's address via the `PeerDirectory`, dials it with mTLS + the `craft-node-<id>` server name, caches one connection/`SendRequest` per peer (spawning the connection driver) and evicts it on error so the next call reconnects. Four end-to-end tests exchange real `postcard` RPCs over authenticated HTTP/3 on loopback: peer-RPC and client-request round-trips, cached-connection reuse, and an unreachable (directory-absent) peer. Track C essentials are now complete; remaining polish: reconnect **backoff** and a truly *dedicated* peer connection separate from client/actor streams (**C5**), and mapping the presented peer cert back to a `NodeId` for per-connection authorization.

#### Track D — Macros (`craft-macros`) **[P]**
| ID | Task | Effort | Status |
|----|------|--------|--------|
| D0 | `StateMachine` + `Command`/`Query` trait API in `craft-core` ([ADR 001](decisions/001-state-machine.md)) | M | **done** |
| D1 | `StateMachine` derive (encode/decode glue) ([ADR 001](decisions/001-state-machine.md)) | M | **n/a** — serde blanket impls |
| D2 | `UserActor` derive (message serde bounds, migratable) | M | pending |
| D3 | Compile-fail tests (trybuild) | S | pending |

> **Track D progress (D0):** the application state-machine port from ADR 001 now lives in `craft-core`: the `StateMachine` trait (`apply`/`query`/`snapshot`/`restore` with associated `Command`/`Query`/`Response`/`Error` types) plus `Command` and `Query` marker traits. The "encode/decode glue" ADR 001 wanted from a derive is instead provided by **blanket impls** over any `serde` type that also meets the replication bounds — `Command: Clone + Send + 'static`, `Query: Send + 'static` — so a borrowed or non-`Clone` command simply fails to compile (the exact owned/clone-safe check ADR 001 specified), and **D1's bespoke derive is unnecessary**. A reference in-memory KV machine and 10 tests cover set/delete/append (incl. the error path leaving state untouched), the applied-index watermark, snapshot↔restore replacement, malformed-snapshot rejection, determinism (byte-identical snapshots for identical input), and the `Command`/`Query` codec round-trips. Remaining: **D2** (`UserActor` derive) and **D3** (trybuild compile-fail suite).

---

### Wave 2 — Integration (`craft-actor`) — after A, B, C, D

Depends on core + storage + net + macros. Some sub-tasks **[P]** among themselves.

| ID | Task | Effort | Notes |
|----|------|--------|-------|
| E1 | `RaftNodeActor` (ractor): mailbox → `RaftInput` → execute `RaftOutput` | XL | **done (async loop)** — `spawn_node` runs a tokio event loop owning `RaftDriver`, dispatching `NetEffect`s over a `craft-net` `Transport` and feeding peer replies back; `NodeHandle` + `NodeService` bridge clients/peers. (Uses a tokio task, not `ractor`; ractor supervision arrives with E6+.) |
| E2 | Timers: election + heartbeat | M | **done** — runtime tick timer (`RuntimeConfig::tick_period`) drives the core's tick-based election/heartbeat clock |
| E3 | Applier loop → `StateMachine::apply` | M | **done** — `RaftDriver` applies committed cmds in order, serves ReadIndex queries, restores snapshots; single-node + 3-node routed tests |
| E4 | Client handling + **transparent forward** ([ADR 003](decisions/003-client-routing.md)) | M | **done** — client propose/query correlated to responses; a follower transparently forwards `/client/wire` requests to the leader over the transport (bounded by `forward_timeout`) and returns its response; `Error` when no leader is known |
| E5 | Join handling → membership change ([ADR 017](decisions/017-join-rpc.md), [ADR 020](decisions/020-join-version-skew.md)) | L | **done** — `/cluster/join` → leader runs joint-consensus membership change; version-skew hard-reject (ADR 020), duplicate/joins-disabled rejects, follower forwards to leader, `Accepted` on commit. `allow_join` gates acceptance |
| E6 | `ActorRegistry`: local spawn/spawn_pool/scale_local/stop ([ADR 012](decisions/012-elastic-cluster.md)) | L | **done** — local `UserActor` runtime: `spawn`/`spawn_pool`/`scale_local`/`stop`, `ActorRef`/`PoolRef` with RR + keyed routing and `ask` via `RpcReplyPort`; production one-worker-per-name guard (ADR 014). tokio-based (not `ractor`), matching E1 |
| E7 | Cross-node directory + `resolve`/`cluster` ([ADR 013](decisions/013-cross-node-actors.md)) | L | **done** — `ActorDirectory` merged cluster view (per-node authoritative, monotonic-`epoch` state-based LWW; empty snapshot revokes), `resolve(ActorId)`/`lookup`/`cluster(name)→ClusterRef` with RR + keyed target selection; `DirectorySync` publishes the local snapshot over `/actor/register` and serves inbound as a `RequestHandler`. Proto: `ActorId`/`ActorTypeId`/`ActorRegistration`/`DirectoryUpdate`/`RegisterAck` + `send_directory_update`; `UserActor::MIGRATABLE`; `ActorRegistry::local_registrations`. Cross-node *delivery* is E8 |
| E8 | Remote delivery `/actor/deliver`, routing RR + keyed ([ADR 019](decisions/019-cluster-routing.md)) | M | **done** — `ClusterMessaging.cast`/`cast_keyed` resolve a target instance via the directory (E7) then deliver locally (`ActorRegistry::deliver_local`) or ship an `ActorEnvelope` over `/actor/deliver`; serves inbound as a `RequestHandler` → `DeliverAck`. Wire ingress is type-erased via `UserActor::decode_message` (default = local-only `NotAddressable` opt-out); proto `ActorEnvelope{to,from,req_id,payload,reply_expected}` + `DeliverAck` + `send_actor_deliver`. Fire-and-forget (cast) only; cross-node `ask` deferred (fields reserved). 2-node LocalNetwork tests for RR local+remote split, keyed pinning, no-target, remote-missing-instance, and non-addressable rejection |
| E9 | `spawn_remote` / `scale_cluster` + placement ([ADR 013](decisions/013-cross-node-actors.md)) | L | **done** — `ClusterControl.spawn_remote` (local, or `SpawnRequest` over `/actor/spawn` reconstructed by a per-node type factory via `register_type::<A>()`) and `scale_cluster` driving a group to a cluster-wide count. Pure `plan_scale` planner implements the one-worker-per-node model (ADR 014): stable keep/spawn/remove diff vs directory + live membership, `InsufficientNodes` when `total > nodes`. `scale_cluster` executes spawns and applies local removals; remote teardown returned in the plan for the supervisor (E10). Proto `SpawnRequest`/`SpawnReply` + `send_actor_spawn`; `UserActor::encode_config`/`decode_config` (default `NotSpawnable`). 13 tests: planner (fill/keep/cap/scale-down/dead-node/prune) + wire spawn (self/remote/unknown-type/not-spawnable) + scale (place/reject/demote) |
| E10 | **ClusterSupervisor** (leader-only) reconcile ([ADR 018](decisions/018-supervisor-leader.md)) | L | **done** — `ClusterSupervisor` holds a declarative set of managed groups (`manage::<A>(name, total, config)`) and a `reconcile()` that runs **only on the leader** (skips with `ran_as_leader=false` otherwise), diffing desired vs directory+membership via the E9 planner and executing spawns through `ClusterControl`. Idempotent once the directory converges; per-group errors surfaced without aborting the pass. Leadership/membership abstracted behind the `ClusterState` port (runtime wires the real one; mock for tests). `manage` registers the type factory so any node can place the group. 4 tests: follower-skip, one-per-node placement, idempotency, over-capacity error. Non-leader `scale_cluster` forwarding is a runtime concern (later) |
| E11 | Auto-spawn workers on join ([ADR 015](decisions/015-auto-spawn-on-join.md)) | M | **done** — `ClusterSupervisor::manage_auto::<A>(name, config)` declares an auto-worker group whose target **tracks the live membership** (one per node): a node that joins gets a worker on the next leader reconcile, a node that leaves has its instance planned for removal. Spawns are now **idempotent** end-to-end (local `NameExists` → no-op; remote `SpawnReply` treats `NameExists` as success, ADR 013), so reconcile is safe even before the directory converges. `Target::{Fixed,PerLiveNode}` resolved per pass; blanket `ClusterState for Arc<T>`. 3 added tests: auto-worker on joiner, idempotent reconcile w/o directory, departed-node removal plan. Reconcile trigger on membership-commit is wired when the facade embeds the supervisor into the runtime |
| E12 | Migration on leave/crash + drain timeout ([ADR 022](decisions/022-drain-timeout.md)) | L | **done** — **Graceful drain** (ADR 022): actor instances run a serial mailbox that also carries a snapshot-capture control item, so `ActorRegistry::stop_graceful(name, timeout)` sets a `draining` flag (new sends rejected with `SendError::Draining`/`DeliverError::Draining`), lets queued + in-flight work finish, and force-aborts on timeout, returning `DrainOutcome::{Completed,TimedOut}`. `DEFAULT_DRAIN_TIMEOUT = 60s`. **Migration** (ADR 013): `UserActor::{migration_snapshot,restore_migration}` (stateless defaults: empty snapshot / no-op); `ClusterControl::migrate::<A>(from, to_node, config, drain_timeout)` captures the source's snapshot **via the mailbox** (ordered after queued messages), ships `MigrateRequest` over `/actor/migrate`, and the target factory spawns a replacement via `spawn_restoring` (restore before first message), then the source is drained+stopped; generation bumped. Idempotent target (`NameExists` → success). 5 tests: drain completes / times-out-and-rejects, stateful state transfer + source teardown, non-local + same-node rejection. Wiring leave/crash → auto-migrate into the runtime is a facade concern (Wave 4) |
| E13 | 1-worker-per-VPS enforcement + dev mode ([ADR 014](decisions/014-one-worker-per-vps.md)) | S | [P] |
| E14 | Restart policies ([ADR 026](decisions/026-observability.md) §5) | M | [P] |

---

### Wave 3 — Parallel feature crates (after `craft-actor` core)

Independent tracks again.

#### Track F — Client (`craft-client`) **[P]**
| ID | Task | Effort |
|----|------|--------|
| F1 | `ClientHandle` (in-process, ractor) | M |
| F2 | `RemoteClient` (HTTP/3 + client mTLS) ([ADR 006](decisions/006-security.md)) | M |
| F3 | `TypedClient<M>` encode/decode | S |
| F4 | Retry/leader-follow ergonomics (forward already server-side) | S |

#### Track G — Redis store (`craft-store-redis`) **[P]**
| ID | Task | Effort |
|----|------|--------|
| G1 | `ActorStateStore` trait in `craft-actor` | S |
| G2 | Redis impl (`fred`/`redis`) get/set/del/CAS/TTL ([ADR 021](decisions/021-actor-state-redis.md)) | M |
| G3 | Example worker using store + idempotency | S |

#### Track H — Observability (`craft-dashboard` + admin) **[P]**
| ID | Task | Effort |
|----|------|--------|
| H1 | Admin HTTP `:8080`: `/health`, `/ready` ([ADR 025](decisions/025-health-admin-port.md)) | M |
| H2 | Prometheus `/metrics` ([ADR 026](decisions/026-observability.md)) | M |
| H3 | Telemetry event stream (`cluster.events()`) | M |
| H4 | Introspection JSON API `/introspect/*` | L |
| H5 | Live dashboard UI + SSE `/dashboard` | L |
| H6 | Opt-in message tracing | M |

#### Track I — Simulation (`craft-sim`) **[P]** (can start once core+net traits exist)
| ID | Task | Effort | Status |
|----|------|--------|--------|
| I1 | In-memory network (delay/drop/partition/isolate) | M | ✅ done |
| I2 | Virtual clock; deterministic seeded scheduler + reorder | L | ✅ done |
| I3 | Fault injectors + safety-invariant assertions (election safety, agreement, monotonic apply) | L | ✅ done |
| I4 | Scenarios: election, replication, partition, join/leave, migrate, scale_cluster | L | partial (election/replication/partition/membership done; actor migrate+scale pending) |
| I5 | Linearizability checker (`porcupine`-style) over sim histories ([ADR 029](decisions/029-testing-strategy.md)) | L | pending |

#### Track T — Testing & CI (cross-cutting) **[P]** ([ADR 029](decisions/029-testing-strategy.md))
| ID | Task | Effort | Notes |
|----|------|--------|-------|
| T1 | `cargo-nextest` wiring + fast/nightly CI lanes | S | [P] anytime |
| T2 | `proptest` harness for Raft safety invariants | M | → A1 |
| T3 | `trybuild` compile-fail suite (macros) | S | → D3 (same) |
| T4 | `craft-fuzz`: `postcard`/wire decoders (`cargo-fuzz`) | M | → W0.4 |
| T5 | Storage crash-recovery tests (kill mid-write, reopen) | M | → B3 |
| T6 | In-process integration: N nodes, loopback QUIC + mTLS | M | → C3 |
| T7 | `testcontainers-rs` Redis integration for `ActorStateStore` | M | → G2 |
| T8 | E2E `docker-compose` cluster + real mTLS certs | L | → J3 |
| T9 | Chaos injection in E2E (`pumba`/`toxiproxy`: partition, latency) | L | → T8 |
| T10 | `criterion` benches (append, apply, deliver) + soak harness | M | → E1 |

---

### Wave 4 — Facade, binary, examples, publish

| ID | Task | Effort | Deps |
|----|------|--------|------|
| J1 | `craft` facade: `CraftCluster::builder()`, re-exports | M | actor, client |
| J2 | Builder options: node_id, listen, join, allow_join, resource_profile, auto_workers, drain_timeout, admin_listen, actor_state_store | M | J1 |
| J3 | `craft-node` reference binary + CLI/env | M | J1 |
| J4 | Examples: KV store, three-node local, VPS join, Redis worker | L | J1 |
| J5 | `docs.rs` metadata, README quickstart, CHANGELOG | M | — [P] |
| J6 | `examples/certs/` script + `docs/certs.md` ([ADR 024](decisions/024-cert-provisioning.md)) | M | — [P] |
| J7 | Release tooling (`cargo release`, publish order) ([ADR 028](decisions/028-library-and-publishing.md)) | M | all |

---

## Parallelization summary

| Wave | Can run in parallel |
|------|---------------------|
| 0 | Mostly sequential (proto gates everything) |
| 1 | **Track A / B / C / D** — 4 concurrent streams |
| 2 | `craft-actor` sub-tasks partly parallel (E2/E3/E6/E13/E14 alongside E1) |
| 3 | **Track F / G / H / I** — 4 concurrent streams |
| — | **Track T (testing/CI)** spans all waves; T1 starts at W0, others follow their target task |
| 4 | J5/J6 anytime; J1–J4, J7 near the end |

**Minimum critical path:** W0.1 → W0.4 → A1→A2→A3→A4 → E1→E5→E10→E11 → J1 → J3.  
Everything else (storage, net polish, macros, client, redis, observability, sim, docs) fans out around it.

## Suggested team split (if parallel)

| Stream | Owns |
|--------|------|
| **Consensus** | Track A + E1/E5/E10/E12 + Track I |
| **Platform/net** | Track C + E7/E8/E9 + client (F) |
| **Storage/state** | Track B + Track G |
| **DX/observability** | Track D + Track H + examples/docs (J4/J5/J6) |

Testing (Track T) is **cross-cutting**: each stream writes tests for its own code; the Consensus stream owns the sim harness + linearizability checker; DX owns CI lanes + E2E/chaos infra. See [ADR 029](decisions/029-testing-strategy.md).

## Milestones

| Milestone | Contains | Demoable |
|-----------|----------|----------|
| **M1 core** | W0, Track A (A1–A3), B, C skeleton | single-node replicate (in-memory) |
| **M2 cluster** | A4, E1–E5, C full | 3-node elect + replicate + join over HTTP/3 |
| **M3 actors** | E6–E13, F, G | cross-node actors, auto-spawn, Redis state |
| **M4 ops** | H, migration E12, sim I | dashboard, metrics, partition tests |
| **M5 release** | J1–J7 | `cargo add craft`, examples, docs.rs |

## Related

- [architecture.md](architecture.md) · [README.md](README.md) (ADR index) · [open-questions.md](open-questions.md)
