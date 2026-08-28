# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the workspace
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) with all
`crafty-*` crates sharing a synchronized version ([library-and-publishing](docs/decisions/library-and-publishing.md)).

Pre-1.0 (`0.x`): breaking changes may land on minor bumps and are noted here.

## [Unreleased]

### Added

- **`tracing` + `pretty_assertions`** — `crafty::init_tracing()`, rebalance/role `tracing` events; `crafty_test_support` helpers.
- **Multi-Raft runtime** ([write-sharding-multi-raft](docs/decisions/multi-raft.md)) — `ShardedNodeService`, keyed `ProposeKeyed`/`QueryKeyed`, per-group redb (`data_dir`), rebalance + cross-node group migration RPC.
- **Per-group membership** ([per-group-raft-membership](docs/decisions/cluster-membership.md#per-group-membership-multi-raft)) — `group_replication_factor`, `sync_group_membership`.
- **Tier 1 multi-Raft** ([tier1-multi-raft-advances](docs/decisions/multi-raft.md#tier-1-advances-landed)) — learners, `expand_shard_count`, `propose_keyed_batch`, `/introspect/raft-groups`.
- **Tier 2 multi-Raft** ([tier2-multi-raft-architecture](docs/decisions/multi-raft.md)) — dynamic catalog (`add_raft_groups`), stable shards (default), `catalog_version`, `switch_to_stable_shards`.
- **Meta-Raft coordinator** ([meta-raft](docs/decisions/multi-raft.md#meta-raft-coordinator)) — dedicated `group-meta.redb` for join/leave, catalog, and saga journal in multi-Raft mode; group 0 is user data only.
- **Cross-shard saga** ([cross-shard-transactions](docs/decisions/multi-raft.md#cross-shard-transactions)) — `run_saga`, `resume_saga`, `StoreSagaJournal`, `MetaRaftSagaJournal`/`CompositeSagaJournal` (alias `Group0SagaJournal`), metrics; optional 2PC (`cross_shard_2pc`); durable 2PC (`durable_cross_shard_2pc`) with per-group Raft log entries, prepare timeout GC, client journal (`StoreTwoPhaseJournal`/`CompositeTwoPhaseJournal`), facade `run_keyed_2pc`/`resume_cross_shard_2pc`, metrics (`crafty_2pc_*`), and `examples/cross_shard_2pc.rs`.
- **Actor routing Tier 3** ([actor-routing-tier3](docs/decisions/actor-routing-tier3.md)) — consistent-hash ring, `ActorSession`, per-group drain, `DirectoryPolicy::ReadYourWrites`.
- **Follower + lease reads** ([read-consistency](docs/decisions/client-and-routing.md#read-consistency)) — `ReadIndexConfirm` path, `RaftNode::lease_read` fast path.
- **Liveness vs membership** ([liveness-vs-membership](docs/decisions/cluster-membership.md#liveness-vs-membership)) — `reachable_nodes()`, crash-driven supervisor reconcile.
- **Discovery & ops** — seed-set + DNS discovery; cluster leave RPC; mTLS hot reload; K8s manifests; `TrafficPolicy`; `crafty-ops` backup/restore; admin HTTPS; linearizability E2E.
- **Dev JSON wire** — `crafty/json-wire` feature.
- **Durable job queue (tier C)** ([job-queue](docs/decisions/job-queue.md)) — `JobQueue` port, `RedbJobQueue`, leader `QueueService`, sync voter replication, `ClusterJobQueue`, worker autoscale.
- **Job queue v2** — sharded streams (`job_queue_sharded`), priority/delayed enqueue (`EnqueueOptions`), enqueue dedup keys, membership autoscale hook (`job_queue_membership_autoscale`); examples in `job_queue_cluster`.
- **Job queue production polish** — parallel voter replicate (`JoinSet`), replicate auth (caller must be Raft leader via `LocalTransport` / QUIC peer id), Meta-Raft persisted autoscale policy (`QueueAutoscalePolicyCommand`, `job_queue_autoscale` / `job_queue_membership_autoscale`), periodic `redb` compaction after acks.
- **Job queue docs + examples + E2E** — `/queue/*` routes in `docs/protocol.md`; `job_queue_worker` cluster follower worker + failover; `crafty-e2e-queue-client` + `e2e/queue.sh` (QUIC, 3-node).
- **Durable mailbox spool** — redb outbox/inbox for cross-node `/actor/deliver`; builder `.durable_mailbox(true)`.

### Changed

- **MSRV 1.98** — workspace, CI, and `deploy/Dockerfile` aligned.
- **Leader-gated forwarded scale** — deposed nodes cannot double-place against the real leader.
- **Shared `crafty_net::RemoteError`** — unified remote error variant across actor/cluster APIs.

### Fixed

- Bounded `ask` timeout (30s); at-most-once side-effecting `ask` dedup; reply-encode errors surfaced; actor-stream backpressure on QUIC.

## [0.1.0] — Unreleased

Initial development release. The full workspace is in place and internally
tested; APIs are still evolving toward a 1.0 stabilization.

### Added

- **`crafty` facade** — `CraftyCluster` + `CraftyClusterBuilder` assemble a whole
  node (consensus runtime, actor registry/control/messaging/directory, the
  leader-only cluster supervisor, and telemetry) from one call. `start_local`
  drives an in-process/`LocalNetwork` cluster; `start_quic` runs the live
  transport. Re-exports the stable public API so users add one dependency.
- **Consensus (`crafty-core`)** — pure, I/O-free Raft state machine: leader
  election, log replication, membership, and `ReadIndex` linearizable reads.
- **Storage (`crafty-storage`)** — durable Raft log, hard state, and snapshots.
- **Transport (`crafty-net`)** — HTTP/3 over QUIC with mutual TLS between nodes,
  a `PeerDirectory` address book, and an in-memory `LocalNetwork` for tests.
  `dev-certs` feature mints a dev cluster CA + node identities.
- **Actors (`crafty-actor`)** — actor runtime, registry, cluster directory, and
  a leader-driven supervisor that auto-places one worker per node; cross-node
  messaging, spawning, and state migration.
- **Client (`crafty-client`)** — in-process and remote (HTTP/3) clients with
  transparent leader forwarding; typed client wrappers.
- **Macros (`crafty-macros`)** — `StateMachine` derive and the `remote_actor`
  attribute (auto codec generation for cross-node delivery).
- **Redis store (`crafty-store-redis`)** — a Redis-backed `ActorStateStore` for
  stateful actors, with an idempotent-worker example.
- **Dashboard (`crafty-dashboard`)** — health/admin endpoints and a live
  cluster/actor introspection view over an `Observer`.
- **Simulation (`crafty-sim`)** — deterministic harness for testing consensus.
- **`crafty-node`** — reference binary that runs a node from environment config
  (`CRAFTY_NODE_ID`, `CRAFTY_LISTEN`, `CRAFTY_ADMIN`, `CRAFTY_PEERS`, PEM cert vars),
  resolving DNS hostnames for peers.
- **Certificate provisioning** — `examples/certs/generate.sh` (portable
  OpenSSL/LibreSSL) mints a cluster CA, per-node certs, and client certs;
  documented in `docs/certs.md`.
- **Testing** — in-process QUIC/mTLS cluster test, a linearizability checker,
  the deterministic simulator, and an `e2e/` docker-compose cluster that asserts
  leader election and failover re-election over real QUIC/mTLS.
- **Docs** — consolidated decision records under `docs/decisions/`, [status.md](docs/status.md), wire protocol in `docs/protocol.md`.

[Unreleased]: https://gitlab.com/lemarco/craft/-/compare/v0.1.0...HEAD
[0.1.0]: https://gitlab.com/lemarco/craft/-/tags/v0.1.0
