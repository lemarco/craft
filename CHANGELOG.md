# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the workspace
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) with all
`craft-*` crates sharing a synchronized version ([ADR 028](docs/decisions/028-library-and-publishing.md)).

Pre-1.0 (`0.x`): breaking changes may land on minor bumps and are noted here.

## [Unreleased]

### Added

- **Lease reads** ([ADR 005](docs/decisions/005-read-consistency.md)) — the
  leader serves `query` locally, with no ReadIndex round-trip, while it holds a
  quorum-confirmed leadership lease. `RaftNode::lease_read` in `craft-core`, used
  automatically by the driver's `query` fast path; conservative lease bound
  (`election_timeout_min/2`, surrendered on step-down).
- **Gossip / seed-set discovery** ([ADR 007](docs/decisions/007-discovery.md)) —
  `craft::discovery` (`Seed`, `resolve_dns_seeds`) and
  `CraftClusterBuilder::join_seeds` bootstrap a dynamic join against a resilient
  ordered seed set instead of a single address; DNS resolution maps Kubernetes
  StatefulSet pod ordinals to node ids.
- **Dev-only JSON wire** — `craft-proto/json-wire` (forwarded as `craft/json-wire`)
  swaps the wire codec from `postcard` to human-readable JSON for debugging;
  `craft::proto::WIRE_CODEC` reports the active format.
- **QUIC traffic-priority tuning** ([ADR 027](docs/decisions/027-future-work-and-risks.md)
  R2) — `craft_net::TrafficPolicy`/`RateLimiter` add opt-in per-traffic-class
  token-bucket admission control so bulk client/actor traffic cannot starve
  consensus; `CraftClusterBuilder::traffic_policy`.
- **Kubernetes / cloud deployment** — `deploy/Dockerfile` (distroless release
  image) and `deploy/kubernetes/` (StatefulSet + headless/client Services) wired
  to the `/health` `/ready` admin probes; `craft-node` gains ordinal-derived node
  ids and `CRAFT_JOIN_SEEDS` / `CRAFT_DISCOVERY` / `CRAFT_ALLOW_JOIN` env config.
- **Multi-Raft routing foundation** ([ADR 031](docs/decisions/031-write-sharding-multi-raft.md))
  — `craft-core::shard`: `ShardRouter` (stable key→shard hash) and rendezvous
  shard→group placement (`place_shard`/`shard_assignment`) with minimal churn on
  group changes. Full multi-group runtime wiring remains future work.
- **Remote scale-down stop RPC** ([ADR 018](docs/decisions/018-supervision.md))
  — `POST /raft/v1/actor/stop`: a planned removal on another node now stops that
  node's instance over the wire instead of being silently dropped (scale-down
  previously only took effect on node departure).
- **Liveness signal, distinct from membership** ([ADR 032](docs/decisions/032-liveness-vs-membership.md))
  — leader-side failure detector from heartbeat acks: `RaftNode::reachable` /
  `reachable_now` in `craft-core`, surfaced as `NodeStatus.reachable` and
  `ClusterState::reachable_nodes()` (defaults to `live_nodes()`). Placement still
  targets committed voters; this unblocks crash-driven auto-migrate/respawn
  (deferred). Foundation for retiring `NodeStatus.voters`-as-"live nodes".

### Changed

- **Leader-gated forwarded scale** ([ADR 018](docs/decisions/018-supervision.md))
  — `ClusterControl::handle_scale` re-confirms this node is still the leader and
  sources `live_nodes` from its own committed voters (never staler than the
  requester's set), so a node deposed mid-flight cannot double-place against the
  real leader's reconcile.
- **Shared `craft_net::RemoteError`** — the near-identical transport/rejection
  arms across `CastError`, `ClusterAskError`, `RemoteSpawnError`, `MigrateError`,
  `ClusterScaleError`, and `ScaleClusterError` are unified behind one
  `Remote(RemoteError)` variant.

### Fixed

- **Bounded `ask`** — same-node and local `ActorRef`/`PoolRef` asks now time out
  (`ASK_TIMEOUT`, 30s) instead of blocking forever on a wedged handler.
- **At-most-once side-effecting `ask`** — a receiver deduplicates a resent ask by
  `(origin, req_id)` and replays the cached reply, so a retried request does not
  run the handler twice.
- **Reply-encode failures surface as errors** — a reply value that fails to
  serialize is reported as a real error rather than looking like a dropped reply
  port.
- **Actor-stream backpressure** — concurrent in-flight streams on the
  `Actor`-class QUIC connection are bounded, so a burst of slow asks queues
  instead of exhausting `MAX_STREAMS` and stalling casts/spawns to a peer.

## [0.1.0] — Unreleased

Initial development release. The full workspace is in place and internally
tested; APIs are still evolving toward a 1.0 stabilization.

### Added

- **`craft` facade** — `CraftCluster` + `CraftClusterBuilder` assemble a whole
  node (consensus runtime, actor registry/control/messaging/directory, the
  leader-only cluster supervisor, and telemetry) from one call. `start_local`
  drives an in-process/`LocalNetwork` cluster; `start_quic` runs the live
  transport. Re-exports the stable public API so users add one dependency.
- **Consensus (`craft-core`)** — pure, I/O-free Raft state machine: leader
  election, log replication, membership, and `ReadIndex` linearizable reads.
- **Storage (`craft-storage`)** — durable Raft log, hard state, and snapshots.
- **Transport (`craft-net`)** — HTTP/3 over QUIC with mutual TLS between nodes,
  a `PeerDirectory` address book, and an in-memory `LocalNetwork` for tests.
  `dev-certs` feature mints a dev cluster CA + node identities.
- **Actors (`craft-actor`)** — actor runtime, registry, cluster directory, and
  a leader-driven supervisor that auto-places one worker per node; cross-node
  messaging, spawning, and state migration.
- **Client (`craft-client`)** — in-process and remote (HTTP/3) clients with
  transparent leader forwarding; typed client wrappers.
- **Macros (`craft-macros`)** — `StateMachine` derive and the `remote_actor`
  attribute (auto codec generation for cross-node delivery).
- **Redis store (`craft-store-redis`)** — a Redis-backed `ActorStateStore` for
  stateful actors, with an idempotent-worker example.
- **Dashboard (`craft-dashboard`)** — health/admin endpoints and a live
  cluster/actor introspection view over an `Observer`.
- **Simulation (`craft-sim`)** — deterministic harness for testing consensus.
- **`craft-node`** — reference binary that runs a node from environment config
  (`CRAFT_NODE_ID`, `CRAFT_LISTEN`, `CRAFT_ADMIN`, `CRAFT_PEERS`, PEM cert vars),
  resolving DNS hostnames for peers.
- **Certificate provisioning** — `examples/certs/generate.sh` (portable
  OpenSSL/LibreSSL) mints a cluster CA, per-node certs, and client certs;
  documented in `docs/certs.md`.
- **Testing** — in-process QUIC/mTLS cluster test, a linearizability checker,
  the deterministic simulator, and an `e2e/` docker-compose cluster that asserts
  leader election and failover re-election over real QUIC/mTLS.
- **Docs** — 30 architecture decision records under `docs/decisions/`, the wire
  protocol in `docs/protocol.md`, and the roadmap in `docs/backlog.md`.

[Unreleased]: https://gitlab.com/lemarco/craft/-/compare/v0.1.0...HEAD
[0.1.0]: https://gitlab.com/lemarco/craft/-/tags/v0.1.0
