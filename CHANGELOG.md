# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the workspace
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html) with all
`craft-*` crates sharing a synchronized version ([ADR 028](docs/decisions/028-library-and-publishing.md)).

Pre-1.0 (`0.x`): breaking changes may land on minor bumps and are noted here.

## [Unreleased]

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
