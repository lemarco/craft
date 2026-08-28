# Library distribution & publishing policy

**Status:** Accepted  
**Date:** 2026-07-05

## Context

[deployment-model](deployment-model.md) sets crafty as a **library-first framework**; [naming](naming.md) reserves publish-ready `crafty-*` names. This ADR makes the **publishing contract** explicit: crafty is intended to be **published to crates.io** as a reusable library, not just an internal repo.

## Decision

**crafty is a public, open-source Rust library published on crates.io.**

### Public vs internal crates

| Crate | Published | Audience |
|-------|-----------|----------|
| `crafty` | **Yes** — primary | Framework users depend on this |
| `crafty-macros` | Yes | Re-exported by `crafty`; also direct |
| `crafty-proto`, `crafty-core`, `crafty-storage`, `crafty-net`, `crafty-actor`, `crafty-client` | Yes | Advanced users / composability |
| `crafty-store-redis` | Yes (optional) | Redis `ActorStateStore` |
| `crafty-dashboard` | Yes (optional) | Monitoring UI |
| `crafty-sim` | Yes (dev) | Testing / simulation |
| `crafty-node` | **No** (`publish = false`) | Reference/demo runner — build from repo or e2e Docker |

`crafty` facade re-exports the stable public API so users typically add **one dependency**.

### Versioning

- **SemVer** across the workspace; all `crafty-*` crates share a synchronized version.
- Pre-1.0 (`0.x`): breaking changes allowed on minor bumps, documented in CHANGELOG.
- Wire/protocol compatibility tracked separately via `Raft-Protocol-Version` ([protocol.md](../protocol.md)); protocol changes are breaking and gated by [cluster-membership](cluster-membership.md#version-skew--hard-reject).

### MSRV

- Declared **Minimum Supported Rust Version** in `Cargo.toml` (`rust-version`).
- MSRV bumps are a minor-version event, noted in CHANGELOG.

### License

- **Dual `MIT OR Apache-2.0`** (Rust ecosystem convention).
- `LICENSE-MIT`, `LICENSE-APACHE`, and SPDX headers where practical.

### Quality gates for publish

- `#![forbid(unsafe_code)]` where feasible; justify any `unsafe` in module docs.
- `#![deny(missing_docs)]` on public crates before 1.0 stabilization (warn pre-1.0).
- `cargo doc` clean; docs.rs metadata configured (features, all-features build).
- CI: `fmt`, `clippy -D warnings`, tests, MSRV check, `cargo publish --dry-run`.
- Keywords/categories set (`concurrency`, `network-programming`, `asynchronous`).

### Release process

- `cargo release` (or workspace script) publishes crates in dependency order:
  `crafty-proto → crafty-core / crafty-storage / crafty-macros → crafty-net → crafty-actor → crafty-client → crafty-store-redis / crafty-dashboard / crafty-sim → crafty`.
- Tag `vX.Y.Z`; GitLab release with CHANGELOG excerpt.
- `docs.rs` builds automatically on publish.

### Repository

- Public repository (folder `distributive_raft_actor_system`); README with quickstart, examples, and link to `docs/`.
- `examples/` (KV store, three-node local, VPS join, Redis worker) double as documentation.

## Consequences

**Positive**

- Clear expectation: crafty is a distributable library, not just app code
- Users get one dep (`crafty`) + optional advanced crates
- Publishing discipline (SemVer, MSRV, docs, license) set from day one

**Negative**

- Publishing overhead (docs, CHANGELOG, dry-run CI, name reservation)
- Synchronized versioning couples crate releases
- `crafty` name must be secured on crates.io (fallback: org scope or `crafty-rs`)

## Related

- [deployment-model.md](deployment-model.md)
- [naming.md](naming.md)
- [cluster-membership.md#version-skew--hard-reject](cluster-membership.md#version-skew--hard-reject)
