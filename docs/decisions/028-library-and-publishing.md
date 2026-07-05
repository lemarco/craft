# ADR 028: Library distribution & publishing policy

**Status:** Accepted  
**Date:** 2026-07-05

## Context

[ADR 004](004-deployment-model.md) sets craft as a **library-first framework**; [ADR 009](009-naming.md) reserves publish-ready `craft-*` names. This ADR makes the **publishing contract** explicit: craft is intended to be **published to crates.io** as a reusable library, not just an internal repo.

## Decision

**craft is a public, open-source Rust library published on crates.io.**

### Public vs internal crates

| Crate | Published | Audience |
|-------|-----------|----------|
| `craft` | **Yes** — primary | Framework users depend on this |
| `craft-macros` | Yes | Re-exported by `craft`; also direct |
| `craft-proto`, `craft-core`, `craft-storage`, `craft-net`, `craft-actor`, `craft-client` | Yes | Advanced users / composability |
| `craft-store-redis` | Yes (optional) | Redis `ActorStateStore` |
| `craft-dashboard` | Yes (optional) | Monitoring UI |
| `craft-sim` | Yes (dev) | Testing / simulation |
| `craft-node` | Published as binary (`cargo install`) | Reference/demo runner |

`craft` facade re-exports the stable public API so users typically add **one dependency**.

### Versioning

- **SemVer** across the workspace; all `craft-*` crates share a synchronized version.
- Pre-1.0 (`0.x`): breaking changes allowed on minor bumps, documented in CHANGELOG.
- Wire/protocol compatibility tracked separately via `Raft-Protocol-Version` ([protocol.md](../protocol.md)); protocol changes are breaking and gated by [ADR 020](020-join-version-skew.md).

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
  `craft-proto → craft-core / craft-storage / craft-macros → craft-net → craft-actor → craft-client → craft-store-redis / craft-dashboard / craft-sim → craft → craft-node`.
- Tag `vX.Y.Z`; GitLab release with CHANGELOG excerpt.
- `docs.rs` builds automatically on publish.

### Repository

- Public repository (folder `distributive_raft_actor_system`); README with quickstart, examples, and link to `docs/`.
- `examples/` (KV store, three-node local, VPS join, Redis worker) double as documentation.

## Consequences

**Positive**

- Clear expectation: craft is a distributable library, not just app code
- Users get one dep (`craft`) + optional advanced crates
- Publishing discipline (SemVer, MSRV, docs, license) set from day one

**Negative**

- Publishing overhead (docs, CHANGELOG, dry-run CI, name reservation)
- Synchronized versioning couples crate releases
- `craft` name must be secured on crates.io (fallback: org scope or `craft-rs`)

## Related

- [004-deployment-model.md](004-deployment-model.md)
- [009-naming.md](009-naming.md)
- [020-join-version-skew.md](020-join-version-skew.md)
