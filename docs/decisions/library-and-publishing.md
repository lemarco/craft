# Library distribution & publishing policy

**Status:** Accepted  
**Date:** 2026-07-05

## Context

[deployment-model](deployment-model.md) sets trembita as a **library-first framework**; [naming](naming.md) reserves publish-ready `trembita-*` names. This ADR makes the **publishing contract** explicit: trembita is intended to be **published to crates.io** as a reusable library, not just an internal repo.

## Decision

**trembita is a public, open-source Rust library published on crates.io.**

### Public vs internal crates

| Crate | Published | Audience |
|-------|-----------|----------|
| `trembita` | **Yes** — primary | Framework users depend on this; optional integrations via [Cargo features](facade.md) |
| `trembita-macros` | Yes | Re-exported by `trembita`; direct dep optional |
| `trembita-proto`, `trembita-core`, `trembita-storage`, `trembita-net`, `trembita-runtime`, `trembita-jobs`, `trembita-events`, `trembita-capstore`, `trembita-client`, `trembita-assembly`, `trembita-test-support`, `trembita-metrics-otlp` | Yes | Advanced users / composability |
| `trembita-cli` | Yes | Scaffold CLI (`trembita new`, `doctor`, …) |
| `trembita-http` | Yes (optional) | Product HTTP — enabled via `trembita/http-jobs` |
| `trembita-capstore-postgres` | Yes (optional) | Postgres `CapStateStore` — `trembita/capstore-postgres` |
| `trembita-store-redis` | Yes (optional) | Redis `CapStateStore` — `trembita/redis-store` |
| `trembita-backlog-postgres` | Yes (optional) | Postgres `ExternalBacklog` — `trembita/external-backlog` |
| `trembita-events-postgres` | Yes (optional) | Postgres outbox — `trembita/domain-outbox` |
| `trembita-dashboard` | Yes (optional) | Monitoring UI (always linked by facade today) |
| `trembita-sim` | Yes (dev) | Testing / simulation |
| `trembita-tools`, `trembita-showcase`, `trembita-test-facade`, `trembita-test-runtime`, e2e/fuzz | **No** (`publish = false`) | Build from repo; reference `trembita-node` / showcase bins |

`trembita` facade re-exports the stable public API so users typically add **one dependency** and enable integrations with features ([facade.md](facade.md)).

### Versioning

- **SemVer** across the workspace; all `trembita-*` crates share a synchronized version.
- Pre-1.0 (`0.x`): breaking changes may land on minor bumps; **detailed history starts at 1.0** ([CHANGELOG.md](../../CHANGELOG.md)).
- Wire/protocol compatibility tracked separately via `Raft-Protocol-Version` ([protocol.md](../protocol.md)); protocol changes are breaking and gated by [cluster-membership](cluster-membership.md#version-skew--hard-reject).

### MSRV

- Declared **Minimum Supported Rust Version** in `Cargo.toml` (`rust-version`).
- MSRV bumps are a minor-version event, noted in CHANGELOG.

### License

- **Dual `MIT OR Apache-2.0`** (Rust ecosystem convention).
- `LICENSE-MIT`, `LICENSE-APACHE`, and SPDX headers where practical.

### Quality gates for publish

- `#![forbid(unsafe_code)]` where feasible; justify any `unsafe` in module docs.
- `#![deny(missing_docs)]` on published crates (see [missing-docs-1.0.md](missing-docs-1.0.md)).
- `cargo doc` clean; docs.rs metadata configured (features, all-features build).
- CI: `fmt`, `clippy -D warnings`, tests, MSRV check, `cargo publish --dry-run`.
- Keywords/categories set (`concurrency`, `network-programming`, `asynchronous`).

### Release process

- `cargo release` (or workspace script) publishes crates in dependency order:
  `./scripts/publish-workspace.sh` order (see script `PUBLISH_ORDER`): proto → core/storage/macros/net → test-support → runtime → capstore → jobs/events → client → optional adapters → **assembly** → **trembita** → **trembita-cli**.
- Tag `vX.Y.Z`; GitLab release with CHANGELOG excerpt.
- `docs.rs` builds automatically on publish.

### Repository

- Public repository (folder `distributive_raft_actor_system`); README with quickstart, examples, and link to `docs/`.
- `examples/` — four [product showcases](../../examples/README.md) plus self-update; excluded from workspace default-members; CI via `./scripts/check-examples.sh`
- Reference KV StateMachine: `trembita_core::kv` (not a separate example crate)

## Consequences

**Positive**

- Clear expectation: trembita is a distributable library, not just app code
- Users get one dep (`trembita`) + optional advanced crates
- Publishing discipline (SemVer, MSRV, docs, license) set from day one

**Negative**

- Publishing overhead (docs, CHANGELOG, dry-run CI, name reservation)
- Synchronized versioning couples crate releases
- `trembita` name must be secured on crates.io (fallback: org scope or `trembita-rs`)

## Related

- [deployment-model.md](deployment-model.md)
- [naming.md](naming.md)
- [facade.md](facade.md)
- [cluster-membership.md#version-skew--hard-reject](cluster-membership.md#version-skew--hard-reject)
