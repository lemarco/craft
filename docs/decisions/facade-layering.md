# Facade layering — product vs assembly vs showcase

**Status:** Accepted  
**Date:** 2026-09-16  

## Context

The `trembita` crate is the **semver product surface** (`TrembitaApp`, gateway, capabilities, `trembita::cluster` runtime handles). Cluster **boot** (builder, env merge, journals, upgrade coordinator wiring) grew large and blurred the product API boundary.

## Decision

### Published: `trembita`

- Product path: [`TrembitaApp::from_env`](../../crates/trembita/src/app/runtime.rs) / [`from_config`](../../crates/trembita/src/app/runtime.rs) + [`AppManifest`](../../crates/trembita/src/app/manifest.rs).
- Re-exports: [`trembita::env`](../../crates/trembita/src/env.rs) (`AppConfig`, `EnvOverrides`, `app_config_from_env`), [`trembita::discovery`](../../crates/trembita/src/lib.rs), [`trembita::upgrade`](../../crates/trembita/src/lib.rs), [`trembita::cluster`](../../crates/trembita/src/cluster.rs) (handles + types after boot).
- **Not exported:** `TrembitaClusterBuilder`, custom state-machine assembly.

### Unpublished: `trembita-assembly`

- [`TrembitaClusterBuilder`](../../crates/trembita-assembly/src/builder/mod.rs), [`merge_app_config`](../../crates/trembita-assembly/src/builder/cluster/config.rs), env parsing ([`env_config.rs`](../../crates/trembita-assembly/src/env_config.rs)), certs, workload, saga/two-phase journals, upgrade HTTP hooks.
- `trembita` depends on it internally; product code should not add a direct dependency unless maintaining the framework.

### Unpublished: `trembita-showcase`

- Maintainer harnesses: custom SM demos, [`cluster_builder`](../../crates/trembita-showcase/src/cluster.rs) for benchmarks/soaks.
- Bins: `showcase-migrate-demo`, `showcase-self-update` (`cargo run -p trembita-showcase --bin …`).

### In-crate tests: `trembita::integration`

- Framework tests use `trembita_assembly::TrembitaClusterBuilder` via `integration` module — not public API.

### Reference binary: `trembita-node`

- Parses ops env into [`NodeConfig`](../../crates/trembita-tools/src/node/config.rs), maps to [`AppConfig`](../../crates/trembita/src/env.rs) with [`EnvOverrides`](../../crates/trembita-assembly/src/env_config.rs) (e.g. `TREMBITA_PEERS` → `env.peers`) so [`merge_app_config`](../../crates/trembita-assembly/src/builder/cluster/config.rs) applies static membership on [`TrembitaApp::from_config`](../../crates/trembita/src/app/builder.rs).

## Consequences

- Docs and ADRs that pointed at `crates/trembita-assembly/src/builder/` should point at **`trembita-assembly`**.
- Soaks/benches import **`trembita_showcase::cluster::cluster_builder`**, not `trembita::workspace_showcase`.
- [public-api-1.0](public-api-1.0.md) remains the product contract; this ADR is the crate map for maintainers.
