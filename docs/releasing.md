# Releasing crafty

crafty publishes to crates.io as a synchronized workspace: every `crafty-*` crate
shares one version and is released together ([library-and-publishing](decisions/library-and-publishing.md)).

## TL;DR

```sh
# 1. Update CHANGELOG.md: move [Unreleased] items under the new version.
# 2. Prepare the release (bump + dry-run gate + commit + tag):
./scripts/release.sh 0.2.0
# 3. Push, then publish:
git push && git push origin v0.2.0
./scripts/release.sh 0.2.0 --publish   # or: cargo publish --workspace
```

## How it works

- **One version.** The version lives in `[workspace.package]` in the root
  `Cargo.toml`; every crate inherits it via `version.workspace = true`.
  `scripts/release.sh <version>` rewrites just that line and refreshes
  `Cargo.lock`.
- **Dependency-ordered publish.** `cargo publish --workspace` resolves the
  intra-workspace path dependencies locally and uploads crates in topological
  order:

  ```
  crafty-macros, crafty-proto → crafty-core, crafty-storage, crafty-net →
  crafty-actor → crafty-client, crafty-dashboard, crafty-sim, crafty-store-redis →
  crafty → crafty-node
  ```

  (`--dry-run` does everything except the upload — this is the CI gate.)
- **Not published.** Workspace members with `publish = false` are skipped:
  `crafty-test-support`, `crafty-ops`, `crafty-e2e-client`,
  `crafty-e2e-queue-client`. Fuzz/benchmark crates live outside the workspace.
- **Manifest hygiene.** Every publishable crate inherits `readme`, `homepage`,
  `license`, and categories from `[workspace.package]`; each has a crate-local
  `README.md` and symlinks to the root `LICENSE-*` files.
- **Tagging.** The script commits `chore(release): crafty vX.Y.Z` and creates an
  annotated `vX.Y.Z` tag. Push the tag to trigger the tagged CI pipeline.
- **docs.rs** builds automatically on publish, using the
  `[package.metadata.docs.rs] all-features = true` metadata on each crate.
- **Doc completeness:** workspace lint `missing_docs = "warn"` on published
  crates ([library-and-publishing](decisions/library-and-publishing.md)); CI
  allows the lint pre-1.0 (`RUSTFLAGS=-A missing_docs`). Before 1.0, flip to
  `deny` and run `./scripts/docs-missing-audit.sh --workspace` until clean.

## Prerequisites

- `CARGO_REGISTRY_TOKEN` in the environment (or `cargo login`) with publish
  rights to the `crafty*` crate names.
- Push access for the release commit and tag.
- A clean working tree (the script refuses to run otherwise).

## Versioning policy

- **SemVer.** Pre-1.0 (`0.x`), breaking changes may land on minor bumps and are
  recorded in `CHANGELOG.md`.
- **MSRV** bumps (`rust-version`) are a minor-version event, noted in the
  changelog.
- **Wire/protocol** compatibility is tracked separately via
  `Raft-Protocol-Version` (see [protocol.md](protocol.md)); protocol changes are
  breaking and gated by [join-version-skew](decisions/cluster-membership.md#version-skew--hard-reject).

## CI

- **Fast lane** (every MR/branch push) runs `cargo publish --workspace
  --dry-run` so a broken manifest, missing include, or version-skew is caught
  before merge.
- **Tag pipelines** repeat the gate in `publish-dry-run` (fast lane does not
  run on tags) before the manual `publish` job.
- The actual publish is a **manual** tagged-pipeline job (`publish`) so a human
  presses the button with the registry token configured.
