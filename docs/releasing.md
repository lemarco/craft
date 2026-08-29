# Releasing crafty

crafty publishes to crates.io as a synchronized workspace: every `crafty-*` crate
shares one version and is released together ([library-and-publishing](decisions/library-and-publishing.md)).

## TL;DR

```sh
# 1. Update CHANGELOG.md: move [Unreleased] items under the new version.
# 2. Prepare the release (dry-run gate + commit if bumped + tag):
./scripts/release.sh 0.1.0          # first release: version already 0.1.0 — bump skipped
./scripts/release.sh 0.2.0          # later releases: bumps workspace version
# 3. Push, then publish:
git push && git push origin v0.1.0
./scripts/release.sh 0.1.0 --publish-only   # rate-limit safe (not cargo publish --workspace)
# 4. After publish (automatic with --publish-only, or manual):
./scripts/post-publish-docs.sh 0.1.0     # README badges + status.md — commit the diff
```

## How it works

- **One version.** The version lives in `[workspace.package]` in the root
  `Cargo.toml`; every crate inherits it via `version.workspace = true`.
  `scripts/release.sh <version>` rewrites just that line and refreshes
  `Cargo.lock`.
- **Dependency-ordered publish.** `./scripts/publish-workspace.sh` uploads crates
  one at a time in topological order (see script `PUBLISH_ORDER`). **Do not**
  use `cargo publish --workspace` for real uploads — a 12-crate first release
  triggers crates.io **HTTP 429** (new-crate burst limit). Dry-run is fine:

  ```
  crafty-macros, crafty-proto → crafty-core, crafty-storage, crafty-net →
  crafty-actor → crafty-client, crafty-dashboard, crafty-sim, crafty-store-redis →
  crafty
  ```

  (`cargo publish --workspace --dry-run` is the CI/MR gate — no uploads.)
- **Rate limits & resume.** Default **30s** pause between uploads
  (`CRAFTY_PUBLISH_DELAY_SECS`). On 429, the script waits for the server
  `try again after … GMT` time (+ 5s buffer) and retries. Re-run the same
  command after a partial publish — already-indexed versions are skipped.
- **Not published.** Workspace members with `publish = false` are skipped:
  `crafty-test-support`, `crafty-ops`, `crafty-e2e-client`,
  `crafty-e2e-queue-client`, `crafty-node` (reference binary — build from the
  repo or Docker/e2e only). Fuzz/benchmark crates live outside the workspace.
- **Manifest hygiene.** Every publishable crate inherits `homepage`, `license`, and
  categories from `[workspace.package]`; each has a crate-local `README.md`
  (`readme = "README.md"` in its manifest) and symlinks to the root `LICENSE-*`
  files. The workspace root `README.md` is for the repository landing page.
- **Tagging.** The script runs the publish dry-run gate, commits manifest/CHANGELOG
  changes when the version was bumped, and creates an annotated `vX.Y.Z` tag on
  the release commit (or on current HEAD when the version was already set).
  Push the tag to trigger the tagged CI pipeline.
- **Post-publish docs.** `./scripts/post-publish-docs.sh <version>` adds
  crates.io/docs.rs badges to the root README and updates [status.md](status.md).
  `release.sh --publish` runs this automatically after a successful upload.
- **docs.rs** builds automatically on publish, using the
  `[package.metadata.docs.rs] all-features = true` metadata on each crate.
- **Doc completeness:** workspace lint `missing_docs = "deny"` on published
  crates ([library-and-publishing](decisions/library-and-publishing.md)); CI and
  hooks enforce it. Run `./scripts/docs-missing-audit.sh --workspace` before release.

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
  presses the button with the registry token configured. It runs
  `./scripts/publish-workspace.sh`, not bare `cargo publish --workspace`.
