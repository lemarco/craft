# Releasing crafty

crafty publishes to crates.io as a synchronized workspace: every `crafty-*` crate
shares one version and is released together ([library-and-publishing](decisions/library-and-publishing.md)).

See [process.md](process.md) for the full commit → push → CI → release diagram.

## TL;DR

```sh
# 1. Update CHANGELOG.md: move [Unreleased] items under the new version.
# 2. One-shot (recommended): release gate + bump + tag + publish + push + release build
./scripts/release.sh 0.2.0 --publish

# Or step by step:
./scripts/release.sh --dry-run              # release gate (autofix + full checks)
./scripts/release.sh 0.2.0                  # gate + bump (if needed) + tag
./scripts/release.sh 0.2.0 --publish-only   # gate + crates.io + docs + push

# Skip git push (crates.io only):
./scripts/release.sh 0.2.0 --publish --no-push
```

## How it works

- **One version.** The version lives in `[workspace.package]` in the root
  `Cargo.toml`; every crate inherits it via `version.workspace = true`.
- **Dependency-ordered publish.** `./scripts/publish-workspace.sh` uploads crates
  one at a time in topological order (see script `PUBLISH_ORDER`). **Do not**
  use `cargo publish --workspace` for real uploads.
- **Rate limits & resume.** Default **30s** pause between uploads
  (`CRAFTY_PUBLISH_DELAY_SECS`). Re-run the same command after a partial publish.
- **Not published.** `publish = false` crates are skipped, including
  `crafty-node` (reference binary — build from the repo or Docker/e2e only).
- **Release gate.** `./scripts/release-gate.sh` (= `gate.sh --tier release`) runs
  autofix, [ci-fast-lane.sh](../scripts/ci-fast-lane.sh), examples, showcase,
  MSRV (**strict**), and release build when publishing.
- **Tagging.** After the gate, bump when needed, publish dry-run on bumped manifest,
  commit, tag `vX.Y.Z`. `--publish` pushes commits + tag by default.
- **Post-publish docs.** Automatic via `release.sh --publish`; CI `publish` job
  runs `post-publish-docs.sh` and uploads README/status artifacts.

## CI

- **Fast lane** (MR/branch): `ci-fast-lane.sh`
- **Tag `publish-dry-run`**: full `ci-fast-lane.sh` (not publish-only)
- **Tag `publish`**: manual; `publish-workspace.sh` + `post-publish-docs.sh`

## Prerequisites

- `CARGO_REGISTRY_TOKEN` or `cargo login`
- Clean working tree for prepare (not `--publish-only`)
- MSRV toolchain installed locally (`rustup toolchain install 1.90`)

## Versioning policy

- **SemVer.** Under `0.x`, breaking changes may land on minor bumps; record in `CHANGELOG.md`.
- **MSRV** bumps are a minor-version event.
- **Wire/protocol** compatibility: `Raft-Protocol-Version` ([protocol.md](protocol.md)).
