#!/usr/bin/env bash
# Verify every publishable crate tarball — the release gate (library-and-publishing).
#
# Usage: ./scripts/publish-dry-run.sh
#
# Uses `cargo publish --workspace --dry-run`, which packages every crate and
# resolves intra-workspace path deps against the sibling tarballs it just built,
# in dependency order. This is the gate named in docs/releasing.md, and the only
# form that works when the workspace version is not yet on crates.io.
#
# Per-crate `cargo publish -p <pkg> --dry-run` cannot stand in for it: a dry run
# leaves nothing in `target/package/`, so the moment one crate depends on another
# at a version that is not yet published, resolution falls back to the crates.io
# index and fails with "candidate versions found which didn't match".
#
# Real uploads still go through publish-workspace.sh one crate at a time — a
# 13-crate `cargo publish --workspace` trips the crates.io new-crate rate limit.
# A dry run uploads nothing, so that constraint does not apply here.

set -euo pipefail
cd "$(dirname "$0")/.."

echo ">> publish dry-run (workspace, dependency order)…"
cargo publish --workspace --dry-run --allow-dirty

echo "OK: publish dry-run (workspace)"
