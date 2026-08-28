#!/usr/bin/env bash
#
# release.sh — cut a synchronized workspace release (library-and-publishing).
#
# All crafty-* crates share one version (`[workspace.package] version`), so a
# release is: bump that version (when needed), refresh the lockfile, run the
# publish dry-run gate, commit, tag `vX.Y.Z`, and (optionally) publish every
# crate in dependency order via `cargo publish --workspace`.
#
# Usage:
#   ./scripts/release.sh <version>            # prepare: bump + verify + commit + tag
#   ./scripts/release.sh <version> --publish  # the above, then publish to crates.io
#   ./scripts/release.sh --dry-run            # just run the publish dry-run gate
#
# When the workspace version already matches <version> (typical for the first
# release), the bump step is skipped and the current HEAD is tagged.
#
# Publishing needs CARGO_REGISTRY_TOKEN (or `cargo login`) and push access for
# the tag. Update CHANGELOG.md's [Unreleased] section before running.

set -euo pipefail
cd "$(dirname "$0")/.."

ROOT_MANIFEST="Cargo.toml"

die() { echo "error: $*" >&2; exit 1; }

current_version() {
    grep -m1 '^version = ' "$ROOT_MANIFEST" | sed -E 's/version = "(.*)"/\1/'
}

# `cargo publish --workspace --dry-run` packages every crate and resolves the
# intra-workspace path deps locally, in dependency order — the release gate.
dry_run() {
    echo ">> publish dry-run (all crates, dependency order)…"
    cargo publish --workspace --dry-run
}

# Bump the workspace version: the `[workspace.package]` version *and* the
# `version = "…"` pin on every internal `crafty-* = { path = …, version = … }`
# dependency (so a bumped crate depends on the bumped, not the previous, crates).
set_version() {
    local version="$1" old tmp
    old="$(current_version)"
    [ -n "$old" ] || die "could not read current [workspace.package] version"
    tmp="$(mktemp)"
    awk -v old="$old" -v ver="$version" '
        $0 == "version = \"" old "\"" { print "version = \"" ver "\""; next }
        /^crafty[a-z-]* = / {
            gsub("version = \"" old "\"", "version = \"" ver "\""); print; next
        }
        { print }
    ' "$ROOT_MANIFEST" > "$tmp"
    mv "$tmp" "$ROOT_MANIFEST"
    grep -q "^version = \"$version\"" "$ROOT_MANIFEST" || die "failed to set version to $version"
    echo ">> bumped workspace version $old -> $version (incl. internal dep pins)"
}

prepare_release() {
    local version="$1"
    local old bumped=0

    old="$(current_version)"
    [ -n "$old" ] || die "could not read current [workspace.package] version"

    git rev-parse "v$version" >/dev/null 2>&1 \
        && die "tag v$version already exists"

    if [ "$old" = "$version" ]; then
        echo ">> version already $version (skipping bump)"
    else
        set_version "$version"
        cargo update --workspace >/dev/null 2>&1 || true
        bumped=1
    fi

    dry_run

    if [ "$bumped" = 1 ]; then
        git add "$ROOT_MANIFEST" Cargo.lock
    fi
    if ! git diff --quiet -- CHANGELOG.md 2>/dev/null; then
        git add CHANGELOG.md
    fi

    if git diff --cached --quiet; then
        echo ">> no manifest changes; tagging current HEAD"
    else
        git commit -m "chore(release): crafty v$version"
    fi

    git tag -a "v$version" -m "crafty v$version"
    echo ">> tagged v$version (push with: git push && git push origin v$version)"
}

# ---- arg parsing ----------------------------------------------------------
if [ "${1:-}" = "--dry-run" ]; then
    dry_run
    echo "OK: dry-run passed."
    exit 0
fi

VERSION="${1:-}"
PUBLISH=0
[ "${2:-}" = "--publish" ] && PUBLISH=1
[ -n "$VERSION" ] || die "usage: $0 <version> [--publish] | --dry-run"
echo "$VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([-.].+)?$' \
    || die "version must look like X.Y.Z (got: $VERSION)"

[ -z "$(git status --porcelain)" ] || die "working tree not clean; commit or stash first"

prepare_release "$VERSION"

if [ "$PUBLISH" = 1 ]; then
    echo ">> publishing to crates.io (dependency order)…"
    cargo publish --workspace
    echo ">> updating docs for published release…"
    ./scripts/post-publish-docs.sh "$VERSION"
    echo "OK: published crafty v$VERSION."
else
    echo "OK: prepared crafty v$VERSION. Re-run with --publish (or run 'cargo publish --workspace') to release."
    echo "tip: after publish, run ./scripts/post-publish-docs.sh $VERSION if you did not use --publish."
fi
