#!/usr/bin/env bash
# Verify every publishable crate tarball — the release gate (library-and-publishing).
#
# Usage: ./scripts/publish-dry-run.sh
#
# Two modes, depending on whether the workspace version is already on crates.io:
#
# 1. **Not yet indexed** — `cargo publish --workspace --dry-run` packages every crate
#    and resolves intra-workspace path deps against sibling tarballs in dependency
#    order. This is the only form that works before the first upload.
#
# 2. **Already indexed** — workspace dry-run can resolve deps from crates.io and
#    give a false pass while local API has diverged. We instead dry-run the leaf
#    crate (`trembita`), which matches the real publish path (`publish-workspace.sh`
#    uploads one crate at a time against the index). If local code changed since
#    the last release, this fails until you bump the workspace version.
#
# Real uploads still go through publish-workspace.sh one crate at a time — a
# 13-crate `cargo publish --workspace` trips the crates.io new-crate rate limit.

set -euo pipefail
cd "$(dirname "$0")/.."

ROOT_MANIFEST="Cargo.toml"
UA="trembita-publish-dry-run (https://gitlab.com/lemarco/trembita)"

current_version() {
    grep -m1 '^version = ' "$ROOT_MANIFEST" | sed -E 's/version = "(.*)"/\1/'
}

crate_version_on_index() {
    local name=$1 ver=$2
    curl -fsS -H "User-Agent: $UA" \
        "https://crates.io/api/v1/crates/${name}/${ver}" >/dev/null 2>&1
}

VERSION="$(current_version)"
LEAF="trembita"

if crate_version_on_index "$LEAF" "$VERSION"; then
    echo ">> publish dry-run (leaf ${LEAF} v${VERSION} — version already on crates.io)…"
    echo "   (local API changes require a version bump before publish; see CHANGELOG.md)"
    if ! cargo publish -p "$LEAF" --dry-run --allow-dirty; then
        echo "error: publish dry-run failed for ${LEAF} v${VERSION}." >&2
        echo "hint: bump [workspace.package] version (e.g. ./scripts/release.sh 0.3.0) —" >&2
        echo "      v${VERSION} is already on crates.io and cannot be overwritten." >&2
        exit 1
    fi
else
    echo ">> publish dry-run (workspace v${VERSION}, dependency order — not yet on crates.io)…"
    cargo publish --workspace --dry-run --allow-dirty
fi

echo "OK: publish dry-run"
