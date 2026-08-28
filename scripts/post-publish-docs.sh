#!/usr/bin/env bash
#
# post-publish-docs.sh — README badges + status text after a crates.io release.
#
# Usage:
#   ./scripts/post-publish-docs.sh 0.1.0
#
# Idempotent: safe to re-run for the same version.

set -euo pipefail
cd "$(dirname "$0")/.."

VERSION="${1:-}"
die() { echo "error: $*" >&2; exit 1; }

[ -n "$VERSION" ] || die "usage: $0 <version>"
echo "$VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([-.].+)?$' \
    || die "version must look like X.Y.Z (got: $VERSION)"

README="README.md"
STATUS="docs/status.md"

# Add crates.io + docs.rs badges after the MSRV badge (once).
if ! grep -q 'img.shields.io/crates/v/crafty' "$README"; then
    tmp="$(mktemp)"
    awk '
        /img.shields.io.*rustc/ {
            print
            print ""
            print "[![crates.io](https://img.shields.io/crates/v/crafty.svg)](https://crates.io/crates/crafty)"
            print "[![docs.rs](https://docs.rs/crafty/badge.svg)](https://docs.rs/crafty)"
            next
        }
        { print }
    ' "$README" > "$tmp"
    mv "$tmp" "$README"
    echo ">> added crates.io + docs.rs badges to $README"
fi

# Drop the pre-publish crates.io bullet from "Not yet".
if grep -q 'crates.io / docs.rs publish' "$README"; then
    sed -i '/^- crates\.io \/ docs\.rs publish/d' "$README"
    echo ">> removed pre-publish crates.io note from $README"
fi

# status.md: maturity + release bullets (idempotent — matches pre-publish wording).
if grep -q 'release-ready' "$STATUS"; then
    sed -i \
        -e 's|release-ready (\[releasing.md\](releasing.md))|published on [crates.io](https://crates.io/crates/crafty)|' \
        -e "s|run \[releasing.md\](releasing.md) (\`./scripts/release.sh [^)]*\`)|published v$VERSION — see [CHANGELOG.md](../CHANGELOG.md)|" \
        "$STATUS"
    echo ">> updated $STATUS for v$VERSION"
fi

echo "OK: post-publish docs refreshed for v$VERSION."
echo "tip: commit with: git commit -am \"docs: mark crafty v$VERSION published on crates.io\""
