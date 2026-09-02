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
if ! grep -q 'img.shields.io/crates/v/trembita' "$README"; then
    tmp="$(mktemp)"
    awk '
        /img.shields.io.*rustc/ {
            print
            print ""
            print "[![crates.io](https://img.shields.io/crates/v/trembita.svg)](https://crates.io/crates/trembita)"
            print "[![docs.rs](https://docs.rs/trembita/badge.svg)](https://docs.rs/trembita)"
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
        -e 's|release-ready (\[releasing.md\](releasing.md))|published on [crates.io](https://crates.io/crates/trembita)|' \
        -e "s|run \[releasing.md\](releasing.md) (\`./scripts/release.sh [^)]*\`)|published v$VERSION — see [CHANGELOG.md](../CHANGELOG.md)|" \
        "$STATUS"
    echo ">> updated $STATUS for v$VERSION"
fi

# Refresh the version strings themselves. The blocks above only fire on the
# first publish (they match pre-publish wording that is gone afterwards), so
# without this the README and status.md keep advertising the previous release.
# Written as substitutions on the version pattern, so re-running is a no-op.
for f in "$README" "$STATUS"; do
    sed -i -E \
        -e "s/(\\| \\*\\*Version\\*\\* \\| \`)[0-9]+\\.[0-9]+\\.[0-9]+(\`)/\\1$VERSION\\2/" \
        -e "s#(\\| \\*\\*Release\\*\\* \\| v)[0-9]+\\.[0-9]+\\.[0-9]+#\\1$VERSION#" \
        -e "s#docs\\.rs/trembita/[0-9]+\\.[0-9]+\\.[0-9]+#docs.rs/trembita/$VERSION#g" \
        -e "s#(crates\\.io / docs\\.rs publish\\*\\* . v)[0-9]+\\.[0-9]+\\.[0-9]+#\\1$VERSION#" \
        "$f"
done
echo ">> refreshed version references to v$VERSION in $README and $STATUS"

echo "OK: post-publish docs refreshed for v$VERSION."
echo "tip: commit with: git commit -am \"docs: mark trembita v$VERSION published on crates.io\""
