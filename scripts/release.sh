#!/usr/bin/env bash
#
# release.sh — cut a synchronized workspace release (library-and-publishing).
#
# Usage:
#   ./scripts/release.sh <version>                      # prepare: gate + bump + tag
#   ./scripts/release.sh <version> --publish            # prepare + publish + push (default)
#   ./scripts/release.sh <version> --publish --no-push  # publish without git push
#   ./scripts/release.sh <version> --publish-only         # publish when tag exists
#   ./scripts/release.sh --dry-run                      # release gate (same as release-gate.sh)
#
# Real publishes use publish-workspace.sh (rate-limit safe). Update CHANGELOG.md
# [Unreleased] before running.

set -euo pipefail
cd "$(dirname "$0")/.."

ROOT_MANIFEST="Cargo.toml"

die() { echo "error: $*" >&2; exit 1; }

current_version() {
    grep -m1 '^version = ' "$ROOT_MANIFEST" | sed -E 's/version = "(.*)"/\1/'
}

release_gate() {
    local extra=()
    if [[ "${RELEASE_BUILD:-0}" == "1" ]]; then
        extra+=(--release-build)
    fi
    echo ">> release gate (autofix + full checks, MSRV strict)…"
    bash scripts/release-gate.sh "${extra[@]}"
}

commit_autofix_if_needed() {
    if git diff --quiet && git diff --cached --quiet; then
        return 0
    fi
    git add -u
    if git diff --cached --quiet; then
        return 0
    fi
    git commit -m "chore: apply fmt/clippy autofix"
}

dry_run() {
    echo ">> publish dry-run (bumped manifest, dependency order)…"
    cargo publish --workspace --dry-run --allow-dirty
}

do_publish() {
    local version=$1
    echo ">> publishing to crates.io (rate-limit safe, dependency order)…"
    ./scripts/publish-workspace.sh "$version"
    echo ">> updating docs for published release…"
    ./scripts/post-publish-docs.sh "$version"
    echo "OK: published trembita v$version."
}

commit_post_publish_docs() {
    local version=$1
    if git diff --quiet -- README.md docs/status.md 2>/dev/null; then
        return 0
    fi
    git add README.md docs/status.md
    git commit -m "docs: mark trembita v${version} published on crates.io"
}

push_release() {
    local version=$1
    echo ">> pushing commits and tag v${version}…"
    git push
    git push origin "v${version}"
}

set_version() {
    local version="$1" old tmp
    old="$(current_version)"
    [ -n "$old" ] || die "could not read current [workspace.package] version"
    tmp="$(mktemp)"
    awk -v old="$old" -v ver="$version" '
        $0 == "version = \"" old "\"" { print "version = \"" ver "\""; next }
        /^trembita[a-z0-9-]* = / {
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
        && die "tag v$version already exists (use --publish-only to publish)"

    if [ "$old" = "$version" ]; then
        echo ">> version already $version (skipping bump)"
    else
        set_version "$version"
        cargo update --workspace >/dev/null 2>&1 || true
        bumped=1
    fi

    # Gate (incl. publish dry-run) after bump so intra-workspace deps resolve
    # against sibling tarballs at the new version, not the previous crates.io release.
    release_gate

    if [ "$bumped" = "1" ]; then
        git add "$ROOT_MANIFEST"
        if git ls-files --error-unmatch Cargo.lock >/dev/null 2>&1; then
            git add Cargo.lock
        fi
    fi
    if ! git diff --quiet -- CHANGELOG.md 2>/dev/null; then
        git add CHANGELOG.md
    fi
    if ! git diff --quiet; then
        git add -u
    fi

    if git diff --cached --quiet; then
        echo ">> no manifest changes; tagging current HEAD"
    else
        git commit -m "chore(release): trembita v$version"
    fi

    git tag -a "v$version" -m "trembita v$version"
    echo ">> tagged v$version"
}

# ---- arg parsing ----------------------------------------------------------
if [ "${1:-}" = "--dry-run" ]; then
    bash scripts/release-gate.sh
    echo "OK: release gate passed."
    exit 0
fi

VERSION="${1:-}"
PUBLISH=0
PUBLISH_ONLY=0
PUSH=0
NO_PUSH=0
RELEASE_BUILD=0
shift || true
while [ $# -gt 0 ]; do
    case "$1" in
        --publish) PUBLISH=1 ;;
        --publish-only) PUBLISH=1; PUBLISH_ONLY=1 ;;
        --push) PUSH=1 ;;
        --no-push) NO_PUSH=1 ;;
        --release-build) RELEASE_BUILD=1 ;;
        *)
            die "usage: $0 <version> [--publish] [--publish-only] [--push|--no-push] [--release-build] | --dry-run"
            ;;
    esac
    shift
done

[ -n "$VERSION" ] || die "usage: $0 <version> [--publish] [--publish-only] [--push|--no-push] [--release-build] | --dry-run"
echo "$VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([-.].+)?$' \
    || die "version must look like X.Y.Z (got: $VERSION)"

# --publish and --publish-only push to origin by default; --no-push opts out.
if [ "$PUBLISH" = "1" ] && [ "$NO_PUSH" = "0" ]; then
    PUSH=1
fi
# Full publish runs release build unless explicitly skipped elsewhere.
if [ "$PUBLISH" = "1" ]; then
    RELEASE_BUILD=1
fi

if [ "$PUBLISH_ONLY" = "1" ]; then
    git rev-parse "v$VERSION" >/dev/null 2>&1 \
        || die "tag v$VERSION not found; run without --publish-only to prepare first"
    release_gate
    commit_autofix_if_needed
    do_publish "$VERSION"
    commit_post_publish_docs "$VERSION"
    if [ "$PUSH" = "1" ]; then
        push_release "$VERSION"
    fi
    exit 0
fi

[ -z "$(git status --porcelain)" ] || die "working tree not clean; commit or stash first"

prepare_release "$VERSION"

if [ "$PUBLISH" = "1" ]; then
    do_publish "$VERSION"
    commit_post_publish_docs "$VERSION"
fi

if [ "$PUSH" = "1" ]; then
    push_release "$VERSION"
elif [ "$PUBLISH" = "1" ]; then
    echo ">> tip: git push && git push origin v$VERSION (or omit --no-push next time)"
else
    echo "OK: prepared trembita v$VERSION."
    echo "  ./scripts/release.sh $VERSION --publish-only"
fi
