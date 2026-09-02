#!/usr/bin/env bash
#
# publish-workspace.sh — publish crafty workspace crates to crates.io (library-and-publishing).
#
# `cargo publish --workspace` uploads many *new* crates in one burst and triggers
# crates.io HTTP 429 ("published too many new crates in a short period"). This
# script publishes in dependency order, one crate at a time, skips versions
# already on the index, waits between uploads, and retries 429s using the
# server-provided retry time.
#
# Usage:
#   ./scripts/publish-workspace.sh              # publish current workspace version
#   ./scripts/publish-workspace.sh 0.1.0        # explicit version (must match manifest)
#
# Env:
#   CRAFTY_PUBLISH_DELAY_SECS=30     pause after each successful new upload
#   CRAFTY_PUBLISH_429_BUFFER_SECS=5 extra slack after a 429 retry-after time
#   CRAFTY_PUBLISH_MAX_ATTEMPTS=12   per-crate attempts before giving up
#   CARGO_REGISTRY_TOKEN             or `cargo login` credentials
#
# Resume after a partial run: re-run the same command; already-indexed crates
# are skipped. See docs/releasing.md and .cursor/skills/crafty-publishing/.

set -euo pipefail
cd "$(dirname "$0")/.."

ROOT_MANIFEST="Cargo.toml"
DELAY="${CRAFTY_PUBLISH_DELAY_SECS:-30}"
BUFFER="${CRAFTY_PUBLISH_429_BUFFER_SECS:-5}"
MAX_ATTEMPTS="${CRAFTY_PUBLISH_MAX_ATTEMPTS:-12}"
UA="crafty-publish (https://gitlab.com/lemarco/craft)"

die() { echo "error: $*" >&2; exit 1; }

log() { printf '[publish] %s\n' "$*"; }

current_version() {
    grep -m1 '^version = ' "$ROOT_MANIFEST" | sed -E 's/version = "(.*)"/\1/'
}

# Topological publish order (must match intra-workspace deps). `publish = false`
# crates are omitted.
PUBLISH_ORDER=(
    crafty-macros
    crafty-proto
    crafty-core
    crafty-storage
    crafty-net
    crafty-actor
    crafty-client
    crafty-dashboard
    crafty-http
    crafty-sim
    crafty-store-redis
    crafty-backlog-postgres
    crafty
    crafty-node
)

VERSION="${1:-$(current_version)}"
[ -n "$VERSION" ] || die "could not determine workspace version"
echo "$VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([-.].+)?$' \
    || die "version must look like X.Y.Z (got: $VERSION)"

manifest_version="$(current_version)"
[ "$manifest_version" = "$VERSION" ] \
    || die "manifest version is $manifest_version but requested $VERSION"

crate_version_on_index() {
    local name=$1 ver=$2
    curl -fsS -H "User-Agent: $UA" \
        "https://crates.io/api/v1/crates/${name}/${ver}" >/dev/null 2>&1
}

wait_until() {
    local target=$1 now wait_secs
    now=$(date +%s)
    wait_secs=$((target - now + BUFFER))
    if [ "$wait_secs" -gt 0 ]; then
        log "rate limit: sleeping ${wait_secs}s (includes ${BUFFER}s buffer)…"
        sleep "$wait_secs"
    fi
}

parse_retry_after_epoch() {
    # "Please try again after Fri, 28 Aug 2026 19:08:29 GMT and see …"
    local line=$1 when
    when=$(sed -n 's/.*try again after \(.*\) GMT and see.*/\1/p' <<<"$line")
    [ -n "$when" ] || return 1
    date -d "${when} GMT" +%s 2>/dev/null || date -u -d "${when} GMT" +%s
}

publish_one() {
    local pkg=$1 attempt=1 output retry_epoch
    while [ "$attempt" -le "$MAX_ATTEMPTS" ]; do
        log "uploading ${pkg} v${VERSION} (attempt ${attempt}/${MAX_ATTEMPTS})…"
        if output=$(cargo publish -p "$pkg" 2>&1); then
            printf '%s\n' "$output"
            return 0
        fi
        printf '%s\n' "$output" >&2

        if grep -q 'already exists on crates.io index\|already uploaded' <<<"$output"; then
            log "${pkg} v${VERSION} already indexed; skipping"
            return 0
        fi

        if grep -q '429 Too Many Requests' <<<"$output"; then
            retry_epoch=$(parse_retry_after_epoch "$output" || true)
            if [ -n "${retry_epoch:-}" ]; then
                wait_until "$retry_epoch"
            else
                log "429 without parseable retry time; sleeping ${DELAY}s…"
                sleep "$DELAY"
            fi
            attempt=$((attempt + 1))
            continue
        fi

        die "failed to publish ${pkg}: ${output}"
    done
    die "exhausted ${MAX_ATTEMPTS} attempts for ${pkg}"
}

log "publishing crafty workspace v${VERSION} (${#PUBLISH_ORDER[@]} crates, ${DELAY}s inter-crate delay)"

published=0
skipped=0
for pkg in "${PUBLISH_ORDER[@]}"; do
    if crate_version_on_index "$pkg" "$VERSION"; then
        log "skip ${pkg} v${VERSION} (already on crates.io)"
        skipped=$((skipped + 1))
        continue
    fi
    publish_one "$pkg"
    published=$((published + 1))
    if [ "$DELAY" -gt 0 ]; then
        log "waiting ${DELAY}s before next crate…"
        sleep "$DELAY"
    fi
done

log "done: ${published} uploaded, ${skipped} skipped (already indexed)"
echo "OK: crafty v${VERSION} publish complete."
