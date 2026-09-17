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

restore_manifest() {
    local backup=$1 path=$2
    if [ -n "$backup" ] && [ -f "$backup" ]; then
        cp "$backup" "$path"
        rm -f "$backup"
    fi
}

# Match publish-workspace.sh: tarball must not list workspace-only dev-deps.
prepare_leaf_manifest() {
    local pkg=$1
    local manifest_backup="" manifest_path=""
    case "$pkg" in
        trembita)
            manifest_path="crates/trembita/Cargo.toml"
            manifest_backup=$(mktemp)
            cp "$manifest_path" "$manifest_backup"
            sed -i '/trembita-test-facade/d; /trembita-test-runtime/d' "$manifest_path"
            ;;
        trembita-cli)
            manifest_path="crates/trembita-cli/Cargo.toml"
            manifest_backup=$(mktemp)
            cp "$manifest_path" "$manifest_backup"
            sed -i '/^trembita-cli\.workspace = true$/d' "$manifest_path"
            ;;
    esac
    printf '%s\n' "$manifest_backup" "$manifest_path"
}

VERSION="$(current_version)"
LEAF="trembita"

if crate_version_on_index "$LEAF" "$VERSION"; then
    echo ">> publish dry-run (leaf ${LEAF} v${VERSION} — version already on crates.io)…"
    echo "   (local API changes require a version bump before publish; see CHANGELOG.md)"
    readarray -t manifest_ctx < <(prepare_leaf_manifest "$LEAF")
    manifest_backup="${manifest_ctx[0]:-}"
    manifest_path="${manifest_ctx[1]:-}"
    trap 'restore_manifest "$manifest_backup" "$manifest_path"' EXIT
    publish_args=(--dry-run)
    if [ -n "$manifest_backup" ]; then
        publish_args+=(--allow-dirty)
    fi
    if ! output="$(cargo publish -p "$LEAF" "${publish_args[@]}" 2>&1)"; then
        printf '%s\n' "$output" >&2
        echo "error: publish dry-run failed for ${LEAF} v${VERSION}." >&2
        echo "hint: bump [workspace.package] version (e.g. ./scripts/release.sh 0.3.0) —" >&2
        echo "      v${VERSION} is already on crates.io and cannot be overwritten." >&2
        exit 1
    fi
    restore_manifest "$manifest_backup" "$manifest_path"
    trap - EXIT
else
    echo ">> publish dry-run (workspace v${VERSION}, dependency order — not yet on crates.io)…"
    trembita_backup="" trembita_path=""
    cli_backup="" cli_path=""
    readarray -t trembita_ctx < <(prepare_leaf_manifest trembita)
    trembita_backup="${trembita_ctx[0]:-}"
    trembita_path="${trembita_ctx[1]:-}"
    readarray -t cli_ctx < <(prepare_leaf_manifest trembita-cli)
    cli_backup="${cli_ctx[0]:-}"
    cli_path="${cli_ctx[1]:-}"
    restore_workspace_manifests() {
        restore_manifest "$trembita_backup" "$trembita_path"
        restore_manifest "$cli_backup" "$cli_path"
    }
    trap restore_workspace_manifests EXIT
    if ! output=$(cargo publish --workspace --dry-run --allow-dirty 2>&1); then
        printf '%s\n' "$output" >&2
        echo "error: publish dry-run failed for workspace v${VERSION}." >&2
        exit 1
    fi
    restore_workspace_manifests
    trap - EXIT
    printf '%s\n' "$output"
fi

echo "OK: publish dry-run"
