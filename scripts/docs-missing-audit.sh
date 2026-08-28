#!/usr/bin/env bash
# Report missing-doc warnings on published crates (library-and-publishing).
#
# Workspace lints use `missing_docs = "warn"` pre-1.0; CI/hooks allow the lint
# via RUSTFLAGS `-A missing_docs` so gates stay green. Run this script locally
# or in release prep to track progress toward `#![deny(missing_docs)]` at 1.0.
#
# Usage:
#   ./scripts/docs-missing-audit.sh              # default-members (fast)
#   ./scripts/docs-missing-audit.sh --workspace  # all published workspace crates

set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/hook-prelude.sh

scope=(--workspace)
if [[ "${1:-}" != "--workspace" ]]; then
  scope=()
fi

log() { printf '[%s] %s\n' "$(date -Is)" "$*"; }

log ">> missing_docs audit (warnings not allowed in this run)"
# Override hook defaults: surface warnings, do not fail the script on them.
export RUSTFLAGS="-W missing_docs"
unset RUSTDOCFLAGS

set +e
out=$(cargo check "${scope[@]}" --all-features 2>&1)
status=$?
set -e

count=$(grep -c 'missing documentation' <<<"$out" || true)
log "missing documentation warnings: ${count}"
if [[ "$count" -gt 0 ]]; then
  grep 'missing documentation' <<<"$out" | head -40
  if [[ "$count" -gt 40 ]]; then
    log "... ($(("$count" - 40)) more; re-run and pipe to a file for full list)"
  fi
fi

exit "$status"
