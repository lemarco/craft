#!/usr/bin/env bash
# Report missing-doc warnings on published crates (library-and-publishing).
#
# Workspace lint enforces `missing_docs = "deny"` on published crates. Run this
# script in release prep to verify the tree is clean (same as CI).
#
# Usage:
#   ./scripts/docs-missing-audit.sh              # default-members (fast)
#   ./scripts/docs-missing-audit.sh --workspace  # all published workspace crates

set -euo pipefail
cd "$(dirname "$0")/.."
./scripts/cargo-wait-lock.sh
export PATH="${HOME}/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:${PATH}"

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
    log "... ($((count - 40)) more; re-run and pipe to a file for full list)"
  fi
  log "tip: document public items or add a targeted allow on publish=false crates"
fi

if [[ "$status" -ne 0 ]]; then
  exit "$status"
fi
