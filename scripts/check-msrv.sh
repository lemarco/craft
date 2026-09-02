#!/usr/bin/env bash
# MSRV gate — mirrors .gitlab-ci.yml `msrv` job (library-and-publishing).
#
# Env:
#   TREMBITA_MSRV=1.90           — toolchain version (default from workspace)
#   TREMBITA_MSRV_STRICT=1       — fail when rustup/toolchain missing (release gate)

set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/hook-prelude.sh

MSRV="${TREMBITA_MSRV:-1.90}"
STRICT="${TREMBITA_MSRV_STRICT:-0}"

fail_or_warn() {
  if [[ "$STRICT" == "1" ]]; then
    echo "error: $*" >&2
    exit 1
  fi
  echo "warn: $*" >&2
  exit 0
}

if ! command -v rustup >/dev/null 2>&1; then
  fail_or_warn "rustup not found — skipping MSRV ${MSRV} check (install rustup or set TREMBITA_MSRV_STRICT=0)"
fi

if ! rustup toolchain list 2>/dev/null | grep -qE "^${MSRV}(-.*)?\$"; then
  fail_or_warn "Rust ${MSRV} toolchain not installed — skipping MSRV check (install: rustup toolchain install ${MSRV})"
fi

rustup run "${MSRV}" cargo check --workspace --all-features
