#!/usr/bin/env bash
# MSRV gate — mirrors .gitlab-ci.yml `msrv` job (library-and-publishing).
#
# Env:
#   TREMBITA_MSRV=1.94           — toolchain version (default: workspace rust-version)
#   TREMBITA_MSRV_STRICT=1       — fail when rustup/toolchain missing (release gate)

set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/hook-prelude.sh

workspace_msrv() {
  # Prefer [workspace.package] rust-version from the root Cargo.toml.
  awk '
    /^\[workspace\.package\]/ { in_pkg = 1; next }
    /^\[/ { in_pkg = 0 }
    in_pkg && /^rust-version[[:space:]]*=/ {
      if (match($0, /"[0-9]+\.[0-9]+(\.[0-9]+)?"/)) {
        print substr($0, RSTART + 1, RLENGTH - 2)
        exit
      }
    }
  ' Cargo.toml
}

MSRV="${TREMBITA_MSRV:-$(workspace_msrv)}"
MSRV="${MSRV:-1.94}"
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
