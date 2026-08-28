#!/usr/bin/env bash
# MSRV gate — mirrors .gitlab-ci.yml `msrv` job (library-and-publishing).

set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/hook-prelude.sh

MSRV="${CRAFTY_MSRV:-1.90}"

if ! command -v rustup >/dev/null 2>&1; then
  echo "warn: rustup not found — skipping MSRV ${MSRV} check" >&2
  exit 0
fi

if ! rustup toolchain list --installed 2>/dev/null | grep -qE "^${MSRV}(-.*)?\$"; then
  echo "warn: Rust ${MSRV} toolchain not installed — skipping MSRV check" >&2
  echo "       install: rustup toolchain install ${MSRV}" >&2
  exit 0
fi

rustup run "${MSRV}" cargo check --workspace --all-features
