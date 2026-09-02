#!/usr/bin/env bash
# Shared prelude for hook jobs — cargo on PATH, no parallel cargo lock.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

export RUSTFLAGS="${RUSTFLAGS:--D warnings}"
export RUSTDOCFLAGS="${RUSTDOCFLAGS:--D warnings}"
export PATH="${HOME}/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:${PATH}"
export CARGO_TERM_COLOR="${CARGO_TERM_COLOR:-always}"
if [[ -t 2 ]]; then
  export CARGO_TERM_PROGRESS_WHEN="${CARGO_TERM_PROGRESS_WHEN:-auto}"
fi

./scripts/cargo-wait-lock.sh
