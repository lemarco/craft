#!/usr/bin/env bash
# Shared prelude for hook jobs — cargo on PATH, no parallel cargo lock.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

export RUSTFLAGS="${RUSTFLAGS:--D warnings}"
export PATH="${HOME}/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:${PATH}"

./scripts/cargo-wait-lock.sh
