#!/usr/bin/env bash
# Clippy gate (type-checks; no separate cargo check).

set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/hook-prelude.sh
source scripts/clippy-args.sh
cargo clippy --workspace --all-targets --all-features -- "${CLIPPY_ARGS[@]}"
