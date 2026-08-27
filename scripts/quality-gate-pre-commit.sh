#!/usr/bin/env bash
# Pre-commit quality gate — run manually or via individual lefthook jobs.
# Prefer: lefthook run pre-commit

set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/hook-prelude.sh
cargo fmt --all -- --check
bash scripts/check-shell-scripts.sh
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo check --workspace --all-features
echo ">> pre-commit gate ok"
