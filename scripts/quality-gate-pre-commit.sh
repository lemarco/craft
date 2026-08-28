#!/usr/bin/env bash
# Pre-commit quality gate — run manually or via individual lefthook jobs.
# Prefer: lefthook run pre-commit
#
# Doc + full test/release checks run on pre-push (see quality-gate-pre-push.sh).

set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/hook-prelude.sh
source scripts/clippy-args.sh
cargo fmt --all -- --check
bash scripts/check-shell-scripts.sh
cargo clippy --workspace --all-targets --all-features -- "${CLIPPY_ARGS[@]}"
echo ">> pre-commit gate ok"
