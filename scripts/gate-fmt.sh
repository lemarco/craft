#!/usr/bin/env bash
# Rustfmt gate — auto-fix (hooks) or check-only (manual gates).
#
# Usage:
#   ./scripts/gate-fmt.sh          # format workspace (fix hook failures)
#   ./scripts/gate-fmt.sh --check  # verify formatting (pre-commit / CI)

set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/hook-prelude.sh

if [[ "${1:-}" == "--check" ]]; then
  cargo fmt --all -- --check
else
  cargo fmt --all
fi
