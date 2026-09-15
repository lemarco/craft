#!/usr/bin/env bash
# Enforce architecture-style: trembita-core and trembita-proto stay I/O-free.
#
# Usage: ./scripts/check-core-purity.sh

set -euo pipefail
cd "$(dirname "$0")/.."

FORBIDDEN='^(tokio|quinn|h3|redb|hyper|reqwest|sqlx|opentelemetry)'

check_crate() {
  local crate=$1
  local manifest="crates/${crate}/Cargo.toml"
  [[ -f "$manifest" ]] || {
    echo "error: missing $manifest" >&2
    exit 1
  }
  local bad
  bad=$(grep -E '^\w' "$manifest" | grep -E "$FORBIDDEN" || true)
  if [[ -n "$bad" ]]; then
    echo "error: $crate must not depend on I/O/runtime crates:" >&2
    echo "$bad" >&2
    exit 1
  fi
}

check_crate trembita-core
check_crate trembita-proto

# Source-level guard: no async runtime imports in core.
if rg -q 'use (tokio|std::net|std::fs::|quinn::)' crates/trembita-core/src crates/trembita-proto/src 2>/dev/null; then
  echo "error: trembita-core/trembita-proto source must not import I/O or tokio" >&2
  rg 'use (tokio|std::net|std::fs::|quinn::)' crates/trembita-core/src crates/trembita-proto/src >&2 || true
  exit 1
fi

printf '[%s] >> core purity ok\n' "$(date -Is)" >&2
