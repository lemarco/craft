#!/usr/bin/env bash
# trembita workflow — resume a durable saga from the workflows showcase.
# Usage: ./scripts/trembita-workflow.sh resume <saga_id>
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cmd="${1:-}"
shift || true
case "$cmd" in
  resume)
    exec "$ROOT/scripts/run-example.sh" workflows --release -- resume "$@"
    ;;
  *)
    echo "usage: $0 resume <saga_id>" >&2
    exit 1
    ;;
esac
