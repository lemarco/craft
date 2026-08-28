#!/usr/bin/env bash
# crafty workflow — ops CLI stub (B-05e).
# Usage: ./scripts/crafty-workflow.sh resume <saga_id> [--data-dir PATH]
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cmd="${1:-}"
shift || true
case "$cmd" in
  resume)
    exec cargo run -q -p crafty --example workflow_resume_cli -- "$@"
    ;;
  *)
    echo "usage: $0 resume <saga_id> [--data-dir PATH]" >&2
    exit 1
    ;;
esac
