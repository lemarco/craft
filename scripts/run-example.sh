#!/usr/bin/env bash
# Run a product showcase from examples/.
# Usage: ./scripts/run-example.sh <name> [extra cargo run args…]
#
# Names: background-jobs | stateful-workers | realtime | workflows
# Aliases: background_jobs, stateful_workers, realtime_sessions
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="${1:?usage: $0 <background-jobs|stateful-workers|realtime|workflows> [cargo run args…]}"
shift

case "$NAME" in
  background-jobs|background_jobs)
    MANIFEST="$ROOT/examples/background-jobs/Cargo.toml"
    ;;
  stateful-workers|stateful_workers)
    MANIFEST="$ROOT/examples/stateful-workers/Cargo.toml"
    ;;
  realtime|realtime-sessions|realtime_sessions)
    MANIFEST="$ROOT/examples/realtime/Cargo.toml"
    ;;
  workflows)
    MANIFEST="$ROOT/examples/workflows/Cargo.toml"
    ;;
  *)
    echo "unknown showcase: $NAME" >&2
    echo "expected: background-jobs | stateful-workers | realtime | workflows" >&2
    exit 1
    ;;
esac

exec cargo run --manifest-path "$MANIFEST" "$@"
