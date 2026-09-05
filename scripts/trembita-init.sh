#!/usr/bin/env bash
# trembita-init.sh — scaffold a new trembita product app (B-06).
#
# Usage: ./scripts/trembita-init.sh my-service [features]
#
# Features default to jobs,gateway,telemetry.
# See docs/decisions/framework-conventions.md

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NAME="${1:-}"
FEATURES="${2:-jobs,gateway,telemetry}"

die() { echo "error: $*" >&2; exit 1; }

[ -n "$NAME" ] || die "usage: $0 <project-name> [features]"

cargo run --manifest-path "$ROOT/crates/trembita-cli/Cargo.toml" \
    --bin trembita -- \
    new "$NAME" --features "$FEATURES" --trembita-path "$ROOT"
