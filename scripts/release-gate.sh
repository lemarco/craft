#!/usr/bin/env bash
# Pre-release gate — push tier + MSRV strict + full-workspace autofix.
#
# Usage: ./scripts/release-gate.sh [--release-build]

set -euo pipefail
exec bash scripts/gate.sh --tier release "$@"
