#!/usr/bin/env bash
# Full test sweep — all nextest tests + 250-case proptest (slow).
#
# Usage:
#   ./scripts/test-heavy.sh                          # default-members
#   ./scripts/test-heavy.sh --workspace --all-features

set -euo pipefail
export NEXTEST_PROFILE=default
export CRAFTY_PROptest_CASES=250
exec "$(dirname "$0")/test-fast.sh" "$@"
