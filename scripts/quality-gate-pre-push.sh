#!/usr/bin/env bash
# Pre-push quality gate — run manually or via lefthook pre-push.
# Aligned with ci-fast-lane.sh + examples, showcase, MSRV, optional release build.
#
# Env: TREMBITA_GATE_AUTOFIX, TREMBITA_AUTOFIX_COMMIT, TREMBITA_NO_AUTOFIX_COMMIT,
#      TREMBITA_SKIP_RELEASE, TREMBITA_HOOK_LOG

set -euo pipefail
exec bash scripts/gate.sh --tier push "$@"
