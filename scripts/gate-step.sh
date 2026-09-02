#!/usr/bin/env bash
# Run one quality-gate step with timestamped stderr headers (hook-friendly).
#
# Usage: ./scripts/gate-step.sh <step>
#
# Steps: autofix, fmt, clippy, tests, doctests, doc, shellcheck, doc-links,
#        publish-dry-run, examples, showcase, msrv

set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/hook-prelude.sh

STEP="${1:-}"
[[ -n "$STEP" ]] || {
  echo "error: step name required" >&2
  exit 1
}

log() { printf '[%s] >> %s\n' "$(date -Is)" "$*" >&2; }

# Avoid block-buffering when lefthook captures stdout (spinner hides partial output).
run_cmd() {
  if [[ -n "${TREMBITA_HOOK_LOG:-}" ]]; then
    "$@" 2>&1 | tee -a "${TREMBITA_TEST_LOG:-target/test-run.log}"
  elif command -v stdbuf >/dev/null 2>&1; then
    stdbuf -oL -eL "$@"
  else
    "$@"
  fi
}

maybe_disk_prune() {
  if [[ "${TREMBITA_CI_DISK_PRUNE:-0}" == "1" ]]; then
    bash scripts/ci-disk-prune.sh
  fi
}

export NEXTEST_PROFILE="${NEXTEST_PROFILE:-ci}"
export CARGO_TERM_COLOR="${CARGO_TERM_COLOR:-always}"
if [[ -t 2 ]]; then
  export CARGO_TERM_PROGRESS_WHEN="${CARGO_TERM_PROGRESS_WHEN:-auto}"
fi

case "$STEP" in
  autofix)
    [[ "${TREMBITA_GATE_AUTOFIX:-0}" == "1" ]] || exit 0
    log "autofix (fmt + clippy --fix)"
    run_cmd bash scripts/gate-autofix.sh --stage
    if [[ "${TREMBITA_NO_AUTOFIX_COMMIT:-0}" == "1" ]]; then
      exit 0
    fi
    if [[ "${TREMBITA_AUTOFIX_COMMIT:-0}" == "1" ]] && ! git diff --cached --quiet; then
      log "autofix commit"
      git commit -m "chore: apply fmt/clippy autofix"
    fi
    ;;
  fmt)
    log "fmt"
    run_cmd bash scripts/gate-fmt.sh --check
    ;;
  clippy)
    log "clippy (pedantic)"
    run_cmd bash scripts/gate-clippy.sh
    ;;
  tests)
    maybe_disk_prune
    log "tests"
    if command -v cargo-nextest >/dev/null 2>&1; then
      run_cmd cargo nextest run --profile ci --workspace --all-features --lib --tests --bins
    else
      run_cmd cargo test --workspace --all-features --lib --tests --bins
    fi
    maybe_disk_prune
    ;;
  doctests)
    log "doctests"
    run_cmd cargo test --workspace --doc --all-features
    ;;
  doc)
    log "doc"
    run_cmd cargo doc --workspace --no-deps --all-features
    ;;
  shellcheck)
    log "shellcheck"
    run_cmd bash scripts/check-shell-scripts.sh
    ;;
  doc-links)
    log "doc links"
    run_cmd bash scripts/check-doc-links.sh
    ;;
  publish-dry-run)
    log "publish dry-run"
    run_cmd bash scripts/publish-dry-run.sh
    ;;
  examples)
    log "examples check"
    run_cmd bash scripts/check-examples.sh
    ;;
  showcase)
    log "trembita-tools lib + showcase client bin"
    run_cmd cargo check -p trembita-tools --bin trembita-showcase-client
    ;;
  msrv)
    log "msrv"
    run_cmd bash scripts/check-msrv.sh
    ;;
  release-build)
    log "release build"
    run_cmd cargo build --workspace --all-features --release
    ;;
  *)
    echo "error: unknown step: $STEP" >&2
    exit 1
    ;;
esac

log "${STEP} ok"
