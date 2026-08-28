#!/usr/bin/env bash
# Pre-push quality gate — run manually or via lefthook pre-push.

set -euo pipefail
cd "$(dirname "$0")/.."
source scripts/hook-prelude.sh

log() { printf '[%s] %s\n' "$(date -Is)" "$*"; }

maybe_tee() {
  if [[ -n "${CRAFT_HOOK_LOG:-}" ]]; then
    tee -a "${CRAFT_TEST_LOG:-target/test-run.log}"
  else
    cat
  fi
}

export NEXTEST_PROFILE=ci

log ">> clippy (pedantic)"
source scripts/clippy-args.sh
cargo clippy --workspace --all-targets --all-features -- "${CLIPPY_ARGS[@]}" 2>&1 | maybe_tee

log ">> tests"
if command -v cargo-nextest >/dev/null 2>&1; then
  cargo nextest run --profile ci --workspace --all-features 2>&1 | maybe_tee
else
  cargo test --workspace --all-features 2>&1 | maybe_tee
fi

log ">> doctests"
cargo test --workspace --doc --all-features 2>&1 | maybe_tee

log ">> doc"
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features 2>&1 | maybe_tee

log ">> release build"
if [[ "${CRAFT_SKIP_RELEASE:-1}" != "1" ]]; then
  cargo build --workspace --all-features --release 2>&1 | maybe_tee
else
  log ">> release build skipped (CRAFT_SKIP_RELEASE=1)"
fi

log ">> pre-push gate ok"
