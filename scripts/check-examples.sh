#!/usr/bin/env bash
# Compile all product showcases (excluded from workspace default-members).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
log() { printf '[%s] %s\n' "$(date -Is)" "$*"; }

for dir in background-jobs stateful-workers realtime workflows self-update; do
    log ">> check examples/$dir"
    cargo check --manifest-path "$ROOT/examples/$dir/Cargo.toml"
done

log ">> examples check ok"
