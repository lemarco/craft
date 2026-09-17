#!/usr/bin/env bash
# B-48 — stop-safe export of TREMBITA_DATA_DIR (+ optional cert dir) via trembita-ops.
#
# Usage:
#   ./scripts/backup-data-dir.sh --data-dir /var/lib/myapp/data --archive /tmp/backup.tar.gz
#   ./scripts/backup-data-dir.sh --data-dir "$TREMBITA_DATA_DIR" --archive out.tar.gz --cert-dir "$TREMBITA_CERT_DIR"
#
# Requires the node process stopped (systemctl stop / no listener on TREMBITA_LISTEN).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OPS="${TREMBITA_OPS:-$ROOT/target/debug/trembita-ops}"

DATA_DIR=""
ARCHIVE=""
CERT_DIR=""
SKIP_RUNNING_CHECK=0

usage() {
  sed -n '2,8p' "$0" >&2
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-dir) DATA_DIR="${2:?}"; shift 2 ;;
    --archive) ARCHIVE="${2:?}"; shift 2 ;;
    --cert-dir) CERT_DIR="${2:?}"; shift 2 ;;
    --skip-running-check) SKIP_RUNNING_CHECK=1; shift ;;
    -h|--help) usage ;;
    *) echo "unknown arg: $1" >&2; usage ;;
  esac
done

if [[ -z "$DATA_DIR" || -z "$ARCHIVE" ]]; then
  echo "error: --data-dir and --archive are required" >&2
  usage
fi

if [[ ! -d "$DATA_DIR" ]]; then
  echo "error: data-dir not found: $DATA_DIR" >&2
  exit 1
fi

if [[ ! -x "$OPS" ]]; then
  echo "building trembita-ops …" >&2
  cargo build -q -p trembita-tools --bin trembita-ops --manifest-path "$ROOT/Cargo.toml"
fi

if [[ "$SKIP_RUNNING_CHECK" -eq 0 ]]; then
  if command -v systemctl >/dev/null 2>&1; then
    for unit in myapp trembita trembita-node; do
      if systemctl is-active --quiet "$unit" 2>/dev/null; then
        echo "error: $unit is active — stop before backup (or pass --skip-running-check)" >&2
        exit 1
      fi
    done
  fi
fi

mkdir -p "$(dirname "$ARCHIVE")"
"$OPS" backup export --data-dir "$DATA_DIR" --archive "$ARCHIVE"
echo "exported data_dir -> $ARCHIVE"

if [[ -n "$CERT_DIR" ]]; then
  if [[ ! -d "$CERT_DIR" ]]; then
    echo "error: cert-dir not found: $CERT_DIR" >&2
    exit 1
  fi
  cert_archive="${ARCHIVE%.tar.gz}-certs.tar.gz"
  if [[ "$cert_archive" == "$ARCHIVE" ]]; then
    cert_archive="${ARCHIVE}.certs.tar.gz"
  fi
  tar -czf "$cert_archive" -C "$CERT_DIR" .
  echo "exported cert_dir -> $cert_archive"
fi
