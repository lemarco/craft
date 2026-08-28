#!/usr/bin/env bash
# Install clang + mold required by .cargo/config.toml on Debian-based CI images.
set -euo pipefail

if [[ "$(uname -s)" != "Linux" ]]; then
  exit 0
fi

if command -v clang >/dev/null 2>&1 && command -v mold >/dev/null 2>&1; then
  exit 0
fi

if command -v apt-get >/dev/null 2>&1; then
  apt-get update -qq
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq clang mold
elif command -v apk >/dev/null 2>&1; then
  apk add --no-cache clang mold
else
  echo "ci-linux-linker-deps: unsupported Linux image (need apt-get or apk)" >&2
  exit 1
fi
