#!/usr/bin/env bash
# Register (or re-register) the local GitLab runner. Idempotent: skips if config exists.
set -euo pipefail

cd "$(dirname "$0")"

if [[ ! -f .env ]]; then
  echo "Missing .env — copy .env.example and set GITLAB_RUNNER_TOKEN." >&2
  exit 1
fi

# shellcheck disable=SC1091
source .env

: "${GITLAB_URL:?GITLAB_URL required in .env}"
: "${GITLAB_RUNNER_TOKEN:?GITLAB_RUNNER_TOKEN required in .env}"
RUNNER_NAME="${RUNNER_NAME:-crafty-local-$(uname -n)}"

mkdir -p config

if [[ -f config/config.toml ]] && grep -q '^\[\[runners\]\]' config/config.toml; then
  echo "config/config.toml already has a runner — delete config/ to re-register."
  exit 0
fi

echo "Registering runner '${RUNNER_NAME}' …"

docker run --rm \
  -v "$(pwd)/config:/etc/gitlab-runner" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  gitlab/gitlab-runner:latest register \
  --non-interactive \
  --url "${GITLAB_URL}" \
  --token "${GITLAB_RUNNER_TOKEN}" \
  --executor docker \
  --docker-image rust:latest \
  --description "${RUNNER_NAME}" \
  --docker-volumes /var/run/docker.sock:/var/run/docker.sock \
  --docker-volumes /cache \
  --docker-privileged=true

echo "Registered. Start with: docker compose up -d"
