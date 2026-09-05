#!/usr/bin/env bash
# CI-only cargo/nextest tuning. Local [profile.dev] in Cargo.toml is unchanged.
#
# Sourced from GitLab CI before_script. Env vars here override dev defaults only
# on runners — never edit Cargo.toml for CI disk/link stability.

set -euo pipefail

export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"
export NEXTEST_PROFILE="${NEXTEST_PROFILE:-ci}"

if [[ -n "${CI:-}" ]]; then
  echo "ci-env: CARGO_INCREMENTAL=$CARGO_INCREMENTAL CARGO_BUILD_JOBS=$CARGO_BUILD_JOBS NEXTEST_PROFILE=$NEXTEST_PROFILE"
  echo "ci-env: CARGO_PROFILE_DEV_DEBUG=${CARGO_PROFILE_DEV_DEBUG:-<unset>} CARGO_PROFILE_DEV_SPLIT_DEBUGINFO=${CARGO_PROFILE_DEV_SPLIT_DEBUGINFO:-<unset>}"
fi
