#!/usr/bin/env bash
# Shared clippy lint set for hooks, scripts, and CI.
# shellcheck disable=SC2034
CLIPPY_ARGS=(-D warnings -W clippy::pedantic)
