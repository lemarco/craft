#!/usr/bin/env bash
# Shared clippy lint set for hooks, scripts, and CI.
#
# Do not combine `-W clippy::pedantic` with `-D warnings`: that denies the
# entire pedantic group. Opt in per crate with `#![warn(clippy::pedantic)]`.
# shellcheck disable=SC2034
CLIPPY_ARGS=(-D warnings)
