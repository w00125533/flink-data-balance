#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
export FDB_RETENTION_MAINTENANCE_SOURCE_ONLY=1
# shellcheck source=scripts/retention-maintenance.sh
source "$ROOT_DIR/scripts/retention-maintenance.sh"

fail_test() {
  echo "[test-fail] $*" >&2
  exit 1
}

expect_safe_runs_dir() {
  local requested=$1
  local expected=$2
  OBSERVABILITY_RUNS_DIR="$requested"
  local actual
  actual="$(safe_observability_runs_dir)"
  if [ "$actual" != "$expected" ]; then
    fail_test "unexpected safe runs dir for $requested: $actual"
  fi
}

expect_rejected_runs_dir() {
  local requested=$1
  if (OBSERVABILITY_RUNS_DIR="$requested"; safe_observability_runs_dir >/dev/null 2>&1); then
    fail_test "expected runs dir to be rejected: $requested"
  fi
}

expect_preflight_rejects_runs_dir_before_docker() {
  local output
  docker() {
    fail_test "docker must not be called before unsafe runs dir is rejected"
  }
  export -f docker

  if output="$(OBSERVABILITY_RUNS_DIR=".." preflight 2>&1)"; then
    fail_test "expected preflight to reject unsafe runs dir"
  fi
  if ! printf '%s\n' "$output" | grep -q "FDB_E2E_RUNS_DIR"; then
    fail_test "preflight rejection did not mention FDB_E2E_RUNS_DIR: $output"
  fi
}

base="$(canonicalize_path "$ROOT_DIR/docker/data/observability-runs")"
expect_safe_runs_dir "docker/data/observability-runs" "$base"
expect_safe_runs_dir "docker/data/observability-runs/run-1" "$base/run-1"
expect_safe_runs_dir "$base/run-2" "$base/run-2"

expect_rejected_runs_dir "../observability-runs"
expect_rejected_runs_dir "docker/data/observability-runs/../outside"
expect_rejected_runs_dir "docker/data"
expect_rejected_runs_dir "$ROOT_DIR"
expect_rejected_runs_dir "/"
expect_preflight_rejects_runs_dir_before_docker

echo "[test-ok] retention maintenance helpers"
