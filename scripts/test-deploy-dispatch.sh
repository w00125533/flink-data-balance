#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

TEST_TMP_DIR="$(mktemp -d)"
OUT_FILE="$TEST_TMP_DIR/fdb-deploy-test.out"
ERR_FILE="$TEST_TMP_DIR/fdb-deploy-test.err"
trap 'rm -rf "$TEST_TMP_DIR"' EXIT

fail() {
  echo "[test-fail] $*" >&2
  exit 1
}

run_expect_success() {
  local description=$1
  shift
  if ! "$@" >"$OUT_FILE" 2>"$ERR_FILE"; then
    cat "$OUT_FILE" >&2 || true
    cat "$ERR_FILE" >&2 || true
    fail "$description should succeed"
  fi
}

run_expect_failure() {
  local description=$1
  shift
  if "$@" >"$OUT_FILE" 2>"$ERR_FILE"; then
    cat "$OUT_FILE" >&2 || true
    cat "$ERR_FILE" >&2 || true
    fail "$description should fail"
  fi
}

run_expect_success "usage" bash scripts/deploy.sh --help
grep -q "Usage: scripts/deploy.sh <target> <command>" "$OUT_FILE" \
  || fail "usage output should include command shape"

run_expect_failure "missing command" bash scripts/deploy.sh local
grep -q "missing command" "$ERR_FILE" \
  || fail "missing command error should be explicit"

run_expect_failure "invalid target" bash scripts/deploy.sh bad-target check
grep -q "unsupported target: bad-target" "$ERR_FILE" \
  || fail "invalid target error should be explicit"

run_expect_failure "invalid command" bash scripts/deploy.sh local invalid
grep -q "unsupported command for local: invalid" "$ERR_FILE" \
  || fail "invalid local command error should be explicit"

tmp_env="$TEST_TMP_DIR/fdb-test.env"
cat > "$tmp_env" <<'ENV'
FDB_DEPLOY_TARGET=external-yarn
FDB_KAFKA_BOOTSTRAP=127.0.0.1:1
FDB_HDFS_URI=hdfs://127.0.0.1:8020
FDB_HIVE_JDBC_URL=jdbc:hive2://127.0.0.1:10000/default
FDB_MYSQL_HOST=127.0.0.1
FDB_MYSQL_PORT=3306
FDB_STARROCKS_FE_ENDPOINT=127.0.0.1:9030
ENV

FDB_ENV_FILE="$tmp_env" run_expect_success "external non-strict check" bash scripts/deploy.sh external-yarn check
grep -F "[OK] loaded env file: $tmp_env" "$OUT_FILE" \
  || fail "external non-strict check should load temp env file"
grep -Eq "\[WARN\]|\[OK\]" "$OUT_FILE" \
  || fail "external non-strict check should emit diagnostic lines"

echo "[test-ok] deploy dispatch"
