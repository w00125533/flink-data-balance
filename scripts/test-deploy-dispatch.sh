#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

TEST_TMP_DIR="$(mktemp -d)"
OUT_FILE="$TEST_TMP_DIR/fdb-deploy-test.out"
ERR_FILE="$TEST_TMP_DIR/fdb-deploy-test.err"
FAKE_BIN_DIR="$TEST_TMP_DIR/bin"
trap 'rm -rf "$TEST_TMP_DIR"' EXIT

mkdir -p "$FAKE_BIN_DIR"
cat > "$FAKE_BIN_DIR/docker" <<'SH'
#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "compose" ]]; then
  shift
  for arg in "$@"; do
    if [[ "$arg" == "config" ]]; then
      exit 0
    fi
  done
fi

echo "unexpected docker invocation: docker $*" >&2
exit 1
SH
chmod +x "$FAKE_BIN_DIR/docker"
export PATH="$FAKE_BIN_DIR:$PATH"

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

tmp_repo="$TEST_TMP_DIR/no-env-repo"
mkdir -p "$tmp_repo/docker" "$tmp_repo/scripts"
cp scripts/deploy.sh "$tmp_repo/scripts/deploy.sh"
cat > "$tmp_repo/docker/docker-compose.yml" <<'YAML'
services:
  noop:
    image: busybox
    profiles:
      - e2e
YAML

run_expect_success "external check missing default env file" env -u FDB_ENV_FILE bash "$tmp_repo/scripts/deploy.sh" external-yarn check
grep -F "[WARN] optional env file not found: .env" "$ERR_FILE" \
  || fail "external check should warn when default env file is missing"

local_tmp_env="$TEST_TMP_DIR/fdb-local-test.env"
cat > "$local_tmp_env" <<'ENV'
FDB_DEPLOY_TARGET=local
ENV

FDB_ENV_FILE="$local_tmp_env" run_expect_success "local check env file" bash scripts/deploy.sh local check
grep -F "[OK] loaded env file: $local_tmp_env" "$OUT_FILE" \
  || fail "local check should load temp env file"

missing_local_env="$TEST_TMP_DIR/missing-local.env"
FDB_ENV_FILE="$missing_local_env" run_expect_success "local check missing explicit env file" bash scripts/deploy.sh local check
grep -F "[WARN] optional env file not found: $missing_local_env" "$ERR_FILE" \
  || fail "local check should warn when explicit env file is missing"

run_expect_success "local check missing default env file" \
  env -u FDB_ENV_FILE bash -c 'cd "$1" && bash scripts/deploy.sh local check' bash "$tmp_repo"
grep -F "[WARN] optional env file not found: .env" "$ERR_FILE" \
  || fail "local check should warn when default env file is missing"

echo "[test-ok] deploy dispatch"
