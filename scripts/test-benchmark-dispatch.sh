#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

TEST_TMP_DIR="$(mktemp -d)"
OUT_FILE="$TEST_TMP_DIR/benchmark-test.out"
ERR_FILE="$TEST_TMP_DIR/benchmark-test.err"
FAKE_BIN_DIR="$TEST_TMP_DIR/bin"
FAKE_JAVA_LOG="$TEST_TMP_DIR/java.log"
trap 'rm -rf "$TEST_TMP_DIR"' EXIT

mkdir -p "$FAKE_BIN_DIR"
cat > "$FAKE_BIN_DIR/java" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${FAKE_JAVA_LOG:?}"
SH
chmod +x "$FAKE_BIN_DIR/java"
export FAKE_JAVA_LOG

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

run_expect_success "help" bash scripts/benchmark.sh --help
grep -F "Usage: scripts/benchmark.sh <local|external-yarn>" "$OUT_FILE" \
  || fail "help should show target usage"

cat > "$TEST_TMP_DIR/empty.env" <<'ENV'
FDB_BENCHMARK_SINKS=none
ENV

run_expect_failure "missing jar" env PATH="$FAKE_BIN_DIR:$PATH" \
  FDB_ENV_FILE="$TEST_TMP_DIR/empty.env" \
  FDB_BENCHMARK_RUNNER_JAR="$TEST_TMP_DIR/missing-runner.jar" \
  bash scripts/benchmark.sh local
grep -F "benchmark-runner jar not found" "$ERR_FILE" \
  || fail "missing jar error should be explicit"

TEST_JAR="$TEST_TMP_DIR/benchmark-runner.jar"
: > "$TEST_JAR"
cat > "$TEST_TMP_DIR/test.env" <<'ENV'
FDB_BENCHMARK_SINKS=none starrocks
FDB_BENCHMARK_CELL_LEVELS=1000 3000
ENV

run_expect_success "accepts env file from runner args" env -u FDB_ENV_FILE PATH="$FAKE_BIN_DIR:$PATH" \
  FDB_BENCHMARK_RUNNER_JAR="$TEST_JAR" \
  bash scripts/benchmark.sh local --env "$TEST_TMP_DIR/test.env" --dry-run
grep -F -- "-jar $TEST_JAR local --env $TEST_TMP_DIR/test.env --dry-run" "$FAKE_JAVA_LOG" \
  || fail "benchmark.sh should honor --env from runner arguments"

run_expect_success "passes target and env file" env PATH="$FAKE_BIN_DIR:$PATH" \
  FDB_ENV_FILE="$TEST_TMP_DIR/test.env" \
  FDB_BENCHMARK_RUNNER_JAR="$TEST_JAR" \
  bash scripts/benchmark.sh local --dry-run
grep -F -- "-jar $TEST_JAR local --env $TEST_TMP_DIR/test.env --dry-run" "$FAKE_JAVA_LOG" \
  || fail "benchmark.sh should pass target, env path, and arguments to java"

echo "[test-ok] benchmark dispatch"
