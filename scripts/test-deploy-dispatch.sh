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
FAKE_RM_LOG="$TEST_TMP_DIR/hdfs-rm.log"
export FAKE_RM_LOG
FAKE_CURL_LOG="$TEST_TMP_DIR/curl.log"
export FAKE_CURL_LOG
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
  if [[ "$*" == *" up -d --no-deps --force-recreate observability-api"* ]]; then
    exit 0
  fi
  if [[ "$*" == *" exec -T starrocks-fe mysql "* ]]; then
    cat >/dev/null
    exit 0
  fi
  if [[ "$*" == *" exec -T namenode "* ]]; then
    if [[ "$*" == *" -ls -R "* ]]; then
      if [[ "$*" == *" /warehouse/iceberg/iceberg_db/cell_kpi"* ]]; then
        cat <<'OUT'
-rw-r--r--   3 flink supergroup       1000 2026-07-10 00:00 /warehouse/iceberg/iceberg_db/cell_kpi/.metadata.inprogress.token
OUT
      else
        cat <<'OUT'
-rw-r--r--   3 flink supergroup       1000 2026-07-10 00:00 /warehouse/fdb/cell_kpi/old.parquet
-rw-r--r--   3 flink supergroup       1000 2026-07-10 00:10 /warehouse/fdb/cell_kpi/old-2.parquet
-rw-r--r--   3 flink supergroup       1000 2099-07-14 00:00 /warehouse/fdb/cell_kpi/new.parquet
-rw-r--r--   3 flink supergroup       1000 2026-07-10 00:00 /warehouse/fdb/cell_kpi/.old.part.parquet.inprogress.token
-rw-r--r--   3 flink supergroup       1000 2099-07-14 00:00 /warehouse/fdb/cell_kpi/.recent.part.parquet.inprogress.token
OUT
      fi
      exit 0
    fi
    if [[ "$*" == *" -find "* ]]; then
      exit 0
    fi
    if [[ "$*" == *" -rm -f "* ]]; then
      printf '%s\n' "$*" >> "${FAKE_RM_LOG:?}"
      exit 0
    fi
  fi
fi

if [[ "${1:-}" == "exec" ]]; then
  if [[ "$*" == *" fdb-flink-jobmanager "* ]]; then
    if [[ "$*" == *" flink run "* ]]; then
      echo "JobID local-job-${RANDOM}"
      exit 0
    fi
    [[ "$*" == *" flink cancel "* ]] && exit 0
  fi

  shift
  container="${1:-}"
  shift || true
  if [[ "$container" == "shared-data-infra-starrocks-fe-1" ]]; then
    cat >/dev/null
    exit 0
  fi
  if [[ "$container" == "shared-data-infra-namenode-1" ]]; then
    if [[ "$*" == *" -ls -R "* ]]; then
      if [[ "$*" == *" /warehouse/iceberg/iceberg_db/cell_kpi"* ]]; then
        cat <<'OUT'
-rw-r--r--   3 flink supergroup       1000 2026-07-10 00:00 /warehouse/iceberg/iceberg_db/cell_kpi/.metadata.inprogress.token
OUT
      else
        cat <<'OUT'
-rw-r--r--   3 flink supergroup       1000 2026-07-10 00:00 /warehouse/fdb/cell_kpi/old.parquet
-rw-r--r--   3 flink supergroup       1000 2026-07-10 00:10 /warehouse/fdb/cell_kpi/old-2.parquet
-rw-r--r--   3 flink supergroup       1000 2099-07-14 00:00 /warehouse/fdb/cell_kpi/new.parquet
-rw-r--r--   3 flink supergroup       1000 2026-07-10 00:00 /warehouse/fdb/cell_kpi/.old.part.parquet.inprogress.token
-rw-r--r--   3 flink supergroup       1000 2099-07-14 00:00 /warehouse/fdb/cell_kpi/.recent.part.parquet.inprogress.token
OUT
      fi
      exit 0
    fi
    if [[ "$*" == *" -find "* ]]; then
      exit 0
    fi
    if [[ "$*" == *" -rm -f "* ]]; then
      printf '%s\n' "$*" >> "${FAKE_RM_LOG:?}"
      exit 0
    fi
  fi
fi

echo "unexpected docker invocation: docker $*" >&2
exit 1
SH
chmod +x "$FAKE_BIN_DIR/docker"
cat > "$FAKE_BIN_DIR/curl" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${FAKE_CURL_LOG:?}"
echo '{"ok":true}'
SH
chmod +x "$FAKE_BIN_DIR/curl"
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

FDB_PRUNE_DRY_RUN=1 run_expect_success "local prune dry run" bash scripts/deploy.sh local prune
grep -F "[INFO] prune dry run" "$OUT_FILE" \
  || fail "local prune dry run should describe planned cleanup"

run_expect_success "local prune removes old HDFS parquet without hdfs find mtime" env \
  FDB_ENV_FILE="$TEST_TMP_DIR/missing.env" \
  FDB_HDFS_KPI_RETENTION_MS=172800000 \
  FDB_HDFS_INPROGRESS_RETENTION_MS=172800000 \
  bash scripts/deploy.sh local prune
grep -F "/warehouse/fdb/cell_kpi/old.parquet" "$FAKE_RM_LOG" \
  || fail "local prune should remove old HDFS parquet from ls timestamp fallback"
grep -F "/warehouse/fdb/cell_kpi/old-2.parquet" "$FAKE_RM_LOG" \
  || fail "local prune should remove every old HDFS parquet from one listing"
if grep -F "/warehouse/fdb/cell_kpi/new.parquet" "$FAKE_RM_LOG" >/dev/null; then
  fail "local prune should not remove recent HDFS parquet"
fi
grep -F "/warehouse/fdb/cell_kpi/.old.part.parquet.inprogress.token" "$FAKE_RM_LOG" \
  || fail "local prune should remove stale HDFS in-progress files"
if grep -F "/warehouse/fdb/cell_kpi/.recent.part.parquet.inprogress.token" "$FAKE_RM_LOG" >/dev/null; then
  fail "local prune should not remove recent HDFS in-progress files"
fi

tmp_env="$TEST_TMP_DIR/fdb-test.env"
cat > "$tmp_env" <<'ENV'
FDB_DEPLOY_TARGET=external-yarn
FDB_KAFKA_BOOTSTRAP=127.0.0.1:1
FDB_HDFS_URI=hdfs://127.0.0.1:8020
FDB_HIVE_JDBC_URL=jdbc:hive2://127.0.0.1:10000/default
FDB_STARROCKS_FE_ENDPOINT=127.0.0.1:9030
FDB_STARROCKS_JDBC_URL=jdbc:mysql://127.0.0.1:9030/fdb
FDB_STARROCKS_USER=root
FDB_STARROCKS_DATABASE=fdb
ENV

FDB_ENV_FILE="$tmp_env" run_expect_success "external non-strict check" bash scripts/deploy.sh external-yarn check
grep -F "[OK] loaded env file: $tmp_env" "$OUT_FILE" \
  || fail "external non-strict check should load temp env file"
grep -Eq "\[WARN\]|\[OK\]" "$OUT_FILE" \
  || fail "external non-strict check should emit diagnostic lines"

FDB_ENV_FILE="$tmp_env" FDB_PRUNE_DRY_RUN=1 run_expect_success "external prune dry run" bash scripts/deploy.sh external-yarn prune
grep -F "[INFO] prune dry run" "$OUT_FILE" \
  || fail "external prune dry run should describe planned cleanup"

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

submit_repo="$TEST_TMP_DIR/submit-repo"
mkdir -p "$submit_repo/scripts" "$submit_repo/docker" "$submit_repo/logs"
cp scripts/deploy.sh "$submit_repo/scripts/deploy.sh"
cat > "$submit_repo/docker/docker-compose.yml" <<'YAML'
services:
  observability-api:
    image: busybox
    profiles:
      - e2e
YAML

fixed_date_bin="$TEST_TMP_DIR/fixed-date-bin"
mkdir -p "$fixed_date_bin"
cat > "$fixed_date_bin/date" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
case "$*" in
  *%Y%m%d-%H%M%S*) echo "20260714-120000" ;;
  *%Y-%m-%dT%H:%M:%SZ*) echo "2026-07-14T12:00:00Z" ;;
  *) command date "$@" ;;
esac
SH
chmod +x "$fixed_date_bin/date"

run_expect_success "first local submit generates run id" env \
  PATH="$fixed_date_bin:$PATH" \
  FDB_ENV_FILE="$submit_repo/missing.env" \
  FDB_LOCAL_STATE_FILE="$submit_repo/logs/local-current-1.env" \
  bash "$submit_repo/scripts/deploy.sh" local submit
first_run_id="$(awk -F= '/^FDB_RUN_ID=/ {print $2}' "$submit_repo/logs/local-current-1.env")"

run_expect_success "second local submit generates distinct run id" env \
  PATH="$fixed_date_bin:$PATH" \
  FDB_ENV_FILE="$submit_repo/missing.env" \
  FDB_LOCAL_STATE_FILE="$submit_repo/logs/local-current-2.env" \
  bash "$submit_repo/scripts/deploy.sh" local submit
second_run_id="$(awk -F= '/^FDB_RUN_ID=/ {print $2}' "$submit_repo/logs/local-current-2.env")"

[[ -n "$first_run_id" && -n "$second_run_id" ]] \
  || fail "local submit should write generated run ids"
[[ "$first_run_id" != "$second_run_id" ]] \
  || fail "generated run ids should differ across consecutive submits"

run_expect_failure "local report without run id or state" env \
  FDB_ENV_FILE="$submit_repo/missing.env" \
  FDB_LOCAL_STATE_FILE="$submit_repo/logs/missing-current.env" \
  bash "$submit_repo/scripts/deploy.sh" local report
grep -F "no run id found; run local submit first or set FDB_RUN_ID" "$ERR_FILE" \
  || fail "local report without run id should fail with submit guidance"

report_env="$TEST_TMP_DIR/report-on-stop.env"
cat > "$report_env" <<'ENV'
FDB_DEPLOY_TARGET=local
FDB_REPORT_ON_STOP=false
FDB_OBSERVABILITY_API_URL=http://env-file-api
ENV
report_state="$TEST_TMP_DIR/report-on-stop-current.env"
cat > "$report_state" <<'ENV'
FDB_LOCAL_FLINK_JOB_ID=local-stop-job
FDB_RUN_ID=stop-run
ENV
: > "$FAKE_CURL_LOG"
FDB_ENV_FILE="$report_env" FDB_LOCAL_STATE_FILE="$report_state" FDB_REPORT_ON_STOP=true \
  run_expect_success "local stop preserves command-line report-on-stop" bash scripts/deploy.sh local stop
grep -F "http://env-file-api/api/runs/report?runId=stop-run" "$FAKE_CURL_LOG" \
  || fail "local stop should call report when FDB_REPORT_ON_STOP=true is provided by the caller"

echo "[test-ok] deploy dispatch"
