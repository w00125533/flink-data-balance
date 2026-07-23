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
FAKE_DOCKER_LOG="$TEST_TMP_DIR/docker.log"
export FAKE_DOCKER_LOG
FAKE_CURL_LOG="$TEST_TMP_DIR/curl.log"
export FAKE_CURL_LOG
FAKE_FLINK_RECOVERED_FILE="$TEST_TMP_DIR/flink-recovered"
export FAKE_FLINK_RECOVERED_FILE
FAKE_FLINK_RUN_COUNT_FILE="$TEST_TMP_DIR/flink-run-count"
export FAKE_FLINK_RUN_COUNT_FILE
FAKE_JAVA_LOG="$TEST_TMP_DIR/java.log"
export FAKE_JAVA_LOG
cat > "$FAKE_BIN_DIR/docker" <<'SH'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >> "${FAKE_DOCKER_LOG:?}"

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
  if [[ "$*" == *" up -d "* && "$*" == *" taskmanager"* ]]; then
    touch "${FAKE_FLINK_RECOVERED_FILE:?}"
    exit 0
  fi
  if [[ "$*" == *" exec -T starrocks-fe mysql "* ]]; then
    cat >/dev/null
    exit 0
  fi
  if [[ "$*" == *" exec -T kafka "* ]]; then
    if [[ "$*" == *" kafka-topics "*" --list"* ]]; then
      exit 0
    fi
    if [[ "$*" == *" kafka-topics "*" --describe"* ]]; then
      cat <<'OUT'
Topic: chr-events	TopicId: fake	PartitionCount: 64	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: pm-stats	TopicId: fake	PartitionCount: 16	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: cfg-config	TopicId: fake	PartitionCount: 8	ReplicationFactor: 1	Configs: cleanup.policy=compact
Topic: topology	TopicId: fake	PartitionCount: 4	ReplicationFactor: 1	Configs: cleanup.policy=compact
Topic: fdb-stage-metrics	TopicId: fake	PartitionCount: 1	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: cell-anomaly-events	TopicId: fake	PartitionCount: 16	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: user-anomaly-events	TopicId: fake	PartitionCount: 16	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: grid-anomaly-events	TopicId: fake	PartitionCount: 16	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: cell-kpi-1m	TopicId: fake	PartitionCount: 8	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: cell-kpi-5m	TopicId: fake	PartitionCount: 8	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: chr-dlq	TopicId: fake	PartitionCount: 4	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: pm-dlq	TopicId: fake	PartitionCount: 4	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: cfg-dlq	TopicId: fake	PartitionCount: 4	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: enrichment-late	TopicId: fake	PartitionCount: 4	ReplicationFactor: 1	Configs: cleanup.policy=delete
OUT
      exit 0
    fi
    if [[ "$*" == *" kafka-topics "*" --delete"* ]]; then
      exit 0
    fi
    if [[ "$*" == *" kafka-topics "*" --create"* ]]; then
      exit 0
    fi
    if [[ "$*" == *" kafka-configs "* ]]; then
      exit 0
    fi
    if [[ "$*" == *" kafka-producer-perf-test "* ]]; then
      echo "1 records sent, 1.0 records/sec"
      exit 0
    fi
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
    if [[ "$*" == *" -rm -f "* || "$*" == *" -rm -r "* ]]; then
      printf '%s\n' "$*" >> "${FAKE_RM_LOG:?}"
      exit 0
    fi
    if [[ "${FAKE_HDFS_MKDIR_SLEEP_SEC:-0}" != "0" && "$*" == *" -mkdir -p "* && "$*" == *" /warehouse/iceberg/iceberg_db/user_anomaly_events"* ]]; then
      sleep "${FAKE_HDFS_MKDIR_SLEEP_SEC:?}"
      exit 124
    fi
    if [[ "$*" == *" -mkdir -p "* || "$*" == *" -chmod -R "* ]]; then
      exit 0
    fi
  fi
fi

if [[ "${1:-}" == "exec" ]]; then
  if [[ "$*" == *" fdb-flink-jobmanager "* ]]; then
    if [[ "$*" == *" flink run "* ]]; then
      run_count=0
      if [[ -f "${FAKE_FLINK_RUN_COUNT_FILE:?}" ]]; then
        run_count="$(cat "${FAKE_FLINK_RUN_COUNT_FILE:?}")"
      fi
      run_count=$((run_count + 1))
      printf '%s\n' "$run_count" > "${FAKE_FLINK_RUN_COUNT_FILE:?}"
      if [[ "${FAKE_FLINK_RUN_FAIL_FIRST:-0}" == "1" && "$run_count" == "1" ]]; then
        exit 124
      fi
      if [[ "${FAKE_FLINK_RUN_SUPPRESS_JOB_ID:-0}" == "1" ]]; then
        exit 0
      fi
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
  if [[ "$container" == "shared-data-infra-kafka-1" ]]; then
    if [[ "$*" == *" kafka-topics "*" --list"* ]]; then
      exit 0
    fi
    if [[ "$*" == *" kafka-topics "*" --describe"* ]]; then
      cat <<'OUT'
Topic: chr-events	TopicId: fake	PartitionCount: 64	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: pm-stats	TopicId: fake	PartitionCount: 16	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: cfg-config	TopicId: fake	PartitionCount: 8	ReplicationFactor: 1	Configs: cleanup.policy=compact
Topic: topology	TopicId: fake	PartitionCount: 4	ReplicationFactor: 1	Configs: cleanup.policy=compact
Topic: fdb-stage-metrics	TopicId: fake	PartitionCount: 1	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: cell-anomaly-events	TopicId: fake	PartitionCount: 16	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: user-anomaly-events	TopicId: fake	PartitionCount: 16	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: grid-anomaly-events	TopicId: fake	PartitionCount: 16	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: cell-kpi-1m	TopicId: fake	PartitionCount: 8	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: cell-kpi-5m	TopicId: fake	PartitionCount: 8	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: chr-dlq	TopicId: fake	PartitionCount: 4	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: pm-dlq	TopicId: fake	PartitionCount: 4	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: cfg-dlq	TopicId: fake	PartitionCount: 4	ReplicationFactor: 1	Configs: cleanup.policy=delete
Topic: enrichment-late	TopicId: fake	PartitionCount: 4	ReplicationFactor: 1	Configs: cleanup.policy=delete
OUT
      exit 0
    fi
    if [[ "$*" == *" kafka-topics "*" --delete"* ]]; then
      exit 0
    fi
    if [[ "$*" == *" kafka-topics "*" --create"* ]]; then
      exit 0
    fi
    if [[ "$*" == *" kafka-configs "* ]]; then
      exit 0
    fi
    if [[ "$*" == *" kafka-producer-perf-test "* ]]; then
      echo "1 records sent, 1.0 records/sec"
      exit 0
    fi
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
    if [[ "$*" == *" -rm -f "* || "$*" == *" -rm -r "* ]]; then
      printf '%s\n' "$*" >> "${FAKE_RM_LOG:?}"
      exit 0
    fi
    if [[ "$*" == *" -mkdir -p "* || "$*" == *" -chmod -R "* ]]; then
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
if [[ "$*" == *"/jobs/"* && "$*" != *"/jobs/overview"* && "$*" != *"-X PATCH"* ]]; then
  echo '{"state":"CANCELED"}'
  exit 0
fi
if [[ "$*" == *"/overview"* ]]; then
  case "${FAKE_FLINK_OVERVIEW_MODE:-ready}" in
    no-taskmanager)
      echo '{"taskmanagers":0,"slots-available":0}'
      ;;
    busy)
      echo '{"taskmanagers":1,"slots-available":0}'
      ;;
    recover-after-taskmanager-up)
      if [[ -f "${FAKE_FLINK_RECOVERED_FILE:?}" ]]; then
        echo '{"taskmanagers":1,"slots-available":4}'
      else
        echo '{"taskmanagers":0,"slots-available":0}'
      fi
      ;;
    active-job)
      echo '{"taskmanagers":1,"slots-available":4,"jobs":[{"jid":"rest-active-job","state":"RUNNING"}]}'
      ;;
    *)
      echo '{"taskmanagers":1,"slots-available":4}'
      ;;
  esac
  exit 0
fi
echo '{"ok":true}'
SH
chmod +x "$FAKE_BIN_DIR/curl"
cat > "$FAKE_BIN_DIR/java" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${FAKE_JAVA_LOG:?}"
if [[ "$*" == *" com.fdb.benchmark.KafkaTopicResetTool"* ]]; then
  echo "[OK] fake Kafka AdminClient reset"
  exit 0
fi
echo "unexpected java invocation: java $*" >&2
exit 1
SH
chmod +x "$FAKE_BIN_DIR/java"
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

fake_benchmark_jar="$TEST_TMP_DIR/benchmark-runner.jar"
touch "$fake_benchmark_jar"
: > "$FAKE_DOCKER_LOG"
: > "$FAKE_JAVA_LOG"
FDB_ENV_FILE="$missing_local_env" FDB_BENCHMARK_RUNNER_JAR="$fake_benchmark_jar" \
  run_expect_success "local prepare resets benchmark data" bash scripts/deploy.sh local prepare
grep -F "[OK] local benchmark data prepared" "$OUT_FILE" \
  || fail "local prepare should complete data reset"
grep -F "com.fdb.benchmark.KafkaTopicResetTool" "$FAKE_JAVA_LOG" >/dev/null \
  || fail "local prepare should prefer Kafka AdminClient reset"
grep -F "kafka-producer-perf-test" "$FAKE_DOCKER_LOG" >/dev/null \
  || fail "local prepare should probe Kafka idempotent producer readiness"
if grep -F "exec shared-data-infra-kafka-1 kafka-topics" "$FAKE_DOCKER_LOG" | grep -F -- "--delete" >/dev/null; then
  fail "local prepare should not delete Kafka topics through docker exec when AdminClient reset is available"
fi
grep -F "/warehouse/fdb/cell_kpi" "$FAKE_RM_LOG" \
  || fail "local prepare should reset HDFS KPI output"

: > "$FAKE_DOCKER_LOG"
start_seconds=$SECONDS
FDB_ENV_FILE="$missing_local_env" \
  FDB_RESULT_SINK=iceberg \
  FDB_SHARED_HDFS_EXEC_TIMEOUT_SEC=1 \
  FAKE_HDFS_MKDIR_SLEEP_SEC=20 \
  run_expect_success "local prepare bounds shared HDFS exec" env \
    FDB_ENV_FILE="$missing_local_env" \
    FDB_RESULT_SINK=iceberg \
    FDB_SHARED_HDFS_EXEC_TIMEOUT_SEC=1 \
    FAKE_HDFS_MKDIR_SLEEP_SEC=20 \
    bash scripts/deploy.sh local prepare
elapsed_seconds=$((SECONDS - start_seconds))
[[ "$elapsed_seconds" -lt 15 ]] \
  || fail "local prepare should not wait indefinitely for shared HDFS compose exec; elapsed=${elapsed_seconds}s"
grep -F "exec shared-data-infra-namenode-1" "$FAKE_DOCKER_LOG" >/dev/null \
  || fail "local prepare should fall back to direct HDFS docker exec after compose timeout"

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
grep -F "http://localhost:8081/overview" "$FAKE_CURL_LOG" >/dev/null \
  || fail "local submit should wait for Flink slots before submitting"
[[ -f "$submit_repo/logs/local-flink-submit.out" ]] \
  || fail "local submit should write submit log under logs/"
[[ ! -f "$submit_repo/logs-local-flink-submit.out" ]] \
  || fail "local submit should not write submit log in repo root"

run_expect_success "local submit recovers job id from REST when submit output omits it" env \
  PATH="$fixed_date_bin:$PATH" \
  FAKE_FLINK_RUN_SUPPRESS_JOB_ID=1 \
  FAKE_FLINK_OVERVIEW_MODE=active-job \
  FDB_LOCAL_FLINK_SUBMIT_LATE_WAIT_SEC=1 \
  FDB_ENV_FILE="$submit_repo/missing.env" \
  FDB_LOCAL_STATE_FILE="$submit_repo/logs/local-current-rest-job.env" \
  bash "$submit_repo/scripts/deploy.sh" local submit
rest_job_id="$(awk -F= '/^FDB_LOCAL_FLINK_JOB_ID=/ {gsub(/'\''/, "", $2); print $2}' "$submit_repo/logs/local-current-rest-job.env")"
[[ "$rest_job_id" == "rest-active-job" ]] \
  || fail "local submit should write REST-discovered Flink job id when submit output omits it"

: > "$FAKE_DOCKER_LOG"
rm -f "$FAKE_FLINK_RUN_COUNT_FILE"
run_expect_failure "local submit does not retry unknown submit by default" env \
  PATH="$fixed_date_bin:$PATH" \
  FAKE_FLINK_RUN_FAIL_FIRST=1 \
  FDB_LOCAL_FLINK_SUBMIT_LATE_WAIT_SEC=1 \
  FDB_ENV_FILE="$submit_repo/missing.env" \
  FDB_LOCAL_STATE_FILE="$submit_repo/logs/local-current-no-retry.env" \
  bash "$submit_repo/scripts/deploy.sh" local submit
no_retry_count="$(cat "$FAKE_FLINK_RUN_COUNT_FILE")"
[[ "$no_retry_count" == "1" ]] \
  || fail "local submit should not retry unknown submit status by default"
grep -F "not retrying unknown submit status" "$ERR_FILE" >/dev/null \
  || fail "local submit should explain why unknown submit status is not retried"

: > "$FAKE_DOCKER_LOG"
rm -f "$FAKE_FLINK_RUN_COUNT_FILE"
run_expect_success "local submit retries command without recreating runtime" env \
  PATH="$fixed_date_bin:$PATH" \
  FAKE_FLINK_RUN_FAIL_FIRST=1 \
  FDB_LOCAL_FLINK_SUBMIT_RETRY_ON_UNKNOWN=1 \
  FDB_LOCAL_FLINK_SUBMIT_LATE_WAIT_SEC=1 \
  FDB_ENV_FILE="$submit_repo/missing.env" \
  FDB_LOCAL_STATE_FILE="$submit_repo/logs/local-current-retry.env" \
  bash "$submit_repo/scripts/deploy.sh" local submit
retry_job_id="$(awk -F= '/^FDB_LOCAL_FLINK_JOB_ID=/ {gsub(/'\''/, "", $2); print $2}' "$submit_repo/logs/local-current-retry.env")"
[[ "$retry_job_id" == local-job-* ]] \
  || fail "local submit should write Flink job id after lightweight retry"
if grep -F "up -d --force-recreate --no-deps jobmanager taskmanager" "$FAKE_DOCKER_LOG" >/dev/null; then
  fail "local submit should not recreate Flink runtime for a submit retry by default"
fi

: > "$FAKE_DOCKER_LOG"
rm -f "$FAKE_FLINK_RECOVERED_FILE"
run_expect_success "local submit recovers missing taskmanager" env \
  PATH="$fixed_date_bin:$PATH" \
  FAKE_FLINK_OVERVIEW_MODE=recover-after-taskmanager-up \
  FDB_FLINK_READY_WAIT_SEC=1 \
  FDB_ENV_FILE="$submit_repo/missing.env" \
  FDB_LOCAL_STATE_FILE="$submit_repo/logs/local-current-recovered.env" \
  bash "$submit_repo/scripts/deploy.sh" local submit
grep -F "up -d --force-recreate --no-deps taskmanager" "$FAKE_DOCKER_LOG" >/dev/null \
  || fail "local submit should recreate TaskManager when none is registered"

: > "$FAKE_DOCKER_LOG"
run_expect_failure "local submit does not restart busy taskmanager" env \
  PATH="$fixed_date_bin:$PATH" \
  FAKE_FLINK_OVERVIEW_MODE=busy \
  FDB_FLINK_READY_WAIT_SEC=1 \
  FDB_ENV_FILE="$submit_repo/missing.env" \
  FDB_LOCAL_STATE_FILE="$submit_repo/logs/local-current-busy.env" \
  bash "$submit_repo/scripts/deploy.sh" local submit
if grep -F "up -d --force-recreate --no-deps taskmanager" "$FAKE_DOCKER_LOG" >/dev/null; then
  fail "local submit should not restart TaskManager when slots are occupied"
fi

: > "$FAKE_DOCKER_LOG"
run_expect_failure "local submit requires enough slots for configured parallelism" env \
  PATH="$fixed_date_bin:$PATH" \
  FDB_FLINK_PARALLELISM=6 \
  FDB_FLINK_READY_WAIT_SEC=1 \
  FDB_ENV_FILE="$submit_repo/missing.env" \
  FDB_LOCAL_STATE_FILE="$submit_repo/logs/local-current-insufficient-slots.env" \
  bash "$submit_repo/scripts/deploy.sh" local submit
grep -F "required=6" "$ERR_FILE" >/dev/null \
  || fail "local submit should report required slot count"
if grep -F "flink run" "$FAKE_DOCKER_LOG" >/dev/null; then
  fail "local submit should not run Flink when available slots are below configured parallelism"
fi

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
FDB_FLINK_REST_URL=http://env-file-flink
ENV
report_state="$TEST_TMP_DIR/report-on-stop-current.env"
cat > "$report_state" <<'ENV'
FDB_LOCAL_FLINK_JOB_ID=local-stop-job
FDB_RUN_ID=stop-run
ENV
: > "$FAKE_CURL_LOG"
FDB_ENV_FILE="$report_env" FDB_LOCAL_STATE_FILE="$report_state" \
  run_expect_success "local stop uses REST cancel first" bash scripts/deploy.sh local stop
grep -F -- "-X PATCH http://env-file-flink/jobs/local-stop-job?mode=cancel" "$FAKE_CURL_LOG" >/dev/null \
  || fail "local stop should cancel through Flink REST before Docker CLI"
grep -F -- "--max-time 10" "$FAKE_CURL_LOG" >/dev/null \
  || fail "local stop REST cancel should have a bounded curl timeout"
if grep -F "http://env-file-flink/overview" "$FAKE_CURL_LOG" >/dev/null; then
  fail "local stop should not wait for Flink slots after cancel"
fi

: > "$FAKE_CURL_LOG"
FDB_ENV_FILE="$report_env" FDB_LOCAL_STATE_FILE="$report_state" FDB_REPORT_ON_STOP=true \
  run_expect_success "local stop preserves command-line report-on-stop" bash scripts/deploy.sh local stop
grep -F "http://env-file-api/api/runs/report?runId=stop-run" "$FAKE_CURL_LOG" >/dev/null \
  || fail "local stop should call report when FDB_REPORT_ON_STOP=true is provided by the caller"

echo "[test-ok] deploy dispatch"
