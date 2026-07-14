#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "$ROOT_DIR/scripts/e2e-summary-lib.sh"

fail() {
  echo "[test-fail] $*" >&2
  exit 1
}

test_runs_dir="$(mktemp -d)"
trap 'rm -rf "$test_runs_dir"' EXIT
export FDB_E2E_RUNS_DIR="$test_runs_dir"
export FDB_E2E_RUN_ID="default-test-run"

unset FDB_E2E_SUMMARY || true
if summary_enabled; then
  fail "summary should be disabled by default"
fi

for value in 1 true TRUE yes on; do
  FDB_E2E_SUMMARY="$value"
  if ! summary_enabled; then
    fail "summary should be enabled for FDB_E2E_SUMMARY=$value"
  fi
done

for value in 0 false no off ""; do
  FDB_E2E_SUMMARY="$value"
  if summary_enabled; then
    fail "summary should be disabled for FDB_E2E_SUMMARY=$value"
  fi
done

unset FDB_E2E_KEEP_RUNNING_ON_SUCCESS || true
if e2e_keep_running_on_success; then
  fail "keep-running-on-success should be disabled by default"
fi

for value in 1 true TRUE yes on; do
  FDB_E2E_KEEP_RUNNING_ON_SUCCESS="$value"
  if ! e2e_keep_running_on_success; then
    fail "keep-running-on-success should be enabled for FDB_E2E_KEEP_RUNNING_ON_SUCCESS=$value"
  fi
done

for value in 0 false no off ""; do
  FDB_E2E_KEEP_RUNNING_ON_SUCCESS="$value"
  if e2e_keep_running_on_success; then
    fail "keep-running-on-success should be disabled for FDB_E2E_KEEP_RUNNING_ON_SUCCESS=$value"
  fi
done

unset FDB_E2E_KEEP_RUNNING_ON_SUCCESS || true
unset FDB_PROMETHEUS_URL || true
unset FDB_PROMETHEUS_PORT || true
unset FDB_OBSERVABILITY_URL || true
links="$(observability_links)"
if ! printf '%s\n' "$links" | grep -q 'Prometheus | http://localhost:19090'; then
  fail "observability_links should include the default shared Prometheus URL"
fi
if ! printf '%s\n' "$links" | grep -q 'Observability API metrics | http://localhost:18080/metrics'; then
  fail "observability_links should include the default observability API metrics URL"
fi

curl() {
  printf '%s\n' '[{"sinkName":"kafka-kpi-1m","dataset":"kpi_1m","records":0},{"sinkName":"hive-kpi-1m","dataset":"kpi_1m","records":9}]'
}
if ! assert_sink_latency_runtime_samples "kpi_1m"; then
  fail "sink latency assertion should pass when runtime records are present"
fi

curl() {
  printf '%s\n' '[{"sinkName":"kafka-kpi-1m","dataset":"kpi_1m","records":0},{"sinkName":"hive-kpi-5m","dataset":"kpi_5m","records":12}]'
}
if assert_sink_latency_runtime_samples "kpi_1m"; then
  fail "sink latency assertion should reject seed-only samples for the requested dataset"
fi
unset -f curl

FDB_PROMETHEUS_PORT=19091
links="$(observability_links)"
if ! printf '%s\n' "$links" | grep -q 'Prometheus | http://localhost:19091'; then
  fail "observability_links should honor FDB_PROMETHEUS_PORT"
fi
unset FDB_PROMETHEUS_PORT

line="$(summary_line "Kafka input" "chr-events records" "128")"
if [ "$line" != "[summary] Kafka input | chr-events records | 128" ]; then
  fail "unexpected summary line: $line"
fi

if ! printf '%s\n' "$(summary_file)" | grep -q '/default-test-run/logs-summary.log'; then
  fail "unexpected default summary file: $(summary_file)"
fi

summary_test_file="$(mktemp)"
rm -f "$summary_test_file"
FDB_E2E_SUMMARY=1
FDB_E2E_SUMMARY_FILE="$summary_test_file"
FDB_E2E_RUN_ID="test-run"
summary_init >/dev/null
summary_line "Test" "records" "12" >/dev/null
summary_finalize 0 >/dev/null
if ! grep -q '\[summary\] Output | file | ' "$summary_test_file"; then
  fail "summary_init should write the output file line"
fi
if ! grep -q '\[summary\] Test | records | 12' "$summary_test_file"; then
  fail "summary_line should append to the configured summary file"
fi
if ! grep -q '"status": "success"' "$test_runs_dir/test-run/meta.json"; then
  fail "summary_finalize should persist success status"
fi
rm -f "$summary_test_file"
unset FDB_E2E_SUMMARY_FILE
unset FDB_E2E_RUN_ID
FDB_E2E_SUMMARY=

if [ "$(summary_extract_last_integer $'noise\n42\nmore')" != "42" ]; then
  fail "summary_extract_last_integer should return the last standalone integer"
fi

if [ "$(summary_extract_last_integer $'| 9913 |\nnoise')" != "9913" ]; then
  fail "summary_extract_last_integer should parse beeline table rows"
fi

if [ "$(summary_extract_last_integer "no numeric row")" != "unavailable" ]; then
  fail "summary_extract_last_integer should be safe when no integer exists"
fi

if [ "$(summary_count_hive_partitions $'window_kind=MIN_1/dt=2026-06-02/hour=09\nnoise')" != "1" ]; then
  fail "summary_count_hive_partitions should count Hive partition rows"
fi

if [ "$(summary_count_hive_partitions $'| window_kind=MIN_1/dt=2026-06-02/hour=09 |\nnoise')" != "1" ]; then
  fail "summary_count_hive_partitions should parse beeline partition rows"
fi

if [ "$(summary_count_hive_partitions "no partitions")" != "0" ]; then
  fail "summary_count_hive_partitions should be safe when no partitions exist"
fi

curl() {
  if [ "${TEST_FLINK_CURL_STATUS:-0}" -ne 0 ]; then
    return "$TEST_FLINK_CURL_STATUS"
  fi
  printf '%s\n' "${TEST_FLINK_PLAN_RESPONSE:-}"
}

if [ "$(type -t assert_flink_dynamic_balancing_vertices_absent || true)" != "function" ]; then
  fail "assert_flink_dynamic_balancing_vertices_absent should be defined"
fi

TEST_FLINK_PLAN_RESPONSE='{"plan":"cfg-source -> kpi-1m -> cell-kpi-iceberg-sink"}'
if ! assert_flink_dynamic_balancing_vertices_absent "test-job-id" >/dev/null; then
  fail "default Flink DAG assertion should pass when dynamic balancing vertices are absent"
fi

TEST_FLINK_PLAN_RESPONSE='{"plan":"chr-source -> routing-assigner -> vbucket-load-meter -> load-coordinator"}'
if assert_flink_dynamic_balancing_vertices_absent "test-job-id" >/dev/null 2>&1; then
  fail "default Flink DAG assertion should fail when dynamic balancing vertices are present"
fi

FDB_E2E_SUMMARY=1
summary_test_file="$(mktemp)"
FDB_E2E_SUMMARY_FILE="$summary_test_file"
TEST_FLINK_CURL_STATUS=0

if ! summary_output="$(bash -euo pipefail -c '
  source "$1"
  export FDB_E2E_SUMMARY=1
  export FDB_E2E_SUMMARY_FILE="$2"
  curl() {
    printf "%s\n" "{\"plan\":\"cfg-source -> kpi-1m -> cell-kpi-iceberg-sink\"}"
  }
  summary_flink_dynamic_balancing_vertices "test-job-id"
' bash "$ROOT_DIR/scripts/e2e-summary-lib.sh" "$summary_test_file")"; then
  fail "summary_flink_dynamic_balancing_vertices should not abort under pipefail when no vertices match"
fi
if ! printf '%s\n' "$summary_output" | grep -q '\[summary\] Flink DAG | dynamic balancing vertices | absent'; then
  fail "summary_flink_dynamic_balancing_vertices should emit absent when no vertices match"
fi

TEST_FLINK_PLAN_RESPONSE='{"plan":"chr-source -> routing-assigner -> vbucket-load-meter -> load-coordinator"}'
summary_output="$(summary_flink_dynamic_balancing_vertices "test-job-id")"
if ! printf '%s\n' "$summary_output" | grep -q '\[summary\] Flink DAG | dynamic balancing vertices | present:load-coordinator,routing-assigner,vbucket-load-meter'; then
  fail "summary_flink_dynamic_balancing_vertices should emit sorted present vertices"
fi

TEST_FLINK_CURL_STATUS=22
summary_output="$(summary_flink_dynamic_balancing_vertices "test-job-id")"
if ! printf '%s\n' "$summary_output" | grep -q '\[summary\] Flink DAG | dynamic balancing vertices | unavailable'; then
  fail "summary_flink_dynamic_balancing_vertices should emit unavailable when the Flink plan cannot be fetched"
fi

rm -f "$summary_test_file"
unset FDB_E2E_SUMMARY_FILE
unset TEST_FLINK_PLAN_RESPONSE
unset TEST_FLINK_CURL_STATUS
unset -f curl

echo "[test-ok] e2e summary helpers"
