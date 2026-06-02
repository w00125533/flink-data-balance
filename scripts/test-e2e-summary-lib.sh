#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "$ROOT_DIR/scripts/e2e-summary-lib.sh"

fail() {
  echo "[test-fail] $*" >&2
  exit 1
}

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

line="$(summary_line "Kafka input" "chr-events records" "128")"
if [ "$line" != "[summary] Kafka input | chr-events records | 128" ]; then
  fail "unexpected summary line: $line"
fi

if [ "$(summary_file)" != "logs-summary.log" ]; then
  fail "unexpected default summary file: $(summary_file)"
fi

summary_test_file="$(mktemp)"
rm -f "$summary_test_file"
FDB_E2E_SUMMARY=1
FDB_E2E_SUMMARY_FILE="$summary_test_file"
summary_init >/dev/null
summary_line "Test" "records" "12" >/dev/null
if ! grep -q '\[summary\] Output | file | ' "$summary_test_file"; then
  fail "summary_init should write the output file line"
fi
if ! grep -q '\[summary\] Test | records | 12' "$summary_test_file"; then
  fail "summary_line should append to the configured summary file"
fi
rm -f "$summary_test_file"
unset FDB_E2E_SUMMARY_FILE
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

echo "[test-ok] e2e summary helpers"
