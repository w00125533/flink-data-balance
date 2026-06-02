#!/usr/bin/env bash

summary_enabled() {
  case "${FDB_E2E_SUMMARY:-}" in
    1|true|TRUE|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

summary_file() {
  printf '%s\n' "${FDB_E2E_SUMMARY_FILE:-logs-summary.log}"
}

summary_emit() {
  local line=$1
  printf '%s\n' "$line"
  summary_enabled || return 0

  local file
  file="$(summary_file)"
  printf '%s\n' "$line" >> "$file"
}

summary_init() {
  summary_enabled || return 0

  local file
  file="$(summary_file)"
  local dir
  dir="$(dirname "$file")"
  [ "$dir" = "." ] || mkdir -p "$dir"
  : > "$file"
  summary_emit "[summary] Output | file | $file"
}

summary_line() {
  local section=$1
  local metric=$2
  local value=$3
  summary_emit "[summary] $section | $metric | $value"
}

summary_extract_last_integer() {
  local output=$1
  local value
  value="$(printf '%s\n' "$output" | awk '
    /^[[:space:]]*[0-9]+[[:space:]]*$/ {value=$1}
    /^[[:space:]]*\|[[:space:]]*[0-9]+[[:space:]]*\|[[:space:]]*$/ {
      gsub(/[|[:space:]]/, "", $0)
      value=$0
    }
    END {print value}
  ')"
  printf '%s\n' "${value:-unavailable}"
}

summary_count_hive_partitions() {
  local output=$1
  printf '%s\n' "$output" | awk 'BEGIN {count=0} /^[[:space:]]*\|?[[:space:]]*window_kind=/ {count++} END {print count}'
}

summary_section() {
  summary_enabled || return 0
  printf '\n'
  summary_emit "[summary] === $1 ==="
}

summary_command() {
  summary_enabled || return 0
  local section=$1
  local metric=$2
  local command=$3
  local value
  if value="$(eval "$command" 2>/dev/null)"; then
    value="$(printf '%s' "$value" | tr '\n' ';' | sed 's/;*$//')"
    [ -n "$value" ] || value="empty"
  else
    value="unavailable"
  fi
  summary_line "$section" "$metric" "$value"
}

summary_code_logs() {
  summary_enabled || return 0
  local section=$1
  shift
  local output
  local max_lines=${FDB_E2E_CODE_SUMMARY_LINES:-80}
  output="$("$@" 2>/dev/null | grep '\[summary-code\]' | tail -n "$max_lines" || true)"
  if [ -z "$output" ]; then
    summary_line "$section" "code summaries" "none"
    return 0
  fi
  printf '%s\n' "$output" |
    sed 's/^.*\[summary-code\]/[summary-code]/' |
    while IFS= read -r line; do
      summary_line "$section" "code" "$line"
    done
}

summary_kafka_topic() {
  summary_enabled || return 0
  local topic=$1
  local offsets
  if ! offsets="$(docker exec fdb-kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:29092 --topic "$topic" 2>/dev/null)"; then
    summary_line "Kafka" "$topic records" "unavailable"
    return 0
  fi
  local partitions
  local records
  partitions="$(printf '%s\n' "$offsets" | sed '/^$/d' | wc -l | tr -d ' ')"
  records="$(printf '%s\n' "$offsets" | awk -F: '{sum += $3} END {print sum + 0}')"
  summary_line "Kafka" "$topic partitions" "$partitions"
  summary_line "Kafka" "$topic records" "$records"
}

summary_mysql_kpi() {
  summary_enabled || return 0
  local query="
SELECT 'rows_by_window_kind', COALESCE(GROUP_CONCAT(CONCAT(window_kind, '=', cnt) ORDER BY window_kind SEPARATOR ','), 'none')
FROM (SELECT window_kind, COUNT(*) cnt FROM cell_kpi GROUP BY window_kind) x;
SELECT 'window_ts_range', CONCAT(COALESCE(MIN(window_start_ts), 0), '..', COALESCE(MAX(window_end_ts), 0)) FROM cell_kpi;
SELECT 'distinct_site_cell_grid', CONCAT(COUNT(DISTINCT site_id), '/', COUNT(DISTINCT cell_id), '/', COUNT(DISTINCT grid_id)) FROM cell_kpi;
"
  docker exec fdb-mysql mysql -N -ufdb -pfdbpwd fdb -e "$query" 2>/dev/null |
    while IFS=$'\t' read -r metric value; do
      summary_line "MySQL KPI" "$metric" "${value:-empty}"
    done
}

summary_parquet_kpi() {
  summary_enabled || return 0
  local root=${1:-docker/data/warehouse/cell_kpi}
  if [ ! -d "$root" ]; then
    summary_line "Parquet KPI" "files" "0"
    summary_line "Parquet KPI" "bytes" "0"
    summary_line "Parquet KPI" "partitions" "0"
    return 0
  fi
  local file_count
  local total_bytes
  local partitions
  file_count="$(find "$root" -name '*.parquet' -type f | wc -l | tr -d ' ')"
  total_bytes="$(find "$root" -name '*.parquet' -type f -printf '%s\n' 2>/dev/null | awk '{sum += $1} END {print sum + 0}')"
  partitions="$(find "$root" -name '*.parquet' -type f -printf '%h\n' 2>/dev/null | sort -u | wc -l | tr -d ' ')"
  summary_line "Parquet KPI" "files" "$file_count"
  summary_line "Parquet KPI" "bytes" "$total_bytes"
  summary_line "Parquet KPI" "partitions" "$partitions"
  summary_command "Parquet KPI" "partition samples" "find '$root' -name '*.parquet' -type f -printf '%h\n' | sed \"s#^$root/##\" | sort -u | head -5"
}

summary_flink_job() {
  summary_enabled || return 0
  summary_command "Flink" "jobs" "docker exec fdb-flink-jobmanager ./bin/flink list | grep -E ' : .* : ' | sed 's/^[[:space:]]*//'"
  summary_command "Flink" "latest completed checkpoints" "docker logs fdb-flink-jobmanager 2>&1 | grep 'Completed checkpoint' | tail -3 | sed 's/^.*Completed checkpoint /checkpoint /'"
}

summary_hive_kpi() {
  summary_enabled || return 0
  local output
  output="$(docker exec fdb-hive-server beeline -u jdbc:hive2://localhost:10000/default --silent=true --showHeader=false \
    -e 'MSCK REPAIR TABLE fdb.cell_kpi; SELECT COUNT(*) FROM fdb.cell_kpi; SHOW PARTITIONS fdb.cell_kpi;' 2>/dev/null || true)"
  local count
  local partitions
  count="$(summary_extract_last_integer "$output")"
  partitions="$(summary_count_hive_partitions "$output")"
  summary_line "Hive KPI" "rows" "$count"
  summary_line "Hive KPI" "partitions" "$partitions"
}
