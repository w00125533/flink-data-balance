#!/usr/bin/env bash

summary_enabled() {
  case "${FDB_E2E_SUMMARY:-}" in
    1|true|TRUE|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

e2e_run_id() {
  if [ -n "${FDB_E2E_RUN_ID:-}" ]; then
    printf '%s\n' "$FDB_E2E_RUN_ID"
    return 0
  fi
  if [ -z "${FDB_E2E_EFFECTIVE_RUN_ID:-}" ]; then
    FDB_E2E_EFFECTIVE_RUN_ID="$(date -u '+%Y%m%d-%H%M%S')"
    export FDB_E2E_EFFECTIVE_RUN_ID
  fi
  printf '%s\n' "$FDB_E2E_EFFECTIVE_RUN_ID"
}

e2e_runs_root() {
  printf '%s\n' "${FDB_E2E_RUNS_DIR:-docker/data/observability-runs}"
}

e2e_run_dir() {
  printf '%s/%s\n' "$(e2e_runs_root)" "$(e2e_run_id)"
}

e2e_run_summary_file() {
  printf '%s/logs-summary.log\n' "$(e2e_run_dir)"
}

e2e_run_meta_file() {
  printf '%s/meta.json\n' "$(e2e_run_dir)"
}

e2e_keep_running_on_success() {
  case "${FDB_E2E_KEEP_RUNNING_ON_SUCCESS:-}" in
    1|true|TRUE|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

observability_prometheus_url() {
  printf '%s\n' "${FDB_PROMETHEUS_URL:-http://localhost:${FDB_PROMETHEUS_PORT:-19090}}"
}

observability_api_url() {
  printf '%s\n' "${FDB_OBSERVABILITY_URL:-http://localhost:18080}"
}

observability_links() {
  printf '[e2e] Prometheus | %s\n' "$(observability_prometheus_url)"
  printf '[e2e] Observability API metrics | %s/metrics\n' "$(observability_api_url)"
}

assert_sink_latency_runtime_samples() {
  local dataset=${1:-kpi_1m}
  local payload
  if ! payload="$(curl -fsS "$(observability_api_url)/api/results/sink-latency")"; then
    return 1
  fi

  printf '%s\n' "$payload" |
    tr '{' '\n' |
    awk -v dataset="$dataset" '
      /"sinkName":/ && $0 ~ "\"dataset\":\"" dataset "\"" {
        if (match($0, /"records":[0-9]+/)) {
          records = substr($0, RSTART + 10, RLENGTH - 10) + 0
          if (records > 0) {
            found = 1
          }
        }
      }
      END { exit(found ? 0 : 1) }
    '
}

shared_infra_dir() {
  printf '%s\n' "${SHARED_INFRA_DIR:-../shared-data-infra}"
}

shared_kafka_exec() {
  if docker compose -f "$(shared_infra_dir)/compose.yaml" -f "$(shared_infra_dir)/compose.streaming.yaml" --profile streaming \
    exec -T kafka "$@" >/tmp/fdb-shared-kafka-exec.out 2>/tmp/fdb-shared-kafka-exec.err; then
    cat /tmp/fdb-shared-kafka-exec.out
    return 0
  fi

  docker exec shared-data-infra-kafka-1 "$@"
}

shared_hive_exec() {
  docker compose -f "$(shared_infra_dir)/compose.yaml" -f "$(shared_infra_dir)/compose.lakehouse.yaml" \
    --profile lakehouse --profile lakehouse-tools exec -T hive-server "$@"
}

shared_hdfs_exec() {
  docker compose -f "$(shared_infra_dir)/compose.yaml" -f "$(shared_infra_dir)/compose.lakehouse.yaml" \
    --profile lakehouse exec -T namenode hdfs dfs -fs "${FDB_HDFS_URI:-hdfs://namenode:8020}" "$@"
}

shared_starrocks_mysql() {
  local use_database=1
  local args=(-h 127.0.0.1 -P 9030 -u "${FDB_STARROCKS_USER:-root}")

  if [ "${1:-}" = "--no-database" ]; then
    use_database=0
    shift
  fi
  if [ -n "${FDB_STARROCKS_PASSWORD:-}" ]; then
    args+=("-p${FDB_STARROCKS_PASSWORD}")
  fi
  if [ "$use_database" -eq 1 ]; then
    docker compose -f "$(shared_infra_dir)/compose.yaml" -f "$(shared_infra_dir)/compose.starrocks.yaml" \
      --profile starrocks exec -T starrocks-fe mysql "${args[@]}" "$@" "${FDB_STARROCKS_DATABASE:-fdb}"
  else
    docker compose -f "$(shared_infra_dir)/compose.yaml" -f "$(shared_infra_dir)/compose.starrocks.yaml" \
      --profile starrocks exec -T starrocks-fe mysql "${args[@]}" "$@"
  fi
}

project_flink_exec() {
  docker exec -T fdb-flink-jobmanager "$@"
}

summary_file() {
  printf '%s\n' "${FDB_E2E_SUMMARY_FILE:-$(e2e_run_summary_file)}"
}

summary_emit() {
  local line=$1
  printf '%s\n' "$line"
  local file
  file="$(summary_file)"
  local dir
  dir="$(dirname "$file")"
  [ "$dir" = "." ] || mkdir -p "$dir"
  printf '%s\n' "$line" >> "$file"
}

summary_init() {
  e2e_run_id >/dev/null
  local file
  file="$(summary_file)"
  local dir
  dir="$(dirname "$file")"
  [ "$dir" = "." ] || mkdir -p "$dir"
  : > "$file"
  mkdir -p "$(e2e_run_dir)"
  cat > "$(e2e_run_meta_file)" <<EOF
{
  "runId": "$(e2e_run_id)",
  "status": "running",
  "startedAt": "$(date -u '+%Y-%m-%dT%H:%M:%SZ')",
  "completedAt": null,
  "summaryFile": "$(basename "$file")"
}
EOF
  summary_emit "[summary] Output | file | $file"
  summary_emit "[summary] Execution | run id | $(e2e_run_id)"
}

summary_finalize() {
  local status=$1
  local status_text="success"
  if [ "$status" -ne 0 ]; then
    status_text="failed"
  fi
  mkdir -p "$(e2e_run_dir)"
  cat > "$(e2e_run_meta_file)" <<EOF
{
  "runId": "$(e2e_run_id)",
  "status": "$status_text",
  "startedAt": "$(summary_started_at)",
  "completedAt": "$(date -u '+%Y-%m-%dT%H:%M:%SZ')",
  "summaryFile": "$(basename "$(summary_file)")"
}
EOF
  summary_line "Execution" "status" "$status_text"
}

summary_started_at() {
  local meta
  meta="$(e2e_run_meta_file)"
  if [ -f "$meta" ]; then
    sed -n 's/.*"startedAt"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$meta" | head -1
  else
    date -u '+%Y-%m-%dT%H:%M:%SZ'
  fi
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

summary_sink_performance() {
  summary_enabled || return 0
  local output
  output="$(docker logs fdb-flink-taskmanager 2>/dev/null |
    grep '\[summary-code\] sink=' |
    sed 's/^.*\[summary-code\]/[summary-code]/' |
    tail -n "${FDB_E2E_SINK_SUMMARY_LINES:-40}" || true)"
  if [ -z "$output" ]; then
    summary_line "Sink Performance" "code summaries" "none"
    return 0
  fi
  printf '%s\n' "$output" |
    while IFS= read -r line; do
      summary_line "Sink Performance" "code" "$line"
    done
}

summary_kafka_topic() {
  summary_enabled || return 0
  local topic=$1
  local offsets
  if ! offsets="$(shared_kafka_exec kafka-run-class kafka.tools.GetOffsetShell --broker-list "${FDB_KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092}" --topic "$topic" 2>/dev/null)"; then
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

summary_starrocks_kpi() {
  summary_enabled || return 0
  local query="
SELECT 'rows_by_window_kind', COALESCE(GROUP_CONCAT(CONCAT(window_kind, '=', cnt) ORDER BY window_kind SEPARATOR ','), 'none')
FROM (SELECT window_kind, COUNT(*) cnt FROM cell_kpi GROUP BY window_kind) x;
SELECT 'window_ts_range', CONCAT(COALESCE(MIN(window_start_ts), 0), '..', COALESCE(MAX(window_end_ts), 0)) FROM cell_kpi;
SELECT 'distinct_site_cell_grid', CONCAT(COUNT(DISTINCT site_id), '/', COUNT(DISTINCT cell_id), '/', COUNT(DISTINCT grid_id)) FROM cell_kpi;
"
  shared_starrocks_mysql -N -e "$query" 2>/dev/null |
    while IFS=$'\t' read -r metric value; do
      summary_line "StarRocks KPI" "$metric" "${value:-empty}"
    done
}

summary_starrocks_scalar() {
  local query=$1
  shared_starrocks_mysql -N -B -e "$query"
}

summary_starrocks_query() {
  summary_enabled || return 0
  local metric=$1
  local query=$2
  local value
  if value="$(summary_starrocks_scalar "$query" 2>/dev/null | tail -1)"; then
    value="${value:-empty}"
  else
    value="unavailable"
  fi
  summary_line "StarRocks" "$metric" "$value"
}

flink_rest_url() {
  printf '%s\n' "${FDB_FLINK_REST_URL:-http://localhost:8081}"
}

flink_dynamic_balancing_vertex_pattern() {
  printf '%s\n' 'routing-assigner|vbucket-load-meter|load-coordinator'
}

fetch_flink_job_plan() {
  local job_id=$1
  curl -fsS "$(flink_rest_url)/jobs/${job_id}/plan"
}

assert_flink_dynamic_balancing_vertices_absent() {
  local job_id=$1
  local plan
  if ! plan="$(fetch_flink_job_plan "$job_id")"; then
    echo "[fail] Unable to fetch Flink DAG plan for job $job_id"
    return 1
  fi

  local pattern
  pattern="$(flink_dynamic_balancing_vertex_pattern)"
  if printf '%s\n' "$plan" | grep -Eiq "$pattern"; then
    echo "[fail] Flink DAG contains dynamic-balancing vertices while FDB_DYNAMIC_BALANCING_ENABLED is false"
    printf '%s\n' "$plan" |
      grep -Eio "$pattern" |
      sort -u |
      sed 's/^/[fail] unexpected vertex: /'
    return 1
  fi

  echo "[ok] Flink DAG has no dynamic-balancing vertices by default"
}

summary_flink_dynamic_balancing_vertices() {
  summary_enabled || return 0
  local job_id=$1
  local plan
  if ! plan="$(fetch_flink_job_plan "$job_id" 2>/dev/null)"; then
    summary_line "Flink DAG" "dynamic balancing vertices" "unavailable"
    return 0
  fi

  local pattern
  pattern="$(flink_dynamic_balancing_vertex_pattern)"
  local vertices
  vertices="$(printf '%s\n' "$plan" | { grep -Eio "$pattern" || true; } | sort -u | tr '\n' ',' | sed 's/,$//')"
  if [ -n "$vertices" ]; then
    summary_line "Flink DAG" "dynamic balancing vertices" "present:$vertices"
  else
    summary_line "Flink DAG" "dynamic balancing vertices" "absent"
  fi
}

hdfs_find_files() {
  local root=$1
  local name=$2
  shared_hdfs_exec -find "$root" -name "$name" 2>/dev/null || true
}

hdfs_du_bytes() {
  local root=$1
  local bytes
  bytes="$(shared_hdfs_exec -du -s "$root" 2>/dev/null | awk '{print $1}' | tail -1)"
  printf '%s\n' "${bytes:-0}"
}

summary_parquet_kpi() {
  summary_enabled || return 0
  local root=${1:-/warehouse/fdb/cell_kpi}
  local files
  files="$(hdfs_find_files "$root" '*.parquet')"
  if [ -z "$files" ]; then
    summary_line "Parquet KPI" "files" "0"
    summary_line "Parquet KPI" "bytes" "0"
    summary_line "Parquet KPI" "partitions" "0"
    return 0
  fi
  local file_count
  local total_bytes
  local partitions
  file_count="$(printf '%s\n' "$files" | sed '/^$/d' | wc -l | tr -d ' ')"
  total_bytes="$(hdfs_du_bytes "$root")"
  partitions="$(printf '%s\n' "$files" | sed 's#/[^/]*$##' | sort -u | wc -l | tr -d ' ')"
  summary_line "Parquet KPI" "files" "$file_count"
  summary_line "Parquet KPI" "bytes" "$total_bytes"
  summary_line "Parquet KPI" "partitions" "$partitions"
  summary_line "Parquet KPI" "partition samples" "$(printf '%s\n' "$files" | sed 's#/[^/]*$##' | sed "s#^$root/##" | sort -u | head -5 | tr '\n' ';' | sed 's/;*$//')"
}

summary_iceberg_kpi() {
  summary_enabled || return 0
  local root=${1:-/warehouse/iceberg/fdb/cell_kpi}
  local data_root="$root/data"
  local metadata_root="$root/metadata"
  local data_files_list
  data_files_list="$(hdfs_find_files "$data_root" '*.parquet')"
  if [ -z "$data_files_list" ]; then
    summary_line "Iceberg KPI" "data files" "0"
    summary_line "Iceberg KPI" "data bytes" "0"
    summary_line "Iceberg KPI" "partitions" "0"
    summary_line "Iceberg KPI" "metadata json" "0"
    summary_line "Iceberg KPI" "snapshots" "0"
    return 0
  fi

  local data_files
  local data_bytes
  local partitions
  local metadata_json
  local latest_metadata
  local snapshots
  data_files="$(printf '%s\n' "$data_files_list" | sed '/^$/d' | wc -l | tr -d ' ')"
  data_bytes="$(hdfs_du_bytes "$data_root")"
  partitions="$(printf '%s\n' "$data_files_list" | sed 's#/[^/]*$##' | sort -u | wc -l | tr -d ' ')"
  metadata_json="$(hdfs_find_files "$metadata_root" '*.metadata.json' | sed '/^$/d' | wc -l | tr -d ' ')"
  latest_metadata="$(hdfs_find_files "$metadata_root" '*.metadata.json' | sort | tail -1)"
  if [ -n "$latest_metadata" ]; then
    snapshots="$(shared_hdfs_exec -cat "$latest_metadata" 2>/dev/null | grep -o '"snapshot-id"[[:space:]]*:[[:space:]]*[0-9]*' | wc -l | tr -d ' ' || true)"
  else
    snapshots="0"
  fi

  summary_line "Iceberg KPI" "data files" "$data_files"
  summary_line "Iceberg KPI" "data bytes" "$data_bytes"
  summary_line "Iceberg KPI" "partitions" "$partitions"
  summary_line "Iceberg KPI" "metadata json" "$metadata_json"
  summary_line "Iceberg KPI" "snapshots" "$snapshots"
  summary_line "Iceberg KPI" "partition samples" "$(printf '%s\n' "$data_files_list" | sed 's#/[^/]*$##' | sed "s#^$data_root/##" | sort -u | head -5 | tr '\n' ';' | sed 's/;*$//')"
}

summary_hive_iceberg_compare() {
  summary_enabled || return 0
  local hive_root=${1:-/warehouse/fdb/cell_kpi}
  local iceberg_root=${2:-/warehouse/iceberg/fdb/cell_kpi/data}
  local hive_files
  local hive_bytes
  local iceberg_files
  local iceberg_bytes
  hive_files="$(hdfs_find_files "$hive_root" '*.parquet' | sed '/^$/d' | wc -l | tr -d ' ')"
  hive_bytes="$(hdfs_du_bytes "$hive_root")"
  iceberg_files="$(hdfs_find_files "$iceberg_root" '*.parquet' | sed '/^$/d' | wc -l | tr -d ' ')"
  iceberg_bytes="$(hdfs_du_bytes "$iceberg_root")"
  summary_line "Hive/Iceberg Compare" "hive_files" "$hive_files"
  summary_line "Hive/Iceberg Compare" "iceberg_files" "$iceberg_files"
  summary_line "Hive/Iceberg Compare" "hive_bytes" "$hive_bytes"
  summary_line "Hive/Iceberg Compare" "iceberg_bytes" "$iceberg_bytes"
}

summary_flink_job() {
  summary_enabled || return 0
  summary_command "Flink" "jobs" "docker exec fdb-flink-jobmanager ./bin/flink list | grep -E ' : .* : ' | sed 's/^[[:space:]]*//'"
  summary_command "Flink" "latest completed checkpoints" "docker logs fdb-flink-jobmanager 2>&1 | grep 'Completed checkpoint' | tail -3 | sed 's/^.*Completed checkpoint /checkpoint /'"
}

summary_observability() {
  summary_enabled || return 0
  summary_line "Observability" "prometheus" "$(observability_prometheus_url)"
  summary_line "Observability" "api metrics" "$(observability_api_url)/metrics"
  summary_kafka_topic "fdb-stage-metrics"
  summary_command "Observability" "prometheus fdb_stage_out_eps series" \
    "curl -fsS '$(observability_prometheus_url)/api/v1/query?query=fdb_stage_out_eps' | grep -o '\"metric\"' | wc -l | tr -d ' '"
  summary_command "Observability" "prometheus nonzero stage eps series" \
    "curl -fsS '$(observability_prometheus_url)/api/v1/query?query=fdb_stage_out_eps%20%3E%200' | grep -o '\"metric\"' | wc -l | tr -d ' '"
}

summary_hive_kpi() {
  summary_enabled || return 0
  local output
  output="$(shared_hive_exec beeline -u jdbc:hive2://localhost:10000/default --silent=true --showHeader=false \
    -e 'MSCK REPAIR TABLE fdb.cell_kpi; SELECT COUNT(*) FROM fdb.cell_kpi; SHOW PARTITIONS fdb.cell_kpi;' 2>/dev/null || true)"
  local count
  local partitions
  count="$(summary_extract_last_integer "$output")"
  partitions="$(summary_count_hive_partitions "$output")"
  summary_line "Hive KPI" "rows" "$count"
  summary_line "Hive KPI" "partitions" "$partitions"
}
