#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

export MSYS_NO_PATHCONV="${MSYS_NO_PATHCONV:-1}"
export MSYS2_ARG_CONV_EXCL="${MSYS2_ARG_CONV_EXCL:-*}"

TARGET="${1:-}"
COMMAND="${2:-}"
ARGS=()
if (($# > 2)); then
  ARGS=("${@:3}")
fi

STRICT=0
for arg in "${ARGS[@]}"; do
  if [[ "$arg" == "--strict" ]]; then
    STRICT=1
  fi
done

usage() {
  echo "Usage: scripts/deploy.sh <target> <command> [options]"
  echo "Targets:"
  echo "  local commands: check, up, init, prepare, submit, stop, smoke, prune, status, report, down"
  echo "  external-yarn commands: check, init, prepare, submit, stop, smoke, prune, status, report"
}

log() {
  echo "[INFO] $*"
}

ok() {
  echo "[OK] $*"
}

warn() {
  echo "[WARN] $*" >&2
}

die() {
  echo "[ERROR] $*" >&2
  exit 1
}

load_env() {
  local env_file="${FDB_ENV_FILE:-.env}"

  if [[ ! -f "$env_file" ]]; then
    die "env file not found: $env_file"
  fi

  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
  ok "loaded env file: $env_file"
}

load_env_optional() {
  local env_file="${FDB_ENV_FILE:-.env}"

  if [[ ! -f "$env_file" ]]; then
    warn "optional env file not found: $env_file"
    return 0
  fi

  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
  ok "loaded env file: $env_file"
}

generate_run_id() {
  echo "run-$(date -u +%Y%m%d-%H%M%S)-$$-${RANDOM}"
}

ensure_run_context() {
  if [[ -z "${FDB_RUN_ID:-}" ]]; then
    export FDB_RUN_ID
    FDB_RUN_ID="$(generate_run_id)"
  fi

  export FDB_RUN_LABEL="${FDB_RUN_LABEL:-}"
  export FDB_RESULT_SINK="${FDB_RESULT_SINK:-starrocks}"
  export FDB_METRICS_HISTORY_ENABLED="${FDB_METRICS_HISTORY_ENABLED:-true}"
  export FDB_METRICS_ENABLED="${FDB_METRICS_ENABLED:-true}"
  export FDB_METRICS_EMIT_INTERVAL_MS="${FDB_METRICS_EMIT_INTERVAL_MS:-5000}"
  export FDB_DLQ_ENABLED="${FDB_DLQ_ENABLED:-true}"
  export FDB_FLINK_PARALLELISM="${FDB_FLINK_PARALLELISM:-4}"
  export FDB_FLINK_CHECKPOINT_INTERVAL_MS="${FDB_FLINK_CHECKPOINT_INTERVAL_MS:-30000}"
}

write_current_run_env() {
  local state_file=$1

  {
    printf 'FDB_RUN_ID=%q\n' "${FDB_RUN_ID:-}"
    printf 'FDB_RUN_LABEL=%q\n' "${FDB_RUN_LABEL:-}"
    printf 'FDB_RESULT_SINK=%q\n' "${FDB_RESULT_SINK:-}"
    printf 'FDB_FLINK_PARALLELISM=%q\n' "${FDB_FLINK_PARALLELISM:-}"
    printf 'FDB_FLINK_CHECKPOINT_INTERVAL_MS=%q\n' "${FDB_FLINK_CHECKPOINT_INTERVAL_MS:-}"
    printf 'FDB_METRICS_ENABLED=%q\n' "${FDB_METRICS_ENABLED:-}"
    printf 'FDB_METRICS_HISTORY_ENABLED=%q\n' "${FDB_METRICS_HISTORY_ENABLED:-}"
    printf 'FDB_DLQ_ENABLED=%q\n' "${FDB_DLQ_ENABLED:-}"
  } >> "$state_file"
}

local_wait_for_job_terminal() {
  local job_id=$1
  local wait_sec="${FDB_FLINK_CANCEL_WAIT_SEC:-300}"
  local rest_url="${FDB_FLINK_REST_URL:-http://localhost:8081}"

  if [[ "$wait_sec" == "0" ]]; then
    return 0
  fi

  local deadline=$((SECONDS + wait_sec))
  local body
  while ((SECONDS < deadline)); do
    if ! body="$(curl -fsS "${rest_url}/jobs/${job_id}" 2>/dev/null)"; then
      ok "Flink job no longer visible: $job_id"
      return 0
    fi
    if [[ "$body" =~ \"state\"[[:space:]]*:[[:space:]]*\"(CANCELED|FAILED|FINISHED)\" ]]; then
      ok "Flink job reached terminal state: $job_id"
      return 0
    fi
    sleep 2
  done

  warn "timed out waiting for Flink job to reach terminal state: $job_id"
  return 1
}

local_wait_for_flink_slots() {
  local wait_sec="${FDB_FLINK_READY_WAIT_SEC:-60}"
  local rest_url="${FDB_FLINK_REST_URL:-http://localhost:8081}"
  local required_slots="${FDB_FLINK_PARALLELISM:-1}"

  if [[ "$wait_sec" == "0" ]]; then
    return 0
  fi
  if ! [[ "$required_slots" =~ ^[0-9]+$ ]] || [[ "$required_slots" -le 0 ]]; then
    required_slots=1
  fi

  local deadline=$((SECONDS + wait_sec))
  local body taskmanagers slots_available
  while ((SECONDS < deadline)); do
    if body="$(curl -fsS "${rest_url}/overview" 2>/dev/null)"; then
      taskmanagers="$(printf '%s\n' "$body" | sed -nE 's/.*"taskmanagers"[[:space:]]*:[[:space:]]*([0-9]+).*/\1/p' | head -n1)"
      slots_available="$(printf '%s\n' "$body" | sed -nE 's/.*"slots-available"[[:space:]]*:[[:space:]]*([0-9]+).*/\1/p' | head -n1)"
      if [[ "${taskmanagers:-0}" -gt 0 && "${slots_available:-0}" -ge "$required_slots" ]]; then
        ok "Flink slots available: taskmanagers=${taskmanagers}, slots=${slots_available}, required=${required_slots}"
        return 0
      fi
    fi
    sleep 2
  done

  warn "timed out waiting for Flink slots to become available: required=${required_slots}"
  return 1
}

local_flink_taskmanager_count() {
  local rest_url="${FDB_FLINK_REST_URL:-http://localhost:8081}"
  local body

  if body="$(curl -fsS "${rest_url}/overview" 2>/dev/null)"; then
    printf '%s\n' "$body" | sed -nE 's/.*"taskmanagers"[[:space:]]*:[[:space:]]*([0-9]+).*/\1/p' | head -n1
  else
    printf '0\n'
  fi
}

local_ensure_flink_slots_for_submit() {
  local taskmanagers

  if local_wait_for_flink_slots; then
    return 0
  fi

  taskmanagers="$(local_flink_taskmanager_count)"
  if [[ "${taskmanagers:-0}" -gt 0 ]]; then
    return 1
  fi

  warn "no Flink TaskManager is registered; recreating local TaskManager before submit"
  docker compose -f docker/docker-compose.yml --profile e2e up -d --force-recreate --no-deps taskmanager
  local_wait_for_flink_slots
}

current_run_id() {
  local state_file=$1
  local state_run_id

  if [[ -n "${FDB_RUN_ID:-}" ]]; then
    echo "$FDB_RUN_ID"
    return 0
  fi

  if [[ -f "$state_file" ]]; then
    state_run_id="$( (
      set +u
      # shellcheck disable=SC1090
      source "$state_file"
      printf '%s' "${FDB_RUN_ID:-}"
    ) 2>/dev/null || true)"
    if [[ -n "$state_run_id" ]]; then
      echo "$state_run_id"
      return 0
    fi
    warn "run state file does not contain FDB_RUN_ID: $state_file"
  else
    warn "run state file not found: $state_file; set FDB_RUN_ID or run ${TARGET:-target} submit first"
  fi

  die "no run id found; run ${TARGET:-target} submit first or set FDB_RUN_ID"
}

warn_or_fail() {
  local message=$1
  if [[ "$STRICT" == "1" ]]; then
    die "$message"
  fi
  warn "$message"
}

require_command_soft() {
  local command_name=$1
  if command -v "$command_name" >/dev/null 2>&1; then
    ok "command available: $command_name"
  else
    warn_or_fail "command not found: $command_name"
  fi
}

require_flink_home_soft() {
  if [[ -z "${FLINK_HOME:-}" ]]; then
    warn_or_fail "FLINK_HOME is not set; expected flink command at \$FLINK_HOME/bin/flink"
    return 0
  fi

  if [[ -x "$FLINK_HOME/bin/flink" ]]; then
    ok "flink command available: $FLINK_HOME/bin/flink"
  else
    warn_or_fail "flink command not executable: $FLINK_HOME/bin/flink"
  fi
}

maven_cmd() {
  if command -v mvn.cmd >/dev/null 2>&1; then
    mvn.cmd "$@"
  else
    mvn "$@"
  fi
}

shared_infra_dir() {
  echo "${SHARED_INFRA_DIR:-../shared-data-infra}"
}

local_hdfs_uri() {
  echo "${FDB_HDFS_URI:-hdfs://namenode:8020}"
}

local_kafka_bootstrap() {
  echo "${FDB_KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092}"
}

shared_streaming() {
  docker compose \
    -f "$(shared_infra_dir)/compose.yaml" \
    -f "$(shared_infra_dir)/compose.streaming.yaml" \
    --profile streaming \
    "$@"
}

shared_kafka_exec() {
  local timeout_sec="${FDB_SHARED_KAFKA_EXEC_TIMEOUT_SEC:-10}"
  if timeout "${timeout_sec}s" docker exec shared-data-infra-kafka-1 "$@" >/tmp/fdb-shared-kafka-exec.out 2>/tmp/fdb-shared-kafka-exec.err; then
    cat /tmp/fdb-shared-kafka-exec.out
    return 0
  fi

  timeout "${timeout_sec}s" docker compose \
    -f "$(shared_infra_dir)/compose.yaml" \
    -f "$(shared_infra_dir)/compose.streaming.yaml" \
    --profile streaming \
    exec -T kafka "$@"
}

shared_lakehouse() {
  docker compose \
    -f "$(shared_infra_dir)/compose.yaml" \
    -f "$(shared_infra_dir)/compose.lakehouse.yaml" \
    --profile lakehouse \
    --profile lakehouse-tools \
    "$@"
}

shared_hdfs_exec() {
  local hdfs_bin="${FDB_LOCAL_HDFS_BIN:-/opt/hadoop-3.2.1/bin/hdfs}"
  local timeout_sec="${FDB_SHARED_HDFS_EXEC_TIMEOUT_SEC:-30}"
  local compose_command=(
    docker compose
    -f "$(shared_infra_dir)/compose.yaml"
    -f "$(shared_infra_dir)/compose.lakehouse.yaml"
    --profile lakehouse
    --profile lakehouse-tools
    exec -T namenode
    "$hdfs_bin" dfs -fs "$(local_hdfs_uri)" "$@"
  )
  local direct_command=(
    docker exec shared-data-infra-namenode-1
    "$hdfs_bin" dfs -fs "$(local_hdfs_uri)" "$@"
  )

  if command -v timeout >/dev/null 2>&1; then
    if timeout "${timeout_sec}s" "${compose_command[@]}" \
        >/tmp/fdb-shared-hdfs-exec.out 2>/tmp/fdb-shared-hdfs-exec.err; then
      cat /tmp/fdb-shared-hdfs-exec.out
      return 0
    fi
    timeout "${timeout_sec}s" "${direct_command[@]}"
    return $?
  fi

  if "${compose_command[@]}" >/tmp/fdb-shared-hdfs-exec.out 2>/tmp/fdb-shared-hdfs-exec.err; then
    cat /tmp/fdb-shared-hdfs-exec.out
    return 0
  fi

  "${direct_command[@]}"
}

shared_starrocks() {
  local timeout_sec="${FDB_SHARED_STARROCKS_EXEC_TIMEOUT_SEC:-60}"
  local command=(
    docker compose
    -f "$(shared_infra_dir)/compose.yaml"
    -f "$(shared_infra_dir)/compose.starrocks.yaml"
    --profile starrocks
    "$@"
  )

  if command -v timeout >/dev/null 2>&1; then
    timeout "$timeout_sec" "${command[@]}"
  else
    "${command[@]}"
  fi
}

shared_starrocks_mysql() {
  local use_database=1
  local args=(-h 127.0.0.1 -P 9030 -u "${FDB_STARROCKS_USER:-root}")

  if [[ "${1:-}" == "--no-database" ]]; then
    use_database=0
    shift
  fi
  if [[ -n "${FDB_STARROCKS_PASSWORD:-}" ]]; then
    args+=("-p${FDB_STARROCKS_PASSWORD}")
  fi
  if [[ "$use_database" == "1" ]]; then
    shared_starrocks exec -T starrocks-fe mysql "${args[@]}" "$@" "${FDB_STARROCKS_DATABASE:-fdb}"
  else
    shared_starrocks exec -T starrocks-fe mysql "${args[@]}" "$@"
  fi
}

starrocks_cell_kpi_connector_column_sql() {
  local column=$1
  case "$column" in
    join_quality)
      echo "ADD COLUMN join_quality VARCHAR(16) NOT NULL DEFAULT 'JOINED' AFTER window_end_ts"
      ;;
    rsrp_sample_count)
      echo 'ADD COLUMN rsrp_sample_count BIGINT NOT NULL DEFAULT "0" AFTER num_users'
      ;;
    sinr_sample_count)
      echo 'ADD COLUMN sinr_sample_count BIGINT NOT NULL DEFAULT "0" AFTER rsrp_sample_count'
      ;;
    attach_attempts)
      echo 'ADD COLUMN attach_attempts BIGINT NOT NULL DEFAULT "0" AFTER sinr_sample_count'
      ;;
    *)
      die "unsupported StarRocks cell_kpi connector column: $column"
      ;;
  esac
}

local_starrocks_column_exists() {
  local column=$1
  shared_starrocks_mysql -N -B -e "SHOW COLUMNS FROM cell_kpi LIKE '$column';" \
    | grep -Eq "^${column}[[:space:]]"
}

ensure_local_starrocks_cell_kpi_connector_schema() {
  local column
  local alter_sql
  local columns=(join_quality rsrp_sample_count sinr_sample_count attach_attempts)

  for column in "${columns[@]}"; do
    if ! local_starrocks_column_exists "$column"; then
      alter_sql="$(starrocks_cell_kpi_connector_column_sql "$column")"
      log "adding StarRocks cell_kpi connector column: $column"
      shared_starrocks_mysql -e "ALTER TABLE cell_kpi ${alter_sql};"
    fi
  done
}

wait_for_command() {
  local label=$1
  local max_attempts=$2
  local sleep_seconds=$3
  shift 3

  for _ in $(seq 1 "$max_attempts"); do
    if "$@" >/dev/null 2>&1; then
      ok "$label"
      return 0
    fi
    sleep "$sleep_seconds"
  done

  die "$label did not become ready"
}

now_epoch_ms() {
  echo "$(($(date +%s) * 1000))"
}

retention_threshold_ms() {
  local retention_ms=$1
  echo "$(($(now_epoch_ms) - retention_ms))"
}

retention_days_floor() {
  local retention_ms=$1
  local days=$((retention_ms / 86400000))
  if ((days < 1)); then
    days=1
  fi
  echo "$days"
}

hdfs_file_epoch_seconds() {
  local file_date=$1
  local file_time=$2
  local year month day hour minute
  IFS=- read -r year month day <<< "$file_date"
  IFS=: read -r hour minute <<< "$file_time"
  awk -v year="$year" -v month="$month" -v day="$day" -v hour="$hour" -v minute="$minute" \
    'BEGIN { print mktime(year " " month " " day " " hour " " minute " 00", 1) }'
}

prune_hdfs_listing_matches() {
  local runner=$1
  local base_path=$2
  local path_pattern=$3
  local retention_ms=${4:-}
  local threshold_seconds=0
  local listing
  local paths_to_delete=()

  if [[ -n "$retention_ms" ]]; then
    threshold_seconds=$(( $(retention_threshold_ms "$retention_ms") / 1000 ))
  fi

  listing="$("$runner" -ls -R "$base_path" 2>/dev/null || true)"

  while read -r permissions replication owner group size file_date file_time path _rest; do
    [[ "${permissions:-}" == -* ]] || continue
    [[ "${path:-}" == $path_pattern ]] || continue

    if [[ -n "$retention_ms" ]]; then
      local file_epoch
      file_epoch="$(hdfs_file_epoch_seconds "$file_date" "$file_time")" || continue
      ((file_epoch < threshold_seconds)) || continue
    fi

    paths_to_delete+=("$path")
  done <<< "$listing"

  local path
  for path in "${paths_to_delete[@]}"; do
    "$runner" -rm -f "$path"
  done
}

prune_starrocks_sql() {
  local kpi_retention_ms=${FDB_STARROCKS_KPI_RETENTION_MS:-3600000}
  local anomaly_retention_ms=${FDB_STARROCKS_ANOMALY_RETENTION_MS:-3600000}
  local kpi_threshold
  local anomaly_threshold
  kpi_threshold="$(retention_threshold_ms "$kpi_retention_ms")"
  anomaly_threshold="$(retention_threshold_ms "$anomaly_retention_ms")"

  cat <<SQL
DELETE FROM cell_kpi WHERE window_end_ts < ${kpi_threshold};
DELETE FROM cell_anomaly_events WHERE event_ts < ${anomaly_threshold};
DELETE FROM user_anomaly_events WHERE event_ts < ${anomaly_threshold};
DELETE FROM grid_anomaly_events WHERE event_ts < ${anomaly_threshold};
SQL
}

reset_starrocks_sql() {
  cat <<SQL
TRUNCATE TABLE cell_kpi;
TRUNCATE TABLE cell_anomaly_events;
TRUNCATE TABLE user_anomaly_events;
TRUNCATE TABLE grid_anomaly_events;
SQL
}

benchmark_kafka_topics() {
  local topics=(
    "${FDB_CHR_TOPIC:-chr-events}"
    "${FDB_PM_TOPIC:-pm-stats}"
    "${FDB_CFG_TOPIC:-cfg-config}"
    "${FDB_TOPOLOGY_TOPIC:-topology}"
    "${FDB_LB_HEARTBEAT_TOPIC:-lb-heartbeat}"
    "${FDB_LB_ROUTING_TOPIC:-lb-routing}"
    "${FDB_METRICS_TOPIC:-fdb-stage-metrics}"
    "${FDB_CELL_ANOMALY_TOPIC:-cell-anomaly-events}"
    "${FDB_USER_ANOMALY_TOPIC:-user-anomaly-events}"
    "${FDB_GRID_ANOMALY_TOPIC:-grid-anomaly-events}"
    "${FDB_KPI_1M_TOPIC:-cell-kpi-1m}"
    "${FDB_KPI_5M_TOPIC:-cell-kpi-5m}"
    "${FDB_CHR_DLQ_TOPIC:-chr-dlq}"
    "${FDB_PM_DLQ_TOPIC:-pm-dlq}"
    "${FDB_CFG_DLQ_TOPIC:-cfg-dlq}"
    "${FDB_ENRICHMENT_LATE_TOPIC:-enrichment-late}"
  )
  printf '%s\n' "${topics[@]}" | awk 'NF && !seen[$0]++'
}

reset_hdfs_path_with() {
  local runner=$1
  local path=$2

  "$runner" -rm -r -skipTrash "$path" >/dev/null 2>&1 || true
  "$runner" -mkdir -p "$path" >/dev/null
  "$runner" -chmod -R 777 "$path" >/dev/null 2>&1 || true
}

reset_hdfs_hive_benchmark_outputs_with() {
  local runner=$1
  local hive_cell_path=${FDB_HIVE_WAREHOUSE_PATH:-/warehouse/fdb/cell_kpi}
  local hive_root=${FDB_HIVE_WAREHOUSE_ROOT:-${hive_cell_path%/cell_kpi}}

  if [[ "$hive_root" == "$hive_cell_path" ]]; then
    hive_root="${hive_cell_path%/*}"
  fi

  reset_hdfs_path_with "$runner" "$hive_cell_path"
  reset_hdfs_path_with "$runner" "$hive_root/cell_anomaly_events"
  reset_hdfs_path_with "$runner" "$hive_root/user_anomaly_events"
  reset_hdfs_path_with "$runner" "$hive_root/grid_anomaly_events"
}

reset_hdfs_iceberg_benchmark_outputs_with() {
  local runner=$1
  local iceberg_root=${FDB_ICEBERG_WAREHOUSE_PATH:-/warehouse/iceberg}/${FDB_ICEBERG_DATABASE:-iceberg_db}

  reset_hdfs_path_with "$runner" "$iceberg_root/${FDB_ICEBERG_TABLE:-cell_kpi}"
  reset_hdfs_path_with "$runner" "$iceberg_root/${FDB_ICEBERG_CELL_ANOMALY_TABLE:-cell_anomaly_events}"
  reset_hdfs_path_with "$runner" "$iceberg_root/${FDB_ICEBERG_USER_ANOMALY_TABLE:-user_anomaly_events}"
  reset_hdfs_path_with "$runner" "$iceberg_root/${FDB_ICEBERG_GRID_ANOMALY_TABLE:-grid_anomaly_events}"
}

reset_hdfs_benchmark_outputs_with() {
  local runner=$1

  reset_hdfs_hive_benchmark_outputs_with "$runner"
  reset_hdfs_iceberg_benchmark_outputs_with "$runner"
}

LOCAL_FLINK_ENV_ARGS=()

build_local_flink_env_args() {
  local key
  local value
  local env_keys=(
    FDB_KAFKA_BOOTSTRAP
    FDB_KAFKA_FETCH_MAX_BYTES
    FDB_KAFKA_MAX_PARTITION_FETCH_BYTES
    FDB_KAFKA_MAX_POLL_RECORDS
    FDB_HDFS_URI
    FDB_STARROCKS_FE_ENDPOINT
    FDB_STARROCKS_JDBC_URL
    FDB_STARROCKS_CONNECTOR_JDBC_URL
    FDB_STARROCKS_LOAD_URL
    FDB_STARROCKS_USER
    FDB_STARROCKS_PASSWORD
    FDB_STARROCKS_DATABASE
    FDB_STARROCKS_SINK_SEMANTIC
    FDB_STARROCKS_SINK_LABEL_PREFIX
    FDB_STARROCKS_SINK_BUFFER_FLUSH_MAX_BYTES
    FDB_STARROCKS_SINK_BUFFER_FLUSH_MAX_ROWS
    FDB_STARROCKS_SINK_BUFFER_FLUSH_INTERVAL_MS
    FDB_RESULT_SINK
    FDB_DLQ_ENABLED
    FDB_HIVE_WAREHOUSE
    FDB_HIVE_WAREHOUSE_PATH
    FDB_ICEBERG_ENABLED
    FDB_ICEBERG_WAREHOUSE
    FDB_ICEBERG_WAREHOUSE_PATH
    FDB_ICEBERG_CATALOG
    FDB_ICEBERG_DATABASE
    FDB_ICEBERG_TABLE
    FDB_ICEBERG_CELL_ANOMALY_TABLE
    FDB_ICEBERG_USER_ANOMALY_TABLE
    FDB_ICEBERG_GRID_ANOMALY_TABLE
    FDB_ICEBERG_METASTORE_URI
    FDB_FLINK_CHECKPOINT_DIR
    FDB_FLINK_CHECKPOINT_INTERVAL_MS
    FDB_FLINK_PARALLELISM
    FDB_METRICS_TOPIC
    FDB_METRICS_ENABLED
    FDB_METRICS_HISTORY_ENABLED
    FDB_METRICS_EMIT_INTERVAL_MS
    FDB_RUN_ID
    FDB_RUN_LABEL
    FDB_E2E_SUMMARY
  )

  LOCAL_FLINK_ENV_ARGS=()
  for key in "${env_keys[@]}"; do
    value="${!key:-}"
    if [[ -n "$value" || "${!key+x}" == "x" ]]; then
      LOCAL_FLINK_ENV_ARGS+=("-e" "$key=$value")
    fi
  done
}

local_flink_submit_once() {
  local jar=$1
  local submit_log=$2
  local timeout_sec="${FDB_LOCAL_FLINK_SUBMIT_TIMEOUT_SEC:-120}"
  local command=(
    docker exec --user flink
    "${LOCAL_FLINK_ENV_ARGS[@]}"
    fdb-flink-jobmanager
    flink run -d -p "$FDB_FLINK_PARALLELISM" "$jar"
  )

  : > "$submit_log"
  if command -v timeout >/dev/null 2>&1; then
    timeout "${timeout_sec}s" "${command[@]}" | tee "$submit_log"
  else
    "${command[@]}" | tee "$submit_log"
  fi
}

parse_local_submit_job_id() {
  local submit_log=$1

  awk '/JobID|Job ID|job id/ {job_id=$NF} END {print job_id}' "$submit_log"
}

local_latest_active_flink_job_id() {
  local rest_url="${FDB_FLINK_REST_URL:-http://localhost:8081}"
  local timeout_sec="${FDB_LOCAL_FLINK_REST_TIMEOUT_SEC:-10}"
  local body

  body="$(curl -fsS --max-time "$timeout_sec" "${rest_url}/jobs/overview" 2>/dev/null || true)"
  printf '%s\n' "$body" |
    tr '{' '\n' |
    sed -nE 's/.*"jid"[[:space:]]*:[[:space:]]*"([^"]+)".*"state"[[:space:]]*:[[:space:]]*"(RUNNING|CREATED|INITIALIZING|RESTARTING)".*/\1/p' |
    tail -n1
}

local_wait_for_late_submit_job_id() {
  local submit_log=$1
  local wait_sec="${FDB_LOCAL_FLINK_SUBMIT_LATE_WAIT_SEC:-30}"
  local deadline=$((SECONDS + wait_sec))
  local flink_job_id

  while ((SECONDS < deadline)); do
    flink_job_id="$(parse_local_submit_job_id "$submit_log")"
    if [[ -n "$flink_job_id" ]]; then
      printf '%s\n' "$flink_job_id"
      return 0
    fi

    flink_job_id="$(local_latest_active_flink_job_id)"
    if [[ -n "$flink_job_id" ]]; then
      printf '%s\n' "$flink_job_id"
      return 0
    fi
    sleep 2
  done
}

local_reset_kafka_topics_with_admin() {
  local jar="${FDB_BENCHMARK_RUNNER_JAR:-benchmark-runner/target/benchmark-runner-0.1.0-SNAPSHOT.jar}"

  if [[ "${FDB_LOCAL_KAFKA_RESET_IMPL:-admin}" == "docker" ]]; then
    return 1
  fi
  if [[ ! -f "$jar" ]] || ! command -v java >/dev/null 2>&1; then
    return 1
  fi

  log "resetting shared Kafka benchmark topics via Kafka AdminClient"
  if java -cp "$jar" com.fdb.benchmark.KafkaTopicResetTool; then
    return 0
  fi

  warn "Kafka AdminClient topic reset failed"
  return 2
}

benchmark_kafka_topics_for_readiness() {
  local lb_heartbeat="${FDB_LB_HEARTBEAT_TOPIC:-lb-heartbeat}"
  local lb_routing="${FDB_LB_ROUTING_TOPIC:-lb-routing}"
  local topic

  while read -r topic; do
    [[ -n "$topic" ]] || continue
    if [[ "${FDB_DYNAMIC_BALANCING_ENABLED:-false}" != "true" ]] \
      && { [[ "$topic" == "$lb_heartbeat" ]] || [[ "$topic" == "$lb_routing" ]]; }; then
      continue
    fi
    printf '%s\n' "$topic"
  done < <(benchmark_kafka_topics)
}

local_wait_for_kafka_topic_metadata() {
  local bootstrap
  local wait_sec="${FDB_KAFKA_TOPIC_READY_WAIT_SEC:-180}"
  local poll_sec="${FDB_KAFKA_TOPIC_READY_POLL_SEC:-2}"
  local stable_polls="${FDB_KAFKA_TOPIC_READY_STABLE_POLLS:-3}"
  local deadline
  local descriptions
  local topic
  local missing
  local stable_count=0

  bootstrap="$(local_kafka_bootstrap)"
  if [[ "$wait_sec" == "0" ]]; then
    return 0
  fi
  if ! [[ "$stable_polls" =~ ^[0-9]+$ ]] || [[ "$stable_polls" -lt 1 ]]; then
    stable_polls=1
  fi
  deadline=$((SECONDS + wait_sec))

  while ((SECONDS < deadline)); do
    missing=()
    descriptions="$(shared_kafka_exec kafka-topics --bootstrap-server "$bootstrap" --describe 2>/dev/null || true)"
    if [[ -n "$descriptions" ]]; then
      while read -r topic; do
        [[ -n "$topic" ]] || continue
        if ! grep -Eq "(^|[[:space:]])Topic:[[:space:]]+${topic}([[:space:]]|$)" <<< "$descriptions"; then
          missing+=("$topic")
        fi
      done < <(benchmark_kafka_topics_for_readiness)
    else
      missing+=("<broker-metadata>")
    fi

    if [[ "${#missing[@]}" == "0" ]]; then
      stable_count=$((stable_count + 1))
      if [[ "$stable_count" -ge "$stable_polls" ]]; then
        ok "Kafka benchmark topic metadata visible: stable_polls=${stable_count}"
        return 0
      fi
    else
      stable_count=0
    fi
    sleep "$poll_sec"
  done

  die "Kafka benchmark topic metadata not ready within ${wait_sec}s: ${missing[*]:-unknown}"
}

local_wait_for_kafka_idempotent_producer() {
  local bootstrap
  local topic="${FDB_KAFKA_READY_PROBE_TOPIC:-fdb-benchmark-probe}"
  local retention_ms="${FDB_KAFKA_READY_PROBE_RETENTION_MS:-600000}"
  local wait_sec="${FDB_KAFKA_PRODUCER_READY_WAIT_SEC:-180}"
  local poll_sec="${FDB_KAFKA_PRODUCER_READY_POLL_SEC:-2}"
  local deadline

  bootstrap="$(local_kafka_bootstrap)"
  if [[ "$wait_sec" == "0" ]]; then
    return 0
  fi

  shared_kafka_exec kafka-topics \
    --bootstrap-server "$bootstrap" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions 1 \
    --replication-factor 1 \
    --config cleanup.policy=delete \
    --config "retention.ms=$retention_ms" >/dev/null

  deadline=$((SECONDS + wait_sec))
  while ((SECONDS < deadline)); do
    if shared_kafka_exec kafka-producer-perf-test \
      --topic "$topic" \
      --num-records 1 \
      --record-size 16 \
      --throughput 1 \
      --producer-props \
        "bootstrap.servers=$bootstrap" \
        acks=all \
        enable.idempotence=true \
        max.block.ms=10000 \
        request.timeout.ms=10000 \
        delivery.timeout.ms=15000 >/dev/null 2>&1; then
      ok "Kafka idempotent producer probe OK"
      return 0
    fi
    sleep "$poll_sec"
  done

  die "Kafka idempotent producer probe did not become ready within ${wait_sec}s"
}

local_wait_for_kafka_benchmark_ready() {
  log "waiting for Kafka benchmark topics and producer path to be ready"
  local_wait_for_kafka_topic_metadata
  local_wait_for_kafka_idempotent_producer
}

run_hdfs_prune_with() {
  local runner=$1
  local hive_path=${FDB_HIVE_WAREHOUSE_PATH:-/warehouse/fdb/cell_kpi}
  local iceberg_path=${FDB_ICEBERG_WAREHOUSE_PATH:-/warehouse/iceberg}/${FDB_ICEBERG_DATABASE:-iceberg_db}/${FDB_ICEBERG_TABLE:-cell_kpi}
  local parquet_retention_ms=${FDB_HDFS_KPI_RETENTION_MS:-86400000}
  local iceberg_retention_ms=${FDB_ICEBERG_FILE_RETENTION_MS:-86400000}
  local inprogress_retention_ms=${FDB_HDFS_INPROGRESS_RETENTION_MS:-$parquet_retention_ms}
  local iceberg_inprogress_retention_ms=${FDB_ICEBERG_INPROGRESS_RETENTION_MS:-$iceberg_retention_ms}
  local parquet_days
  local iceberg_days
  parquet_days="$(retention_days_floor "$parquet_retention_ms")"
  iceberg_days="$(retention_days_floor "$iceberg_retention_ms")"

  log "pruning stale HDFS in-progress KPI files"
  prune_hdfs_listing_matches "$runner" "$hive_path" "*.inprogress*" "$inprogress_retention_ms"

  log "pruning HDFS KPI parquet files older than ${parquet_days}d"
  prune_hdfs_listing_matches "$runner" "$hive_path" "*.parquet" "$parquet_retention_ms"

  log "pruning stale Iceberg orphan in-progress files"
  prune_hdfs_listing_matches "$runner" "$iceberg_path" "*.inprogress*" "$iceberg_inprogress_retention_ms"

  if [[ "${FDB_ICEBERG_PRUNE_DATA_FILES:-0}" == "1" ]]; then
    warn "FDB_ICEBERG_PRUNE_DATA_FILES=1 deletes Iceberg data files by mtime; use only after expiring snapshots"
    log "pruning old Iceberg data files older than ${iceberg_days}d"
    prune_hdfs_listing_matches "$runner" "$iceberg_path/data" "*.parquet" "$iceberg_retention_ms"
  else
    log "skipping Iceberg data-file mtime prune; expire snapshots with an Iceberg engine before deleting referenced files"
  fi
}

prepare_flink_hadoop_runtime() {
  local artifact="${FLINK_HADOOP_RUNTIME_ARTIFACT:-org.apache.flink:flink-shaded-hadoop-2-uber:2.8.3-10.0}"
  local jar="${FLINK_HADOOP_RUNTIME_JAR:-docker/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar}"

  if [[ -f "$jar" ]]; then
    return 0
  fi

  log "downloading Flink Hadoop runtime jar"
  mkdir -p "$(dirname "$jar")"

  local old_msys_no_pathconv_set=0
  local old_msys_no_pathconv=""
  if [[ "${MSYS_NO_PATHCONV+x}" == "x" ]]; then
    old_msys_no_pathconv_set=1
    old_msys_no_pathconv="$MSYS_NO_PATHCONV"
    unset MSYS_NO_PATHCONV
  fi

  maven_cmd -q dependency:copy \
    -Dartifact="$artifact" \
    -DoutputDirectory="$(dirname "$jar")" \
    -Dtransitive=false

  if [[ "$old_msys_no_pathconv_set" == "1" ]]; then
    export MSYS_NO_PATHCONV="$old_msys_no_pathconv"
  fi
}

remove_legacy_local_infra() {
  local legacy=(
    fdb-zookeeper
    fdb-kafka
    fdb-kafka-ui
    fdb-hms-postgres
    fdb-hive-metastore
    fdb-hive-server
    fdb-mysql
    fdb-prometheus
    fdb-grafana
  )
  local name
  local container_ids

  for name in "${legacy[@]}"; do
    container_ids="$(docker ps -aq --filter "name=^/${name}$")"
    if [[ -n "$container_ids" ]]; then
      docker rm -f "$name" >/dev/null 2>&1 || true
    fi
  done
}

local_check() {
  load_env_optional
  log "checking local docker compose configuration"
  docker compose -f docker/docker-compose.yml --profile e2e config >/dev/null
  ok "local docker compose configuration is valid"
}

local_up() {
  load_env_optional
  log "checking shared infrastructure network"
  if ! docker network inspect shared-data-infra >/dev/null 2>&1; then
    warn "shared-data-infra network is missing"
    warn "start shared infrastructure first:"
    warn "  cd $(shared_infra_dir) && sh scripts/infra-up.sh lakehouse lakehouse-tools streaming starrocks observability"
    exit 1
  fi

  prepare_flink_hadoop_runtime

  log "starting local project containers"
  docker compose -f docker/docker-compose.yml --profile e2e up -d \
    observability-api \
    frontend \
    jobmanager \
    taskmanager
}

local_init() {
  load_env_optional
  log "waiting for shared Kafka to be ready (up to 60s)"
  wait_for_command "shared Kafka OK" 30 2 \
    shared_kafka_exec kafka-broker-api-versions --bootstrap-server "$(local_kafka_bootstrap)"

  log "waiting for shared HiveServer2 to be ready (up to 90s)"
  wait_for_command "shared HiveServer2 OK" 45 2 \
    shared_lakehouse exec -T hive-server \
      beeline -u jdbc:hive2://localhost:10000/default -e "SELECT 1"

  log "waiting for shared StarRocks to be ready (up to 120s)"
  wait_for_command "shared StarRocks OK" 60 2 \
    shared_starrocks_mysql --no-database -e "SELECT 1"
  wait_for_command "shared StarRocks BE OK" 60 2 \
    shared_starrocks exec -T starrocks-be bash -lc "exec 3<>/dev/tcp/localhost/8040"

  log "preparing shared HDFS warehouse directories"
  shared_lakehouse exec -T namenode \
    hdfs dfs -fs "$(local_hdfs_uri)" -mkdir -p /warehouse/fdb/cell_kpi /warehouse/iceberg
  shared_lakehouse exec -T namenode \
    hdfs dfs -fs "$(local_hdfs_uri)" -chmod -R 777 /warehouse/fdb /warehouse/iceberg

  prepare_flink_hadoop_runtime

  log "creating Kafka topics"
  bash scripts/init-kafka-topics.sh

  log "initializing shared StarRocks tables"
  shared_starrocks_mysql --no-database < scripts/init-starrocks.sql
  ensure_local_starrocks_cell_kpi_connector_schema

  log "initializing shared Hive table"
  bash scripts/init-hive.sh

  ok "local dependencies initialized"
  docker compose -f docker/docker-compose.yml ps
}

local_reset_kafka_topics() {
  local bootstrap
  local topic
  local current_topics
  local all_deleted
  bootstrap="$(local_kafka_bootstrap)"

  if local_reset_kafka_topics_with_admin; then
    local_wait_for_kafka_benchmark_ready
    return 0
  else
    local admin_status=$?
    if [[ "$admin_status" == "2" && "${FDB_LOCAL_KAFKA_RESET_FALLBACK:-0}" != "1" ]]; then
      die "Kafka AdminClient topic reset failed"
    fi
  fi

  log "resetting shared Kafka benchmark topics"
  while read -r topic; do
    [[ -n "$topic" ]] || continue
    shared_kafka_exec kafka-topics \
      --bootstrap-server "$bootstrap" \
      --delete --if-exists \
      --topic "$topic" >/dev/null || true
  done < <(benchmark_kafka_topics)

  for _ in {1..30}; do
    current_topics="$(shared_kafka_exec kafka-topics --bootstrap-server "$bootstrap" --list 2>/dev/null || true)"
    all_deleted=1
    while read -r topic; do
      [[ -n "$topic" ]] || continue
      if grep -Fxq "$topic" <<< "$current_topics"; then
        all_deleted=0
        break
      fi
    done < <(benchmark_kafka_topics)
    [[ "$all_deleted" == "1" ]] && break
    sleep 1
  done

  bash scripts/init-kafka-topics.sh >/dev/null
  local_wait_for_kafka_benchmark_ready
}

local_prepare() {
  load_env_optional
  local_reset_kafka_topics

  case "${FDB_RESULT_SINK:-starrocks}" in
    starrocks)
      log "resetting shared StarRocks benchmark tables"
      reset_starrocks_sql | shared_starrocks_mysql
      ;;
    hive)
      log "resetting shared Hive HDFS benchmark outputs"
      reset_hdfs_hive_benchmark_outputs_with shared_hdfs_exec
      ;;
    iceberg)
      log "resetting shared Iceberg HDFS benchmark outputs"
      reset_hdfs_iceberg_benchmark_outputs_with shared_hdfs_exec
      ;;
    kafka|none)
      log "no shared storage reset required for ${FDB_RESULT_SINK:-starrocks} sink"
      ;;
    *)
      die "unsupported FDB_RESULT_SINK for local prepare: ${FDB_RESULT_SINK:-}"
      ;;
  esac

  ok "local benchmark data prepared"
}

local_submit() {
  local explicit_run_id="${FDB_RUN_ID:-}"
  local explicit_run_label="${FDB_RUN_LABEL:-}"
  local explicit_result_sink="${FDB_RESULT_SINK:-}"
  load_env_optional
  if [[ -n "$explicit_run_id" ]]; then
    export FDB_RUN_ID="$explicit_run_id"
  fi
  if [[ -n "$explicit_run_label" ]]; then
    export FDB_RUN_LABEL="$explicit_run_label"
  fi
  if [[ -n "$explicit_result_sink" ]]; then
    export FDB_RESULT_SINK="$explicit_result_sink"
  fi
  ensure_run_context
  local jar="${FDB_FLINK_JOB_JAR:-/opt/fdb/flink-job-0.1.0-SNAPSHOT.jar}"
  mkdir -p logs
  local submit_log="logs/local-flink-submit.out"
  local state_file="${FDB_LOCAL_STATE_FILE:-logs/local-current.env}"
  local state_dir
  local flink_job_id

  local_ensure_flink_slots_for_submit || die "Flink slots are not available for local submit"

  log "recreating observability-api with run context: ${FDB_RUN_ID}"
  docker compose -f docker/docker-compose.yml --profile e2e up -d --no-deps --force-recreate observability-api

  build_local_flink_env_args
  log "submitting local Flink job: $jar"
  if ! local_flink_submit_once "$jar" "$submit_log"; then
    flink_job_id="$(parse_local_submit_job_id "$submit_log")"
    if [[ -z "$flink_job_id" ]]; then
      flink_job_id="$(local_wait_for_late_submit_job_id "$submit_log")"
    fi
    if [[ -z "$flink_job_id" ]]; then
      if [[ "${FDB_LOCAL_FLINK_SUBMIT_RETRY_ON_UNKNOWN:-0}" != "1" ]]; then
        die "local Flink submit did not return JobID; not retrying unknown submit status"
      fi
      warn "local Flink submit failed or timed out without JobID; retrying once"
      sleep "${FDB_LOCAL_FLINK_SUBMIT_RETRY_SLEEP_SEC:-2}"
      log "retrying local Flink job submit: $jar"
      if ! local_flink_submit_once "$jar" "$submit_log"; then
        flink_job_id="$(parse_local_submit_job_id "$submit_log")"
        if [[ -z "$flink_job_id" ]]; then
          flink_job_id="$(local_wait_for_late_submit_job_id "$submit_log")"
        fi
        if [[ -z "$flink_job_id" && "${FDB_LOCAL_FLINK_SUBMIT_RECREATE_ON_RETRY:-0}" == "1" ]]; then
          warn "local Flink submit retry failed without JobID; recreating JobManager/TaskManager and retrying once"
          docker compose -f docker/docker-compose.yml --profile e2e up -d --force-recreate --no-deps jobmanager taskmanager
          local_ensure_flink_slots_for_submit || die "Flink slots are not available after local runtime recreate"
          log "retrying local Flink job submit after runtime recreate: $jar"
          if ! local_flink_submit_once "$jar" "$submit_log"; then
            flink_job_id="$(parse_local_submit_job_id "$submit_log")"
            if [[ -z "$flink_job_id" ]]; then
              flink_job_id="$(local_wait_for_late_submit_job_id "$submit_log")"
            fi
          fi
        fi
        [[ -n "$flink_job_id" ]] || die "local Flink submit failed after retry"
        warn "local Flink submit retry returned non-zero after JobID was observed: $flink_job_id"
      fi
    else
      warn "local Flink submit command returned non-zero after JobID was observed: $flink_job_id"
    fi
  fi

  state_dir="$(dirname "$state_file")"
  if [[ "$state_dir" != "." ]]; then
    mkdir -p "$state_dir"
  fi

  flink_job_id="$(parse_local_submit_job_id "$submit_log")"
  if [[ -z "$flink_job_id" ]]; then
    flink_job_id="$(local_wait_for_late_submit_job_id "$submit_log" || true)"
  fi
  {
    printf 'FDB_LOCAL_SUBMITTED_AT=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf 'FDB_LOCAL_ENV_FILE=%q\n' "${FDB_ENV_FILE:-.env}"
    printf 'FDB_LOCAL_FLINK_JOB_ID=%q\n' "$flink_job_id"
  } > "$state_file"
  write_current_run_env "$state_file"

  if [[ -z "$flink_job_id" ]]; then
    warn "wrote local runtime state without parsed Flink job id: $state_file"
    warn "set FDB_FLINK_JOB_ID explicitly when stopping this job"
  else
    ok "wrote local runtime state: $state_file"
  fi
}

local_stop() {
  local requested_report_on_stop="${FDB_REPORT_ON_STOP:-}"
  load_env_optional
  if [[ -n "$requested_report_on_stop" ]]; then
    export FDB_REPORT_ON_STOP="$requested_report_on_stop"
  fi
  local submit_log="logs/local-flink-submit.out"
  local job_id="${FDB_FLINK_JOB_ID:-}"
  local state_file="${FDB_LOCAL_STATE_FILE:-logs/local-current.env}"
  local stop_status=0

  if [[ -z "$job_id" && -f "$state_file" ]]; then
    # shellcheck disable=SC1090
    source "$state_file"
    job_id="${FDB_LOCAL_FLINK_JOB_ID:-}"
  fi

  if [[ -z "$job_id" && -f "$submit_log" ]]; then
    job_id="$(awk '/JobID/ {job_id=$NF} END {print job_id}' "$submit_log")"
  fi

  if [[ -z "$job_id" ]]; then
    die "no local Flink job id found; set FDB_FLINK_JOB_ID or run local submit first"
  fi

  log "cancelling local Flink job: $job_id"
  local cancel_timeout_sec="${FDB_FLINK_REST_CANCEL_TIMEOUT_SEC:-10}"
  if ! curl -fsS --max-time "$cancel_timeout_sec" -X PATCH "${FDB_FLINK_REST_URL:-http://localhost:8081}/jobs/${job_id}?mode=cancel" >/dev/null; then
    warn "Flink REST cancel failed; falling back to CLI cancel"
    docker exec --user flink fdb-flink-jobmanager flink cancel "$job_id" || stop_status=$?
  fi
  if [[ "$stop_status" -eq 0 ]]; then
    local_wait_for_job_terminal "$job_id" || stop_status=$?
  fi

  if [[ "${FDB_REPORT_ON_STOP:-false}" == "true" ]]; then
    run_report || true
  fi

  return "$stop_status"
}

local_smoke() {
  load_env_optional
  # shellcheck source=scripts/e2e-summary-lib.sh
  source "$ROOT_DIR/scripts/e2e-summary-lib.sh"
  PIDS=()
  summary_init

  cleanup() {
    local status=$?
    summary_finalize "$status" || true
    for pid in "${PIDS[@]:-}"; do kill "$pid" >/dev/null 2>&1 || true; done
    if [ "$status" -eq 0 ] && e2e_keep_running_on_success; then
      echo "[e2e] Completed; keeping containers running because FDB_E2E_KEEP_RUNNING_ON_SUCCESS=${FDB_E2E_KEEP_RUNNING_ON_SUCCESS}"
      observability_links
    elif [ "$status" -eq 0 ] || [ "${FDB_E2E_KEEP_RUNNING_ON_FAIL:-0}" != "1" ]; then
      bash scripts/deploy.sh local down >/dev/null 2>&1 || true
    else
      echo "[e2e] Failed; keeping containers running because FDB_E2E_KEEP_RUNNING_ON_FAIL=1"
      COMPOSE_PROFILES=e2e docker compose -f docker/docker-compose.yml ps || true
    fi
  }
  trap cleanup EXIT

  local_smoke_wait_for() {
    local description=$1
    local command=$2
    local attempts=${3:-60}
    for _ in $(seq 1 "$attempts"); do
      if eval "$command"; then echo "[ok] $description"; return 0; fi
      sleep 2
    done
    echo "[fail] timed out: $description"
    return 1
  }

  KPI_5M_WAIT_ATTEMPTS=${FDB_E2E_5M_WAIT_ATTEMPTS:-240}
  ICEBERG_KPI_ROOT=${FDB_E2E_ICEBERG_KPI_ROOT:-/warehouse/iceberg/${FDB_ICEBERG_DATABASE:-iceberg_db}/${FDB_ICEBERG_TABLE:-cell_kpi}}

  echo "[e2e] Building jars..."
  maven_cmd package ${FDB_E2E_MAVEN_ARGS:--DskipTests}
  summary_section "Build"
  summary_line "Build" "maven package" "success"

  # Git Bash rewrites Unix-like arguments such as /opt/fdb/... into Windows paths
  # unless this is disabled. Docker commands below use container-internal paths.
  export MSYS_NO_PATHCONV=1

  echo "[e2e] Starting infrastructure and Flink containers..."
  bash scripts/deploy.sh local up
  bash scripts/deploy.sh local init
  local_smoke_wait_for "Flink JobManager" "curl -fsS http://localhost:8081/overview >/dev/null"
  local_smoke_wait_for "HiveServer2" "shared_hive_exec beeline -u jdbc:hive2://localhost:10000/default -e 'SELECT 1' >/dev/null 2>&1"
  local_smoke_wait_for "Observability API" "curl -fsS \"$(observability_api_url)/metrics\" >/dev/null"
  local_smoke_wait_for "Prometheus" "curl -fsS \"$(observability_prometheus_url)/-/ready\" >/dev/null"
  summary_section "Infrastructure"
  summary_command "Infrastructure" "running containers" "COMPOSE_PROFILES=e2e docker compose -f docker/docker-compose.yml ps --services --filter status=running | wc -l | tr -d ' '"
  summary_command "Infrastructure" "Kafka topics" "shared_kafka_exec kafka-topics --bootstrap-server ${FDB_KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092} --list | wc -l | tr -d ' '"
  summary_section "Observability"
  summary_observability
  observability_links

  echo "[e2e] Publishing topology and starting simulators..."
  local host_kafka_bootstrap="${FDB_KAFKA_HOST_BOOTSTRAP:-localhost:9092}"
  mkdir -p logs
  FDB_KAFKA_BOOTSTRAP="$host_kafka_bootstrap" java -jar topology-service/target/topology-service-0.1.0-SNAPSHOT.jar > logs/topology.log 2>&1
  FDB_KAFKA_BOOTSTRAP="$host_kafka_bootstrap" java -jar simulator/target/simulator-0.1.0-SNAPSHOT.jar cfg > logs/cfg.log 2>&1 & PIDS+=("$!")
  FDB_KAFKA_BOOTSTRAP="$host_kafka_bootstrap" java -jar simulator/target/simulator-0.1.0-SNAPSHOT.jar pm > logs/pm.log 2>&1 & PIDS+=("$!")
  FDB_KAFKA_BOOTSTRAP="$host_kafka_bootstrap" java -jar simulator/target/simulator-0.1.0-SNAPSHOT.jar chr > logs/chr.log 2>&1 & PIDS+=("$!")
  summary_section "Data Generation"
  summary_command "Data Generation" "topology log lines" "wc -l < logs/topology.log | tr -d ' '"
  summary_line "Data Generation" "simulator processes" "${#PIDS[@]}"
  summary_code_logs "Data Generation" cat logs/topology.log logs/cfg.log logs/pm.log logs/chr.log

  echo "[e2e] Submitting Flink job..."
  local_submit
  FLINK_JOB_ID="$(awk '/JobID/ {job_id=$NF} END {print job_id}' logs/local-flink-submit.out)"
  [ -n "$FLINK_JOB_ID" ] || { echo "[fail] Unable to parse Flink JobID"; exit 1; }
  summary_section "Flink Submit"
  summary_line "Flink Submit" "job id" "$FLINK_JOB_ID"

  local_smoke_wait_for "CFG baseline messages" "shared_kafka_exec kafka-run-class kafka.tools.GetOffsetShell --broker-list ${FDB_KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092} --topic cfg-config | grep -Eq ':[1-9][0-9]*$'"
  local_smoke_wait_for "PM messages" "shared_kafka_exec kafka-run-class kafka.tools.GetOffsetShell --broker-list ${FDB_KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092} --topic pm-stats | grep -Eq ':[1-9][0-9]*$'"
  local_smoke_wait_for "CHR messages" "shared_kafka_exec kafka-run-class kafka.tools.GetOffsetShell --broker-list ${FDB_KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092} --topic chr-events | grep -Eq ':[1-9][0-9]*$'"
  summary_section "Kafka Input"
  summary_kafka_topic "cfg-config"
  summary_kafka_topic "pm-stats"
  summary_kafka_topic "chr-events"
  summary_kafka_topic "cell-kpi-1m"
  summary_kafka_topic "cell-kpi-5m"
  summary_kafka_topic "cell-anomaly-events"
  summary_kafka_topic "user-anomaly-events"
  summary_kafka_topic "grid-anomaly-events"
  local_smoke_wait_for "1m KPI rows in StarRocks" "shared_starrocks_mysql -N -e \"SELECT COUNT(*) FROM cell_kpi WHERE window_kind='MIN_1'\" | grep -Eq '^[1-9][0-9]*$'" 90
  summary_section "StarRocks KPI"
  summary_starrocks_kpi
  local_smoke_wait_for "runtime stage metric messages" "shared_kafka_exec kafka-run-class kafka.tools.GetOffsetShell --broker-list ${FDB_KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092} --topic fdb-stage-metrics | grep -Eq ':[1-9][0-9]*$'" 60
  local_smoke_wait_for "nonzero observability metrics" "curl -fsS \"$(observability_api_url)/metrics\" | awk '/^fdb_stage_out_eps|^fdb_source_eps/ { if (\$2+0 > 0) found=1 } END { exit(found ? 0 : 1) }'" 60
  local_smoke_wait_for "Prometheus fdb_stage_out_eps" "curl -fsS \"$(observability_prometheus_url)/api/v1/query?query=fdb_stage_out_eps%20%3E%200\" | grep -q '\"metric\"'" 60
  summary_section "Observability"
  summary_observability
  if [ "${FDB_DYNAMIC_BALANCING_ENABLED:-false}" = "true" ]; then
    local_smoke_wait_for "heartbeat messages" "shared_kafka_exec kafka-run-class kafka.tools.GetOffsetShell --broker-list ${FDB_KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092} --topic lb-heartbeat | grep -Eq ':[1-9][0-9]*$'"
    summary_section "Load Balancing"
    summary_kafka_topic "lb-heartbeat"
    summary_kafka_topic "lb-routing"
  fi
  summary_flink_job
  summary_section "Flink DAG"
  if [ "${FDB_DYNAMIC_BALANCING_ENABLED:-false}" = "true" ]; then
    summary_flink_dynamic_balancing_vertices "$FLINK_JOB_ID"
  else
    local_smoke_wait_for "Flink DAG has no dynamic-balancing vertices" "assert_flink_dynamic_balancing_vertices_absent \"$FLINK_JOB_ID\"" 30
    summary_flink_dynamic_balancing_vertices "$FLINK_JOB_ID"
  fi
  summary_code_logs "Flink Code" docker logs fdb-flink-taskmanager
  local_smoke_wait_for "Parquet KPI files" "shared_hdfs_exec -find /warehouse/fdb/cell_kpi -name '*.parquet' | grep -q ."
  summary_section "Parquet KPI"
  summary_parquet_kpi "/warehouse/fdb/cell_kpi"
  local_smoke_wait_for "Iceberg metadata" "shared_hdfs_exec -find \"$ICEBERG_KPI_ROOT/metadata\" -name '*.metadata.json' | grep -q ."
  local_smoke_wait_for "Iceberg 1m KPI data files" "shared_hdfs_exec -find \"$ICEBERG_KPI_ROOT/data\" -name '*.parquet' | grep 'window_kind=MIN_1' | grep -q ."
  local_smoke_wait_for "Iceberg 5m KPI data files" "shared_hdfs_exec -find \"$ICEBERG_KPI_ROOT/data\" -name '*.parquet' | grep 'window_kind=MIN_5' | grep -q ." "$KPI_5M_WAIT_ATTEMPTS"
  local_smoke_wait_for "5m KPI rows in StarRocks" "shared_starrocks_mysql -N -e \"SELECT COUNT(*) FROM cell_kpi WHERE window_kind='MIN_5'\" | grep -Eq '^[1-9][0-9]*$'" "$KPI_5M_WAIT_ATTEMPTS"
  local_smoke_wait_for "cell anomaly table queryable in StarRocks" "summary_starrocks_scalar \"SELECT COUNT(*) FROM cell_anomaly_events\" | grep -Eq '^[0-9]+$'" 30
  local_smoke_wait_for "user anomaly table queryable in StarRocks" "summary_starrocks_scalar \"SELECT COUNT(*) FROM user_anomaly_events\" | grep -Eq '^[0-9]+$'" 30
  local_smoke_wait_for "grid anomaly table queryable in StarRocks" "summary_starrocks_scalar \"SELECT COUNT(*) FROM grid_anomaly_events\" | grep -Eq '^[0-9]+$'" 30
  summary_section "StarRocks"
  summary_starrocks_query "KPI 1m rows" "SELECT COUNT(*) FROM cell_kpi WHERE window_kind='MIN_1'"
  summary_starrocks_query "KPI 5m rows" "SELECT COUNT(*) FROM cell_kpi WHERE window_kind='MIN_5'"
  summary_starrocks_query "Cell anomaly rows" "SELECT COUNT(*) FROM cell_anomaly_events"
  summary_starrocks_query "User anomaly rows" "SELECT COUNT(*) FROM user_anomaly_events"
  summary_starrocks_query "Grid anomaly rows" "SELECT COUNT(*) FROM grid_anomaly_events"
  summary_section "Iceberg KPI"
  summary_iceberg_kpi "$ICEBERG_KPI_ROOT"
  summary_section "Hive/Iceberg Compare"
  summary_hive_iceberg_compare "/warehouse/fdb/cell_kpi" "$ICEBERG_KPI_ROOT/data"
  local_smoke_wait_for "1m sink latency runtime samples" "assert_sink_latency_runtime_samples kpi_1m" 60
  local_smoke_wait_for "5m sink latency runtime samples" "assert_sink_latency_runtime_samples kpi_5m" 60
  summary_section "Sink Performance"
  summary_sink_performance

  echo "[e2e] Initializing Hive table and verifying query..."
  bash scripts/init-hive.sh
  shared_hive_exec beeline -u jdbc:hive2://localhost:10000/default \
    -e 'MSCK REPAIR TABLE fdb.cell_kpi; SELECT COUNT(*) FROM fdb.cell_kpi;'
  summary_section "Hive KPI"
  summary_hive_kpi

  echo "[e2e] Completed successfully"
}

local_down() {
  if [[ "${1:-}" == "--clean" ]]; then
    log "stopping and removing project containers plus data volumes"
    docker compose -f docker/docker-compose.yml --profile e2e down -v
    remove_legacy_local_infra
    rm -rf docker/data
  else
    log "stopping project containers"
    docker compose -f docker/docker-compose.yml --profile e2e down
  fi
}

local_prune() {
  load_env_optional
  if [[ "${FDB_PRUNE_DRY_RUN:-0}" == "1" ]]; then
    log "prune dry run"
    prune_starrocks_sql
    echo "HDFS prune: ${FDB_HIVE_WAREHOUSE_PATH:-/warehouse/fdb/cell_kpi} retention=${FDB_HDFS_KPI_RETENTION_MS:-86400000}ms"
    echo "Iceberg prune: ${FDB_ICEBERG_WAREHOUSE_PATH:-/warehouse/iceberg}/${FDB_ICEBERG_DATABASE:-iceberg_db}/${FDB_ICEBERG_TABLE:-cell_kpi} retention=${FDB_ICEBERG_FILE_RETENTION_MS:-86400000}ms"
    return 0
  fi

  log "pruning shared StarRocks rows"
  prune_starrocks_sql | shared_starrocks_mysql

  log "pruning shared HDFS files"
  run_hdfs_prune_with shared_hdfs_exec
  ok "local storage prune completed"
}

local_status() {
  load_env_optional
  echo "[status] containers"
  docker ps --format "table {{.Names}}\t{{.Status}}" |
    grep -E "^(NAMES|fdb-|shared-data-infra-(kafka|starrocks|hive|namenode|datanode|prometheus))" || true

  echo "[status] flink jobs"
  curl -fsS http://localhost:8081/jobs/overview || true
  echo

  echo "[status] starrocks"
  shared_starrocks_mysql -N -e "
    SELECT 'cell_kpi', COUNT(*), MIN(window_start_ts), MAX(window_start_ts) FROM cell_kpi;
    SELECT 'cell_anomaly_events', COUNT(*), MIN(event_ts), MAX(event_ts) FROM cell_anomaly_events;
    SELECT 'user_anomaly_events', COUNT(*), MIN(event_ts), MAX(event_ts) FROM user_anomaly_events;
    SELECT 'grid_anomaly_events', COUNT(*), MIN(event_ts), MAX(event_ts) FROM grid_anomaly_events;
  " || true

  echo "[status] hdfs"
  shared_hdfs_exec -count -h "${FDB_HIVE_WAREHOUSE_PATH:-/warehouse/fdb/cell_kpi}" || true
  shared_hdfs_exec -count -h "${FDB_ICEBERG_WAREHOUSE_PATH:-/warehouse/iceberg}/${FDB_ICEBERG_DATABASE:-iceberg_db}/${FDB_ICEBERG_TABLE:-cell_kpi}" || true
}

external_require_env() {
  local name=$1
  local value="${!name:-}"

  if [[ -n "$value" ]]; then
    ok "env set: $name"
  else
    warn_or_fail "env missing: $name"
  fi
}

require_env() {
  local name=$1
  local value="${!name:-}"

  [[ -n "$value" ]] || die "env missing: $name"
}

external_flink_bin() {
  echo "${FLINK_HOME:-}/bin/flink"
}

external_starrocks_host() {
  echo "${FDB_STARROCKS_FE_ENDPOINT:-}" | awk -F: '{print $1}'
}

external_starrocks_port() {
  echo "${FDB_STARROCKS_FE_ENDPOINT:-}" | awk -F: '{print ($2 == "" ? 9030 : $2)}'
}

external_starrocks_load_port() {
  echo "${FDB_STARROCKS_LOAD_PORT:-8030}"
}

external_mysql_select_one() {
  local host=$1
  local port=$2
  local user=$3
  local password=$4
  local database=$5
  local args=(-h "$host" -P "$port" -u "$user")

  if [[ -n "$password" ]]; then
    args+=("-p$password")
  fi
  if [[ -n "$database" ]]; then
    args+=("$database")
  fi

  mysql "${args[@]}" -e "SELECT 1" >/dev/null 2>&1
}

external_mysql_run_file() {
  local host=$1
  local port=$2
  local user=$3
  local password=$4
  local database=$5
  local sql_file=$6
  local args=(-h "$host" -P "$port" -u "$user")

  [[ -f "$sql_file" ]] || die "SQL file not found: $sql_file"

  if [[ -n "$password" ]]; then
    args+=("-p$password")
  fi
  if [[ -n "$database" ]]; then
    args+=("$database")
  fi

  mysql "${args[@]}" < "$sql_file"
}

external_mysql_run_sql() {
  local host=$1
  local port=$2
  local user=$3
  local password=$4
  local database=$5
  local args=(-h "$host" -P "$port" -u "$user")

  if [[ -n "$password" ]]; then
    args+=("-p$password")
  fi
  if [[ -n "$database" ]]; then
    args+=("$database")
  fi

  mysql "${args[@]}"
}

external_starrocks_column_exists() {
  local column=$1
  external_mysql_run_sql \
    "$(external_starrocks_host)" \
    "$(external_starrocks_port)" \
    "${FDB_STARROCKS_USER:-root}" \
    "${FDB_STARROCKS_PASSWORD:-}" \
    "${FDB_STARROCKS_DATABASE:-fdb}" <<SQL | grep -Eq "^${column}[[:space:]]"
SHOW COLUMNS FROM cell_kpi LIKE '${column}';
SQL
}

ensure_external_starrocks_cell_kpi_connector_schema() {
  local column
  local alter_sql
  local columns=(join_quality rsrp_sample_count sinr_sample_count attach_attempts)

  for column in "${columns[@]}"; do
    if ! external_starrocks_column_exists "$column"; then
      alter_sql="$(starrocks_cell_kpi_connector_column_sql "$column")"
      log "adding external StarRocks cell_kpi connector column: $column"
      external_mysql_run_sql \
        "$(external_starrocks_host)" \
        "$(external_starrocks_port)" \
        "${FDB_STARROCKS_USER:-root}" \
        "${FDB_STARROCKS_PASSWORD:-}" \
        "${FDB_STARROCKS_DATABASE:-fdb}" <<SQL
ALTER TABLE cell_kpi ${alter_sql};
SQL
    fi
  done
}

external_hdfs_exec() {
  hdfs dfs -fs "$FDB_HDFS_URI" "$@"
}

external_hive_cell_kpi_location() {
  if [[ -n "${FDB_HIVE_CELL_KPI_LOCATION:-}" ]]; then
    echo "$FDB_HIVE_CELL_KPI_LOCATION"
  elif [[ -n "${FDB_HIVE_WAREHOUSE_PATH:-}" ]]; then
    echo "${FDB_HDFS_URI%/}$FDB_HIVE_WAREHOUSE_PATH"
  elif [[ -n "${FDB_HIVE_WAREHOUSE:-}" ]]; then
    echo "${FDB_HIVE_WAREHOUSE%/}/cell_kpi"
  else
    echo "${FDB_HDFS_URI%/}/warehouse/fdb/cell_kpi"
  fi
}

external_hive_warehouse_location() {
  local cell_location
  local root_path

  if [[ -n "${FDB_HIVE_WAREHOUSE:-}" ]]; then
    echo "$FDB_HIVE_WAREHOUSE"
  elif [[ -n "${FDB_HIVE_WAREHOUSE_ROOT:-}" ]]; then
    echo "${FDB_HDFS_URI%/}$FDB_HIVE_WAREHOUSE_ROOT"
  else
    cell_location="$(external_hive_cell_kpi_location)"
    root_path="${cell_location%/cell_kpi}"
    if [[ "$root_path" == "$cell_location" ]]; then
      root_path="${cell_location%/*}"
    fi
    echo "$root_path"
  fi
}

external_hdfs_path_from_location() {
  local location=$1
  local path="$location"

  if [[ "$path" =~ ^[A-Za-z][A-Za-z0-9+.-]*:// ]]; then
    path="/${path#*://*/}"
  fi
  [[ "$path" == /* ]] || path="/$path"
  echo "$path"
}

external_apply_runtime_defaults() {
  if [[ -z "${FDB_STARROCKS_JDBC_URL:-}" && -n "${FDB_STARROCKS_FE_ENDPOINT:-}" ]]; then
    export FDB_STARROCKS_JDBC_URL="jdbc:mysql://$(external_starrocks_host):$(external_starrocks_port)/${FDB_STARROCKS_DATABASE:-fdb}?rewriteBatchedStatements=true&useServerPrepStmts=false"
  fi
  if [[ -z "${FDB_STARROCKS_CONNECTOR_JDBC_URL:-}" && -n "${FDB_STARROCKS_FE_ENDPOINT:-}" ]]; then
    export FDB_STARROCKS_CONNECTOR_JDBC_URL="jdbc:mysql://$(external_starrocks_host):$(external_starrocks_port)"
  fi
  if [[ -z "${FDB_STARROCKS_LOAD_URL:-}" && -n "${FDB_STARROCKS_FE_ENDPOINT:-}" ]]; then
    export FDB_STARROCKS_LOAD_URL="$(external_starrocks_host):$(external_starrocks_load_port)"
  fi
  if [[ -z "${FDB_STARROCKS_SINK_SEMANTIC:-}" ]]; then
    export FDB_STARROCKS_SINK_SEMANTIC="exactly-once"
  fi
  if [[ -z "${FDB_HIVE_WAREHOUSE:-}" && -n "${FDB_HDFS_URI:-}" ]]; then
    export FDB_HIVE_WAREHOUSE
    FDB_HIVE_WAREHOUSE="$(external_hive_warehouse_location)"
  fi
  if [[ -z "${FDB_ICEBERG_WAREHOUSE:-}" && -n "${FDB_HDFS_URI:-}" ]]; then
    export FDB_ICEBERG_WAREHOUSE="${FDB_HDFS_URI%/}$(external_iceberg_warehouse_path)"
  fi
  if [[ -z "${FDB_ICEBERG_METASTORE_URI:-}" && -n "${FDB_HIVE_METASTORE_URI:-}" ]]; then
    export FDB_ICEBERG_METASTORE_URI="$FDB_HIVE_METASTORE_URI"
  fi
  if [[ -z "${FDB_FLINK_CHECKPOINT_DIR:-}" && -n "${FDB_HDFS_URI:-}" ]]; then
    export FDB_FLINK_CHECKPOINT_DIR="${FDB_HDFS_URI%/}$(external_flink_checkpoint_path)"
  fi
}

external_iceberg_warehouse_path() {
  if [[ -n "${FDB_ICEBERG_WAREHOUSE_PATH:-}" ]]; then
    echo "$(external_hdfs_path_from_location "$FDB_ICEBERG_WAREHOUSE_PATH")"
  elif [[ -n "${FDB_ICEBERG_WAREHOUSE:-}" ]]; then
    echo "$(external_hdfs_path_from_location "$FDB_ICEBERG_WAREHOUSE")"
  else
    echo "/warehouse/iceberg"
  fi
}

external_flink_checkpoint_path() {
  if [[ -n "${FDB_FLINK_CHECKPOINT_PATH:-}" ]]; then
    echo "$(external_hdfs_path_from_location "$FDB_FLINK_CHECKPOINT_PATH")"
  elif [[ -n "${FDB_FLINK_CHECKPOINT_DIR:-}" ]]; then
    echo "$(external_hdfs_path_from_location "$FDB_FLINK_CHECKPOINT_DIR")"
  else
    echo "/flink-data-balance/checkpoints"
  fi
}

external_init_hive() {
  local schema_file="docs/hive-schema.q"
  local tmp_file
  local location
  local escaped_location

  [[ -f "$schema_file" ]] || die "Hive schema file not found: $schema_file"

  tmp_file="$(mktemp)"
  location="$(external_hive_cell_kpi_location)"
  escaped_location="${location//&/\\&}"
  if sed "s#hdfs://namenode:8020/warehouse/fdb/cell_kpi#$escaped_location#g" "$schema_file" > "$tmp_file"; then
    :
  else
    local status=$?
    rm -f "$tmp_file"
    return "$status"
  fi

  if beeline -u "$FDB_HIVE_JDBC_URL" -f "$tmp_file"; then
    rm -f "$tmp_file"
  else
    local status=$?
    rm -f "$tmp_file"
    return "$status"
  fi
}

create_external_topic() {
  local name=$1
  local partitions=$2
  local cleanup=$3
  local retention_ms=${4:-}
  local retention_bytes=${5:-${FDB_RETENTION_BYTES:-}}
  local segment_ms=${FDB_KAFKA_SEGMENT_MS:-}
  local extra=()
  local config_values=("cleanup.policy=$cleanup")

  if [[ -n "$retention_ms" && "$cleanup" == "delete" ]]; then
    extra+=(--config "retention.ms=$retention_ms")
    config_values+=("retention.ms=$retention_ms")
    if [[ -n "$segment_ms" ]]; then
      extra+=(--config "segment.ms=$segment_ms")
      config_values+=("segment.ms=$segment_ms")
    fi
    if [[ -n "$retention_bytes" ]]; then
      extra+=(--config "retention.bytes=$retention_bytes")
      config_values+=("retention.bytes=$retention_bytes")
    fi
  fi

  kafka-topics \
    --bootstrap-server "$FDB_KAFKA_BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "${FDB_KAFKA_REPLICATION_FACTOR:-1}" \
    --config "cleanup.policy=$cleanup" \
    "${extra[@]}"

  kafka-configs \
    --bootstrap-server "$FDB_KAFKA_BOOTSTRAP" \
    --alter \
    --entity-type topics \
    --entity-name "$name" \
    --add-config "$(IFS=,; printf '%s' "${config_values[*]}")" >/dev/null
}

EXTERNAL_FLINK_ENV_ARGS=()

build_external_flink_env_args() {
  local key
  local value
  local env_keys=(
    FDB_KAFKA_BOOTSTRAP
    FDB_KAFKA_FETCH_MAX_BYTES
    FDB_KAFKA_MAX_PARTITION_FETCH_BYTES
    FDB_KAFKA_MAX_POLL_RECORDS
    FDB_STARROCKS_FE_ENDPOINT
    FDB_STARROCKS_JDBC_URL
    FDB_STARROCKS_CONNECTOR_JDBC_URL
    FDB_STARROCKS_LOAD_URL
    FDB_STARROCKS_USER
    FDB_STARROCKS_DATABASE
    FDB_STARROCKS_SINK_SEMANTIC
    FDB_STARROCKS_SINK_LABEL_PREFIX
    FDB_RESULT_SINK
    FDB_DLQ_ENABLED
    FDB_HIVE_WAREHOUSE
    FDB_ICEBERG_ENABLED
    FDB_ICEBERG_WAREHOUSE
    FDB_ICEBERG_CATALOG
    FDB_ICEBERG_DATABASE
    FDB_ICEBERG_TABLE
    FDB_ICEBERG_METASTORE_URI
    FDB_FLINK_CHECKPOINT_DIR
    FDB_FLINK_CHECKPOINT_INTERVAL_MS
    FDB_FLINK_PARALLELISM
    FDB_FLINK_TASKMANAGER_MEMORY
    FDB_FLINK_TASKMANAGER_SLOTS
    FDB_FLINK_RETAINED_CHECKPOINTS
    FDB_FLINK_HEARTBEAT_TIMEOUT_MS
    FDB_FLINK_HEARTBEAT_INTERVAL_MS
    FDB_FLINK_PEKKO_ASK_TIMEOUT
    FDB_FLINK_TASKMANAGER_MANAGED_FRACTION
    FDB_METRICS_TOPIC
    FDB_METRICS_ENABLED
    FDB_METRICS_HISTORY_ENABLED
    FDB_METRICS_EMIT_INTERVAL_MS
    FDB_RUN_ID
    FDB_RUN_LABEL
    FDB_E2E_SUMMARY
  )

  EXTERNAL_FLINK_ENV_ARGS=()
  for key in "${env_keys[@]}"; do
    value="${!key:-}"
    if [[ -n "$value" || "${!key+x}" == "x" ]]; then
      EXTERNAL_FLINK_ENV_ARGS+=("-Dcontainerized.master.env.$key=$value")
      EXTERNAL_FLINK_ENV_ARGS+=("-Dcontainerized.taskmanager.env.$key=$value")
    fi
  done

  if [[ -n "${FDB_FLINK_SECRET_ENV_KEYS:-}" ]]; then
    warn "FDB_FLINK_SECRET_ENV_KEYS propagates secret values through Flink CLI/YARN metadata; prefer cluster-side secret injection"
    local secret_key
    local secret_keys=()
    read -r -a secret_keys <<< "$FDB_FLINK_SECRET_ENV_KEYS"
    for secret_key in "${secret_keys[@]}"; do
      value="${!secret_key:-}"
      if [[ -n "$value" ]]; then
        EXTERNAL_FLINK_ENV_ARGS+=("-Dcontainerized.master.env.$secret_key=$value")
        EXTERNAL_FLINK_ENV_ARGS+=("-Dcontainerized.taskmanager.env.$secret_key=$value")
      else
        warn "secret env key requested but not set: $secret_key"
      fi
    done
  fi
}

append_args_from_file() {
  local target_name=$1
  local file=$2
  local line

  [[ -f "$file" ]] || die "args file not found: $file"

  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ -z "$line" || "$line" == \#* ]] && continue
    case "$target_name" in
      flink) EXTERNAL_FLINK_FILE_ARGS+=("$line") ;;
      cancel) EXTERNAL_CANCEL_FILE_ARGS+=("$line") ;;
      *) die "unsupported args target: $target_name" ;;
    esac
  done < "$file"
}

record_external_submit_output() {
  local output_file=$1
  local state_file="${FDB_EXTERNAL_STATE_FILE:-logs/external-yarn-current.env}"
  local state_dir
  local flink_job_id
  local yarn_app_id

  state_dir="$(dirname "$state_file")"
  if [[ "$state_dir" != "." ]]; then
    mkdir -p "$state_dir"
  fi

  flink_job_id="$(awk '/JobID|Job ID|job id/ {job_id=$NF} END {print job_id}' "$output_file")"
  yarn_app_id="$(grep -Eo 'application_[0-9]+_[0-9]+' "$output_file" | tail -1 || true)"

  {
    printf 'FDB_EXTERNAL_SUBMITTED_AT=%s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf 'FDB_EXTERNAL_ENV_FILE=%q\n' "${FDB_ENV_FILE:-.env}"
    printf 'FDB_EXTERNAL_FLINK_JOB_ID=%q\n' "$flink_job_id"
    printf 'FDB_EXTERNAL_YARN_APPLICATION_ID=%q\n' "$yarn_app_id"
  } > "$state_file"
  write_current_run_env "$state_file"

  if [[ -z "$flink_job_id" && -z "$yarn_app_id" ]]; then
    warn "wrote external runtime state without parsed Flink/YARN ids: $state_file"
    warn "set FDB_FLINK_JOB_ID or FDB_YARN_APPLICATION_ID explicitly when stopping this job"
  else
    ok "wrote external runtime state: $state_file"
  fi
}

external_check() {
  load_env_optional
  external_require_env FDB_DEPLOY_TARGET
  if [[ "${FDB_DEPLOY_TARGET:-}" != "external-yarn" ]]; then
    warn_or_fail "FDB_DEPLOY_TARGET should be external-yarn"
  fi

  external_require_env FLINK_HOME
  external_require_env HADOOP_CONF_DIR
  external_require_env YARN_CONF_DIR
  external_require_env FDB_KAFKA_BOOTSTRAP
  external_require_env FDB_HDFS_URI
  external_require_env FDB_HIVE_JDBC_URL
  external_require_env FDB_STARROCKS_FE_ENDPOINT

  require_command_soft java
  require_command_soft mvn
  require_command_soft yarn
  require_command_soft hdfs
  require_command_soft beeline
  require_command_soft kafka-topics
  require_command_soft kafka-broker-api-versions
  require_command_soft mysql
  require_flink_home_soft

  if [[ -n "${FDB_KAFKA_BOOTSTRAP:-}" ]] \
    && kafka-broker-api-versions --bootstrap-server "$FDB_KAFKA_BOOTSTRAP" >/dev/null 2>&1; then
    ok "Kafka reachable"
  else
    warn_or_fail "Kafka check failed"
  fi

  if [[ -n "${FDB_HDFS_URI:-}" ]] \
    && hdfs dfs -fs "$FDB_HDFS_URI" -ls / >/dev/null 2>&1; then
    ok "HDFS reachable"
  else
    warn_or_fail "HDFS check failed"
  fi

  if [[ -n "${FDB_HIVE_JDBC_URL:-}" ]] \
    && beeline -u "$FDB_HIVE_JDBC_URL" -e "SELECT 1" >/dev/null 2>&1; then
    ok "Hive reachable"
  else
    warn_or_fail "Hive check failed"
  fi

  if yarn node -list >/dev/null 2>&1; then
    ok "YARN reachable"
  else
    warn_or_fail "YARN check failed"
  fi

  if [[ -n "${FDB_STARROCKS_FE_ENDPOINT:-}" ]] \
    && external_mysql_select_one "$(external_starrocks_host)" "$(external_starrocks_port)" "${FDB_STARROCKS_USER:-root}" "${FDB_STARROCKS_PASSWORD:-}" ""; then
    ok "StarRocks reachable"
  else
    warn_or_fail "StarRocks check failed"
  fi
}

external_init() {
  load_env
  [[ "${FDB_DEPLOY_TARGET:-}" == "external-yarn" ]] || die "FDB_DEPLOY_TARGET must be external-yarn"
  require_env FDB_KAFKA_BOOTSTRAP
  require_env FDB_HDFS_URI
  require_env FDB_HIVE_JDBC_URL
  require_env FDB_STARROCKS_FE_ENDPOINT
  local hive_cell_kpi_path
  local hive_warehouse_root
  local iceberg_warehouse_path
  local flink_checkpoint_path
  hive_cell_kpi_path="$(external_hdfs_path_from_location "$(external_hive_cell_kpi_location)")"
  hive_warehouse_root="$(external_hdfs_path_from_location "${FDB_HIVE_WAREHOUSE_ROOT:-${hive_cell_kpi_path%/*}}")"
  iceberg_warehouse_path="$(external_iceberg_warehouse_path)"
  flink_checkpoint_path="$(external_flink_checkpoint_path)"

  log "creating external Kafka topics"
  create_external_topic "${FDB_CHR_TOPIC:-chr-events}" 64 delete "${FDB_CHR_RETENTION_MS:-604800000}"
  create_external_topic "${FDB_PM_TOPIC:-pm-stats}" 16 delete "${FDB_PM_RETENTION_MS:-259200000}"
  create_external_topic "${FDB_CFG_TOPIC:-cfg-config}" 8 compact
  create_external_topic "${FDB_TOPOLOGY_TOPIC:-topology}" 4 compact
  create_external_topic "${FDB_LB_HEARTBEAT_TOPIC:-lb-heartbeat}" 1 delete 3600000
  create_external_topic "${FDB_LB_ROUTING_TOPIC:-lb-routing}" 1 compact
  create_external_topic "${FDB_METRICS_TOPIC:-fdb-stage-metrics}" 1 delete "${FDB_METRICS_RETENTION_MS:-3600000}"
  create_external_topic "${FDB_CELL_ANOMALY_TOPIC:-cell-anomaly-events}" 16 delete "${FDB_CELL_ANOMALY_RETENTION_MS:-${FDB_ANOMALY_RETENTION_MS:-604800000}}"
  create_external_topic "${FDB_USER_ANOMALY_TOPIC:-user-anomaly-events}" 16 delete "${FDB_USER_ANOMALY_RETENTION_MS:-${FDB_ANOMALY_RETENTION_MS:-604800000}}"
  create_external_topic "${FDB_GRID_ANOMALY_TOPIC:-grid-anomaly-events}" 16 delete "${FDB_GRID_ANOMALY_RETENTION_MS:-${FDB_ANOMALY_RETENTION_MS:-604800000}}"
  create_external_topic "${FDB_KPI_1M_TOPIC:-cell-kpi-1m}" 8 delete "${FDB_KPI_1M_RETENTION_MS:-259200000}"
  create_external_topic "${FDB_KPI_5M_TOPIC:-cell-kpi-5m}" 8 delete "${FDB_KPI_5M_RETENTION_MS:-604800000}"
  create_external_topic "${FDB_CHR_DLQ_TOPIC:-chr-dlq}" 4 delete 604800000
  create_external_topic "${FDB_PM_DLQ_TOPIC:-pm-dlq}" 4 delete 604800000
  create_external_topic "${FDB_CFG_DLQ_TOPIC:-cfg-dlq}" 4 delete 604800000
  create_external_topic "${FDB_ENRICHMENT_LATE_TOPIC:-enrichment-late}" 4 delete 604800000

  log "creating external HDFS directories"
  hdfs dfs -fs "$FDB_HDFS_URI" -mkdir -p \
    "$hive_cell_kpi_path" \
    "$iceberg_warehouse_path" \
    "$flink_checkpoint_path"
  hdfs dfs -fs "$FDB_HDFS_URI" -chmod -R 777 \
    "$hive_warehouse_root" \
    "$iceberg_warehouse_path" \
    "$flink_checkpoint_path"

  log "initializing external Hive table"
  external_init_hive

  log "initializing external StarRocks objects"
  external_mysql_run_file \
    "$(external_starrocks_host)" \
    "$(external_starrocks_port)" \
    "${FDB_STARROCKS_USER:-root}" \
    "${FDB_STARROCKS_PASSWORD:-}" \
    "" \
    scripts/init-starrocks.sql
  ensure_external_starrocks_cell_kpi_connector_schema

  ok "external-yarn dependencies initialized"
}

external_reset_kafka_topics() {
  local topic
  local current_topics
  local all_deleted

  log "resetting external Kafka benchmark topics"
  while read -r topic; do
    [[ -n "$topic" ]] || continue
    kafka-topics \
      --bootstrap-server "$FDB_KAFKA_BOOTSTRAP" \
      --delete --if-exists \
      --topic "$topic" >/dev/null || true
  done < <(benchmark_kafka_topics)

  for _ in {1..30}; do
    current_topics="$(kafka-topics --bootstrap-server "$FDB_KAFKA_BOOTSTRAP" --list 2>/dev/null || true)"
    all_deleted=1
    while read -r topic; do
      [[ -n "$topic" ]] || continue
      if grep -Fxq "$topic" <<< "$current_topics"; then
        all_deleted=0
        break
      fi
    done < <(benchmark_kafka_topics)
    [[ "$all_deleted" == "1" ]] && break
    sleep 1
  done
}

external_reset_hive_benchmark_outputs() {
  local hive_cell_path
  local hive_root

  hive_cell_path="$(external_hdfs_path_from_location "$(external_hive_cell_kpi_location)")"
  hive_root="$(external_hdfs_path_from_location "${FDB_HIVE_WAREHOUSE_ROOT:-${hive_cell_path%/*}}")"

  reset_hdfs_path_with external_hdfs_exec "$hive_cell_path"
  reset_hdfs_path_with external_hdfs_exec "$hive_root/cell_anomaly_events"
  reset_hdfs_path_with external_hdfs_exec "$hive_root/user_anomaly_events"
  reset_hdfs_path_with external_hdfs_exec "$hive_root/grid_anomaly_events"
}

external_reset_iceberg_benchmark_outputs() {
  local iceberg_root

  iceberg_root="$(external_iceberg_warehouse_path)/${FDB_ICEBERG_DATABASE:-iceberg_db}"

  reset_hdfs_path_with external_hdfs_exec "$iceberg_root/${FDB_ICEBERG_TABLE:-cell_kpi}"
  reset_hdfs_path_with external_hdfs_exec "$iceberg_root/${FDB_ICEBERG_CELL_ANOMALY_TABLE:-cell_anomaly_events}"
  reset_hdfs_path_with external_hdfs_exec "$iceberg_root/${FDB_ICEBERG_USER_ANOMALY_TABLE:-user_anomaly_events}"
  reset_hdfs_path_with external_hdfs_exec "$iceberg_root/${FDB_ICEBERG_GRID_ANOMALY_TABLE:-grid_anomaly_events}"
}

external_reset_hdfs_benchmark_outputs() {
  external_reset_hive_benchmark_outputs
  external_reset_iceberg_benchmark_outputs
}

external_prepare() {
  load_env
  [[ "${FDB_DEPLOY_TARGET:-}" == "external-yarn" ]] || die "FDB_DEPLOY_TARGET must be external-yarn"
  require_env FDB_KAFKA_BOOTSTRAP
  require_env FDB_HDFS_URI
  require_env FDB_HIVE_JDBC_URL
  require_env FDB_STARROCKS_FE_ENDPOINT
  external_apply_runtime_defaults

  external_reset_kafka_topics
  external_init

  case "${FDB_RESULT_SINK:-starrocks}" in
    starrocks)
      log "resetting external StarRocks benchmark tables"
      reset_starrocks_sql | external_mysql_run_sql \
        "$(external_starrocks_host)" \
        "$(external_starrocks_port)" \
        "${FDB_STARROCKS_USER:-root}" \
        "${FDB_STARROCKS_PASSWORD:-}" \
        "${FDB_STARROCKS_DATABASE:-fdb}"
      ;;
    hive)
      log "resetting external Hive HDFS benchmark outputs"
      external_reset_hive_benchmark_outputs
      ;;
    iceberg)
      log "resetting external Iceberg HDFS benchmark outputs"
      external_reset_iceberg_benchmark_outputs
      ;;
    kafka|none)
      log "no external storage reset required for ${FDB_RESULT_SINK:-starrocks} sink"
      ;;
    *)
      die "unsupported FDB_RESULT_SINK for external prepare: ${FDB_RESULT_SINK:-}"
      ;;
  esac

  ok "external-yarn benchmark data prepared"
}

external_submit() {
  local explicit_run_id="${FDB_RUN_ID:-}"
  local explicit_run_label="${FDB_RUN_LABEL:-}"
  local explicit_result_sink="${FDB_RESULT_SINK:-}"
  load_env
  if [[ -n "$explicit_run_id" ]]; then
    export FDB_RUN_ID="$explicit_run_id"
  fi
  if [[ -n "$explicit_run_label" ]]; then
    export FDB_RUN_LABEL="$explicit_run_label"
  fi
  if [[ -n "$explicit_result_sink" ]]; then
    export FDB_RESULT_SINK="$explicit_result_sink"
  fi
  [[ "${FDB_DEPLOY_TARGET:-}" == "external-yarn" ]] || die "FDB_DEPLOY_TARGET must be external-yarn"
  require_env FDB_KAFKA_BOOTSTRAP
  require_env FDB_HDFS_URI
  require_env FDB_HIVE_JDBC_URL
  require_env FLINK_HOME
  require_env HADOOP_CONF_DIR
  require_env YARN_CONF_DIR
  [[ -x "$(external_flink_bin)" ]] || die "flink command not executable: $(external_flink_bin)"
  ensure_run_context
  external_apply_runtime_defaults
  if [[ "${FDB_RESULT_SINK:-starrocks}" == "starrocks" ]]; then
    require_env FDB_STARROCKS_CONNECTOR_JDBC_URL
    require_env FDB_STARROCKS_LOAD_URL
  fi
  require_env FDB_HIVE_WAREHOUSE
  require_env FDB_ICEBERG_WAREHOUSE
  require_env FDB_FLINK_CHECKPOINT_DIR

  log "building project jars"
  maven_cmd package ${FDB_E2E_MAVEN_ARGS:--DskipTests}

  local jar="${FDB_FLINK_JOB_LOCAL_JAR:-flink-job/target/flink-job-0.1.0-SNAPSHOT.jar}"
  [[ -f "$jar" ]] || die "Flink job jar not found: $jar"

  local output_file="${FDB_EXTERNAL_SUBMIT_LOG:-logs/external-yarn-submit.out}"
  local output_dir
  output_dir="$(dirname "$output_file")"
  if [[ "$output_dir" != "." ]]; then
    mkdir -p "$output_dir"
  fi

  local flink_args=(run)
  if [[ -n "${FDB_FLINK_MASTER:-}" ]]; then
    case "$FDB_FLINK_MASTER" in
      yarn-cluster | yarn-session | yarn-application | yarn-*)
        flink_args+=(-m "$FDB_FLINK_MASTER")
        ;;
      *)
        die "FDB_FLINK_MASTER must be a YARN target for external-yarn submit: $FDB_FLINK_MASTER"
        ;;
    esac
  elif [[ -n "${FDB_FLINK_TARGET:-}" ]]; then
    case "$FDB_FLINK_TARGET" in
      yarn-application | yarn-session | yarn-per-job | yarn-*)
        flink_args+=(-t "$FDB_FLINK_TARGET")
        ;;
      *)
        die "FDB_FLINK_TARGET must be a YARN target for external-yarn submit: $FDB_FLINK_TARGET"
        ;;
    esac
  else
    flink_args+=(-t yarn-application)
  fi

  if [[ -n "${FDB_FLINK_YARN_QUEUE:-}" ]]; then
    flink_args+=(-yqu "$FDB_FLINK_YARN_QUEUE")
  fi
  flink_args+=(-p "${FDB_FLINK_PARALLELISM:-4}")
  build_external_flink_env_args
  flink_args+=("${EXTERNAL_FLINK_ENV_ARGS[@]}")

  if [[ -n "${FDB_FLINK_EXTRA_ARGS:-}" ]]; then
    local extra_args=()
    read -r -a extra_args <<< "$FDB_FLINK_EXTRA_ARGS"
    flink_args+=("${extra_args[@]}")
  fi
  if [[ -n "${FDB_FLINK_TASKMANAGER_MEMORY:-}" ]]; then
    flink_args+=(-D "taskmanager.memory.process.size=${FDB_FLINK_TASKMANAGER_MEMORY}")
  fi
  if [[ -n "${FDB_FLINK_TASKMANAGER_METASPACE:-}" ]]; then
    flink_args+=(-D "taskmanager.memory.jvm-metaspace.size=${FDB_FLINK_TASKMANAGER_METASPACE}")
  fi
  if [[ -n "${FDB_FLINK_TASKMANAGER_SLOTS:-}" ]]; then
    flink_args+=(-D "taskmanager.numberOfTaskSlots=${FDB_FLINK_TASKMANAGER_SLOTS}")
  fi
  if [[ -n "${FDB_FLINK_RETAINED_CHECKPOINTS:-}" ]]; then
    flink_args+=(-D "state.checkpoints.num-retained=${FDB_FLINK_RETAINED_CHECKPOINTS}")
  fi
  if [[ -n "${FDB_FLINK_HEARTBEAT_TIMEOUT_MS:-}" ]]; then
    flink_args+=(-D "heartbeat.timeout=${FDB_FLINK_HEARTBEAT_TIMEOUT_MS}")
  fi
  if [[ -n "${FDB_FLINK_HEARTBEAT_INTERVAL_MS:-}" ]]; then
    flink_args+=(-D "heartbeat.interval=${FDB_FLINK_HEARTBEAT_INTERVAL_MS}")
  fi
  if [[ -n "${FDB_FLINK_PEKKO_ASK_TIMEOUT:-}" ]]; then
    flink_args+=(-D "pekko.ask.timeout=${FDB_FLINK_PEKKO_ASK_TIMEOUT}")
  fi
  if [[ -n "${FDB_FLINK_TASKMANAGER_MANAGED_FRACTION:-}" ]]; then
    flink_args+=(-D "taskmanager.memory.managed.fraction=${FDB_FLINK_TASKMANAGER_MANAGED_FRACTION}")
  fi
  if [[ -n "${FDB_FLINK_EXTRA_ARGS_FILE:-}" ]]; then
    EXTERNAL_FLINK_FILE_ARGS=()
    append_args_from_file flink "$FDB_FLINK_EXTRA_ARGS_FILE"
    flink_args+=("${EXTERNAL_FLINK_FILE_ARGS[@]}")
  fi
  flink_args+=("$jar")

  log "submitting Flink job to external YARN"
  "$(external_flink_bin)" "${flink_args[@]}" 2>&1 | tee "$output_file"
  record_external_submit_output "$output_file"
}

external_stop() {
  local requested_report_on_stop="${FDB_REPORT_ON_STOP:-}"
  load_env
  if [[ -n "$requested_report_on_stop" ]]; then
    export FDB_REPORT_ON_STOP="$requested_report_on_stop"
  fi
  local explicit_flink_job_id="${FDB_FLINK_JOB_ID:-}"
  local explicit_yarn_app_id="${FDB_YARN_APPLICATION_ID:-}"
  local state_file="${FDB_EXTERNAL_STATE_FILE:-logs/external-yarn-current.env}"
  if [[ -f "$state_file" ]]; then
    # shellcheck disable=SC1090
    source "$state_file"
  fi

  local flink_job_id="${explicit_flink_job_id:-${FDB_EXTERNAL_FLINK_JOB_ID:-}}"
  local yarn_app_id="${explicit_yarn_app_id:-${FDB_EXTERNAL_YARN_APPLICATION_ID:-}}"

  if [[ -n "$flink_job_id" && -x "$(external_flink_bin)" ]]; then
    local cancel_args=(cancel)
    if [[ -n "${FDB_FLINK_CANCEL_ARGS:-}" ]]; then
      local extra_cancel_args=()
      read -r -a extra_cancel_args <<< "$FDB_FLINK_CANCEL_ARGS"
      cancel_args+=("${extra_cancel_args[@]}")
    fi
    if [[ -n "${FDB_FLINK_CANCEL_ARGS_FILE:-}" ]]; then
      EXTERNAL_CANCEL_FILE_ARGS=()
      append_args_from_file cancel "$FDB_FLINK_CANCEL_ARGS_FILE"
      cancel_args+=("${EXTERNAL_CANCEL_FILE_ARGS[@]}")
    fi
    cancel_args+=("$flink_job_id")

    log "canceling Flink job: $flink_job_id"
    if "$(external_flink_bin)" "${cancel_args[@]}"; then
      if [[ "${FDB_REPORT_ON_STOP:-false}" == "true" ]]; then
        run_report || true
      fi
      return 0
    fi
    warn "Flink cancel failed"
  fi

  if [[ -n "$yarn_app_id" ]]; then
    log "killing YARN application: $yarn_app_id"
    yarn application -kill "$yarn_app_id"
    if [[ "${FDB_REPORT_ON_STOP:-false}" == "true" ]]; then
      run_report || true
    fi
    return 0
  fi

  die "missing Flink job id or YARN application id; set FDB_FLINK_JOB_ID or FDB_YARN_APPLICATION_ID"
}

external_smoke() {
  load_env
  log "running external-yarn smoke diagnostics"
  external_apply_runtime_defaults
  STRICT=0
  external_check

  if [[ "${FDB_EXTERNAL_SMOKE_SUBMIT:-0}" == "1" ]]; then
    external_submit
  else
    warn "skipping Flink submit; set FDB_EXTERNAL_SMOKE_SUBMIT=1 to include submit in smoke"
  fi
}

external_prune() {
  load_env
  [[ "${FDB_DEPLOY_TARGET:-}" == "external-yarn" ]] || die "FDB_DEPLOY_TARGET must be external-yarn"
  require_env FDB_HDFS_URI
  require_env FDB_STARROCKS_FE_ENDPOINT
  external_apply_runtime_defaults

  if [[ "${FDB_PRUNE_DRY_RUN:-0}" == "1" ]]; then
    log "prune dry run"
    prune_starrocks_sql
    echo "HDFS prune: ${FDB_HIVE_WAREHOUSE_PATH:-/warehouse/fdb/cell_kpi} retention=${FDB_HDFS_KPI_RETENTION_MS:-86400000}ms"
    echo "Iceberg prune: $(external_iceberg_warehouse_path)/${FDB_ICEBERG_DATABASE:-iceberg_db}/${FDB_ICEBERG_TABLE:-cell_kpi} retention=${FDB_ICEBERG_FILE_RETENTION_MS:-86400000}ms"
    return 0
  fi

  log "pruning external StarRocks rows"
  prune_starrocks_sql | external_mysql_run_sql \
    "$(external_starrocks_host)" \
    "$(external_starrocks_port)" \
    "${FDB_STARROCKS_USER:-root}" \
    "${FDB_STARROCKS_PASSWORD:-}" \
    "${FDB_STARROCKS_DATABASE:-fdb}"

  log "pruning external HDFS files"
  run_hdfs_prune_with external_hdfs_exec
  ok "external-yarn storage prune completed"
}

external_status() {
  load_env
  external_apply_runtime_defaults
  echo "[status] yarn applications"
  yarn application -list 2>/dev/null || true
  echo "[status] starrocks"
  external_mysql_run_sql \
    "$(external_starrocks_host)" \
    "$(external_starrocks_port)" \
    "${FDB_STARROCKS_USER:-root}" \
    "${FDB_STARROCKS_PASSWORD:-}" \
    "${FDB_STARROCKS_DATABASE:-fdb}" <<'SQL' || true
SELECT 'cell_kpi', COUNT(*), MIN(window_start_ts), MAX(window_start_ts) FROM cell_kpi;
SELECT 'cell_anomaly_events', COUNT(*), MIN(event_ts), MAX(event_ts) FROM cell_anomaly_events;
SELECT 'user_anomaly_events', COUNT(*), MIN(event_ts), MAX(event_ts) FROM user_anomaly_events;
SELECT 'grid_anomaly_events', COUNT(*), MIN(event_ts), MAX(event_ts) FROM grid_anomaly_events;
SQL
  echo "[status] hdfs"
  external_hdfs_exec -count -h "${FDB_HIVE_WAREHOUSE_PATH:-/warehouse/fdb/cell_kpi}" || true
  external_hdfs_exec -count -h "$(external_iceberg_warehouse_path)/${FDB_ICEBERG_DATABASE:-iceberg_db}/${FDB_ICEBERG_TABLE:-cell_kpi}" || true
}

run_report() {
  local explicit_run_id="${FDB_RUN_ID:-}"
  local state_file
  local run_id
  local api_url

  case "$TARGET" in
    local)
      load_env_optional
      state_file="${FDB_LOCAL_STATE_FILE:-logs/local-current.env}"
      ;;
    external-yarn)
      load_env
      state_file="${FDB_EXTERNAL_STATE_FILE:-logs/external-yarn-current.env}"
      ;;
    *)
      die "unsupported target for report: $TARGET"
      ;;
  esac

  if [[ -n "$explicit_run_id" ]]; then
    export FDB_RUN_ID="$explicit_run_id"
  fi

  run_id="$(current_run_id "$state_file")"
  if [[ ! "$run_id" =~ ^[A-Za-z0-9._:-]+$ ]]; then
    die "FDB_RUN_ID contains unsafe characters for report URL: $run_id"
  fi

  api_url="${FDB_OBSERVABILITY_API_URL:-http://localhost:18080}"
  api_url="${api_url%/}"
  curl -fsS "${api_url}/api/runs/report?runId=${run_id}"
  echo
}

dispatch_local() {
  case "$COMMAND" in
    check) local_check ;;
    up) local_up ;;
    init) local_init ;;
    prepare) local_prepare ;;
    submit) local_submit ;;
    stop) local_stop ;;
    smoke) local_smoke ;;
    prune) local_prune ;;
    status) local_status ;;
    report) run_report ;;
    down) local_down "${ARGS[@]}" ;;
    *) die "unsupported command for local: $COMMAND" ;;
  esac
}

dispatch_external_yarn() {
  case "$COMMAND" in
    check) external_check ;;
    init) external_init ;;
    prepare) external_prepare ;;
    submit) external_submit ;;
    stop) external_stop ;;
    smoke) external_smoke ;;
    prune) external_prune ;;
    status) external_status ;;
    report) run_report ;;
    *) die "unsupported command for external-yarn: $COMMAND" ;;
  esac
}

main() {
  if [[ "$TARGET" == "--help" || "$TARGET" == "-h" ]]; then
    usage
    exit 0
  fi

  if [[ -z "$TARGET" ]]; then
    usage >&2
    die "missing target"
  fi

  if [[ -z "$COMMAND" ]]; then
    usage >&2
    die "missing command"
  fi

  case "$TARGET" in
    local) dispatch_local ;;
    external-yarn) dispatch_external_yarn ;;
    *) die "unsupported target: $TARGET" ;;
  esac
}

main "$@"
