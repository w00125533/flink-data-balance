#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

# Git Bash rewrites container-internal paths and URI-like arguments unless this is disabled.
export MSYS_NO_PATHCONV=1

# shellcheck source=scripts/e2e-summary-lib.sh
source "$ROOT_DIR/scripts/e2e-summary-lib.sh"

RETENTION_MS=${FDB_RETENTION_MS:-3600000}
RETENTION_BYTES=${FDB_RETENTION_BYTES:-10737418240}
HDFS_URI=${FDB_HDFS_URI:-hdfs://namenode:8020}
KAFKA_BOOTSTRAP=${FDB_KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092}
ICEBERG_WAREHOUSE=${FDB_ICEBERG_WAREHOUSE:-${HDFS_URI}/warehouse/iceberg}
ICEBERG_DATABASE=${FDB_ICEBERG_DATABASE:-iceberg_db}
ICEBERG_TABLE=${FDB_ICEBERG_TABLE:-cell_kpi}
ICEBERG_METASTORE_URI=${FDB_ICEBERG_METASTORE_URI:-${FDB_HIVE_METASTORE_URI:-thrift://hive-metastore:9083}}
STARROCKS_DATABASE=${FDB_STARROCKS_DATABASE:-fdb}
OBSERVABILITY_RUNS_DIR=${FDB_E2E_RUNS_DIR:-docker/data/observability-runs}
SAFE_OBSERVABILITY_RUNS_DIR=""

log() {
  printf '[retention] %s\n' "$*"
}

fail() {
  printf '[retention] ERROR: %s\n' "$*" >&2
  exit 1
}

canonicalize_path() {
  local path=$1
  if command -v realpath >/dev/null 2>&1; then
    realpath -m "$path"
    return 0
  fi
  if command -v python3 >/dev/null 2>&1; then
    python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$path"
    return 0
  fi
  fail "realpath or python3 is required to validate local delete path"
}

shared_compose_file() {
  local file=$1
  printf '%s/%s\n' "$(shared_infra_dir)" "$file"
}

require_file() {
  local file=$1
  local label=$2
  [ -f "$file" ] || fail "$label not found: $file"
}

preflight() {
  SAFE_OBSERVABILITY_RUNS_DIR="$(safe_observability_runs_dir)"
  log "Observability runs dir | $OBSERVABILITY_RUNS_DIR | resolved=$SAFE_OBSERVABILITY_RUNS_DIR"

  require_file "$(shared_compose_file compose.yaml)" "shared infra compose"
  require_file "$(shared_compose_file compose.streaming.yaml)" "shared streaming compose"
  require_file "$(shared_compose_file compose.lakehouse.yaml)" "shared lakehouse compose"
  require_file "$(shared_compose_file compose.starrocks.yaml)" "shared StarRocks compose"

  command -v docker >/dev/null 2>&1 || fail "docker command is not available"
  docker info >/dev/null 2>&1 || fail "docker daemon is not reachable"
  docker compose version >/dev/null 2>&1 || fail "docker compose is not available"

  shared_kafka_exec true >/dev/null 2>&1 || fail "Kafka container is not reachable via shared infra compose"
  shared_starrocks_exec true >/dev/null 2>&1 || fail "StarRocks FE container is not reachable via shared infra compose"
  shared_hdfs_exec -ls / >/dev/null 2>&1 || fail "HDFS namenode is not reachable via shared infra compose"
  docker container inspect fdb-flink-jobmanager >/dev/null 2>&1 || fail "fdb-flink-jobmanager container does not exist"
  project_flink_exec test -f /opt/fdb/flink-job-0.1.0-SNAPSHOT.jar >/dev/null 2>&1 \
    || fail "Flink job jar not found in fdb-flink-jobmanager: /opt/fdb/flink-job-0.1.0-SNAPSHOT.jar"
}

safe_observability_runs_dir() {
  validate_observability_runs_dir "$OBSERVABILITY_RUNS_DIR"
}

validate_observability_runs_dir() {
  local requested=$1
  [ -n "$requested" ] || fail "FDB_E2E_RUNS_DIR must not be empty"

  case "$requested" in
    *..*) fail "FDB_E2E_RUNS_DIR must not contain '..': $requested" ;;
    /|[A-Za-z]:|[A-Za-z]:/|[A-Za-z]:\\) fail "FDB_E2E_RUNS_DIR must not be a filesystem root: $requested" ;;
  esac

  local base_dir="$ROOT_DIR/docker/data/observability-runs"
  local candidate
  case "$requested" in
    /*|[A-Za-z]:/*|[A-Za-z]:\\*)
      candidate="$requested"
      ;;
    docker/data/observability-runs|docker/data/observability-runs/*)
      candidate="$ROOT_DIR/$requested"
      ;;
    *)
      fail "FDB_E2E_RUNS_DIR must be docker/data/observability-runs or a path below it: $requested"
      ;;
  esac

  local base_canonical
  local candidate_canonical
  base_canonical="$(canonicalize_path "$base_dir")"
  candidate_canonical="$(canonicalize_path "$candidate")"

  case "$candidate_canonical" in
    "$base_canonical"|"$base_canonical"/*)
      printf '%s\n' "$candidate_canonical"
      ;;
    *)
      fail "FDB_E2E_RUNS_DIR resolves outside $base_canonical: $candidate_canonical"
      ;;
  esac
}

alter_topic_retention() {
  local topic=$1
  if ! shared_kafka_exec kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" --list 2>/dev/null | grep -qx "$topic"; then
    log "Kafka topic missing, skipping: $topic"
    return 0
  fi

  log "Kafka topic retention | $topic | retention.ms=$RETENTION_MS retention.bytes=$RETENTION_BYTES"
  shared_kafka_exec kafka-configs \
    --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --alter \
    --entity-type topics \
    --entity-name "$topic" \
    --add-config "retention.ms=$RETENTION_MS,retention.bytes=$RETENTION_BYTES"
}

alter_kafka_retention() {
  local topics=(
    chr-events
    pm-stats
    fdb-stage-metrics
    cell-kpi-1m
    cell-kpi-5m
    cell-anomaly-events
    grid-anomaly-events
    chr-dlq
    pm-dlq
    cfg-dlq
    enrichment-late
  )
  if [ "${FDB_DYNAMIC_BALANCING_ENABLED:-false}" = "true" ]; then
    topics+=(lb-heartbeat)
  fi

  local topic
  for topic in "${topics[@]}"; do
    alter_topic_retention "$topic"
  done
}

hive_partition_key() {
  local path=$1
  local dt
  local hour
  dt="$(printf '%s\n' "$path" | sed -n 's#.*[/]dt=\([^/]*\)[/]hour=.*#\1#p')"
  hour="$(printf '%s\n' "$path" | sed -n 's#.*[/]hour=\([^/]*\)$#\1#p')"
  if [ -n "$dt" ] && [ -n "$hour" ]; then
    printf '%s%02d\n' "$dt" "$((10#$hour))"
  fi
}

delete_old_hive_partitions() {
  local cutoff_key
  cutoff_key="$(date -u -d '1 hour ago' '+%Y-%m-%d%H')"
  local path
  while IFS= read -r path; do
    [ -n "$path" ] || continue
    local key
    key="$(hive_partition_key "$path")"
    if [ -n "$key" ] && [[ "$key" < "$cutoff_key" ]]; then
      log "HDFS Hive partition delete | $path"
      shared_hdfs_exec -rm -r -skipTrash "$path"
    fi
  done < <(shared_hdfs_exec -ls -d '/warehouse/fdb/cell_kpi/window_kind=*/dt=*/hour=*' 2>/dev/null | awk '{print $NF}' || true)
}

run_iceberg_retention() {
  log "Iceberg retention | $ICEBERG_DATABASE.$ICEBERG_TABLE | warehouse=$ICEBERG_WAREHOUSE"
  project_flink_exec java \
    -cp '/opt/fdb/flink-job-0.1.0-SNAPSHOT.jar:/opt/flink/lib/*' \
    com.fdb.job.maintenance.IcebergRetentionTool \
    --warehouse "$ICEBERG_WAREHOUSE" \
    --metastore-uri "$ICEBERG_METASTORE_URI" \
    --database "$ICEBERG_DATABASE" \
    --table "$ICEBERG_TABLE" \
    --older-than-ms "$RETENTION_MS" \
    --max-bytes "$RETENTION_BYTES" \
    --orphan-delete-mode manual-safe \
    --allow-manual-orphan-delete true
}

starrocks_mysql() {
  local password_args=()
  if [ -n "${FDB_STARROCKS_PASSWORD:-}" ]; then
    password_args=(-p"${FDB_STARROCKS_PASSWORD}")
  fi
  shared_starrocks_exec mysql -h 127.0.0.1 -P 9030 -u "${FDB_STARROCKS_USER:-root}" "${password_args[@]}" "$@"
}

run_starrocks_retention_sql() {
  local cutoff_ms
  cutoff_ms=$(( $(date -u '+%s') * 1000 - RETENTION_MS ))
  local table
  for table in cell_anomaly_events grid_anomaly_events; do
    log "StarRocks retention | ${STARROCKS_DATABASE}.${table} | detection_ts < $cutoff_ms"
    starrocks_mysql -e "DELETE FROM \`${STARROCKS_DATABASE}\`.\`${table}\` WHERE detection_ts < ${cutoff_ms};"
  done
}

delete_old_observability_runs() {
  local runs_dir
  runs_dir="${SAFE_OBSERVABILITY_RUNS_DIR:-$(safe_observability_runs_dir)}"
  if [ ! -d "$runs_dir" ]; then
    log "observability runs dir missing, skipping: $OBSERVABILITY_RUNS_DIR"
    return 0
  fi
  log "Local observability files delete | $OBSERVABILITY_RUNS_DIR | older than 60 minutes"
  find "$runs_dir" -type f -mmin +60 -delete
}

main() {
  preflight
  alter_kafka_retention
  delete_old_hive_partitions
  run_iceberg_retention
  run_starrocks_retention_sql
  delete_old_observability_runs

  log "Done"
}

if [ "${FDB_RETENTION_MAINTENANCE_SOURCE_ONLY:-}" != "1" ]; then
  main "$@"
fi
