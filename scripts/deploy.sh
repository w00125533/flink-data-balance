#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

export MSYS_NO_PATHCONV="${MSYS_NO_PATHCONV:-1}"

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

shared_lakehouse() {
  docker compose \
    -f "$(shared_infra_dir)/compose.yaml" \
    -f "$(shared_infra_dir)/compose.lakehouse.yaml" \
    --profile lakehouse \
    --profile lakehouse-tools \
    "$@"
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

  mvn -q dependency:copy \
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
    warn "  cd $(shared_infra_dir) && sh scripts/infra-up.sh lakehouse lakehouse-tools streaming observability"
    exit 1
  fi

  prepare_flink_hadoop_runtime

  log "starting local project containers"
  docker compose -f docker/docker-compose.yml --profile e2e up -d \
    mysql \
    observability-api \
    prometheus \
    frontend \
    jobmanager \
    taskmanager

  log "waiting for MySQL to be ready (up to 60s)"
  wait_for_command "MySQL OK" 30 2 \
    docker compose -f docker/docker-compose.yml exec -T mysql \
      mysqladmin ping -h localhost -ufdb -pfdbpwd --silent
}

local_init() {
  load_env_optional
  log "waiting for shared Kafka to be ready (up to 60s)"
  wait_for_command "shared Kafka OK" 30 2 \
    shared_streaming exec -T kafka \
      kafka-broker-api-versions --bootstrap-server "$(local_kafka_bootstrap)"

  log "waiting for shared HiveServer2 to be ready (up to 90s)"
  wait_for_command "shared HiveServer2 OK" 45 2 \
    shared_lakehouse exec -T hive-server \
      beeline -u jdbc:hive2://localhost:10000/default -e "SELECT 1"

  log "preparing shared HDFS warehouse directories"
  shared_lakehouse exec -T namenode \
    hdfs dfs -fs "$(local_hdfs_uri)" -mkdir -p /warehouse/fdb/cell_kpi /warehouse/iceberg
  shared_lakehouse exec -T namenode \
    hdfs dfs -fs "$(local_hdfs_uri)" -chmod -R 777 /warehouse/fdb /warehouse/iceberg

  prepare_flink_hadoop_runtime

  log "creating Kafka topics"
  bash scripts/init-kafka-topics.sh

  log "initializing MySQL tables"
  docker exec -i fdb-mysql mysql -ufdb -pfdbpwd fdb < scripts/init-mysql.sql

  log "initializing shared Hive table"
  bash scripts/init-hive.sh

  ok "local dependencies initialized"
  docker compose -f docker/docker-compose.yml ps
}

local_submit() {
  load_env_optional
  local jar="${FDB_FLINK_JOB_JAR:-/opt/fdb/flink-job-0.1.0-SNAPSHOT.jar}"
  local submit_log="logs-local-flink-submit.out"

  log "submitting local Flink job: $jar"
  docker exec --user flink fdb-flink-jobmanager flink run -d "$jar" | tee "$submit_log"
}

local_stop() {
  load_env_optional
  local submit_log="logs-local-flink-submit.out"
  local job_id="${FDB_FLINK_JOB_ID:-}"

  if [[ -z "$job_id" && -f "$submit_log" ]]; then
    job_id="$(awk '/JobID/ {job_id=$NF} END {print job_id}' "$submit_log")"
  fi

  if [[ -z "$job_id" ]]; then
    die "no local Flink job id found; set FDB_FLINK_JOB_ID or run local submit first"
  fi

  log "cancelling local Flink job: $job_id"
  docker exec --user flink fdb-flink-jobmanager flink cancel "$job_id"
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

  echo "[e2e] Building jars..."
  mvn package ${FDB_E2E_MAVEN_ARGS:--DskipTests}
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
  java -jar topology-service/target/topology-service-0.1.0-SNAPSHOT.jar > logs-topology.log 2>&1
  java -jar simulator/target/simulator-0.1.0-SNAPSHOT.jar cm > logs-cm.log 2>&1 & PIDS+=("$!")
  java -jar simulator/target/simulator-0.1.0-SNAPSHOT.jar mr > logs-mr.log 2>&1 & PIDS+=("$!")
  java -jar simulator/target/simulator-0.1.0-SNAPSHOT.jar chr > logs-chr.log 2>&1 & PIDS+=("$!")
  summary_section "Data Generation"
  summary_command "Data Generation" "topology log lines" "wc -l < logs-topology.log | tr -d ' '"
  summary_line "Data Generation" "simulator processes" "${#PIDS[@]}"
  summary_code_logs "Data Generation" cat logs-topology.log logs-cm.log logs-mr.log logs-chr.log

  echo "[e2e] Submitting Flink job..."
  local_submit
  summary_section "Flink Submit"
  summary_line "Flink Submit" "job id" "$(awk '/JobID/ {job_id=$NF} END {print job_id}' logs-local-flink-submit.out)"

  local_smoke_wait_for "CHR messages" "shared_kafka_exec kafka-run-class kafka.tools.GetOffsetShell --broker-list ${FDB_KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092} --topic chr-events | grep -Eq ':[1-9][0-9]*$'"
  summary_section "Kafka Input"
  summary_kafka_topic "cm-config"
  summary_kafka_topic "mr-stats"
  summary_kafka_topic "chr-events"
  local_smoke_wait_for "1m KPI rows in MySQL" "docker exec fdb-mysql mysql -N -ufdb -pfdbpwd fdb -e \"SELECT COUNT(*) FROM cell_kpi WHERE window_kind='MIN_1'\" | grep -Eq '^[1-9][0-9]*$'" 90
  summary_section "MySQL KPI"
  summary_mysql_kpi
  local_smoke_wait_for "runtime stage metric messages" "shared_kafka_exec kafka-run-class kafka.tools.GetOffsetShell --broker-list ${FDB_KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092} --topic fdb-stage-metrics | grep -Eq ':[1-9][0-9]*$'" 60
  local_smoke_wait_for "nonzero observability metrics" "curl -fsS \"$(observability_api_url)/metrics\" | awk '/^fdb_stage_out_eps|^fdb_source_eps/ { if (\$2+0 > 0) found=1 } END { exit(found ? 0 : 1) }'" 60
  local_smoke_wait_for "Prometheus fdb_stage_out_eps" "curl -fsS \"$(observability_prometheus_url)/api/v1/query?query=fdb_stage_out_eps%20%3E%200\" | grep -q '\"metric\"'" 60
  summary_section "Observability"
  summary_observability
  local_smoke_wait_for "heartbeat messages" "shared_kafka_exec kafka-run-class kafka.tools.GetOffsetShell --broker-list ${FDB_KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092} --topic lb-heartbeat | grep -Eq ':[1-9][0-9]*$'"
  summary_section "Load Balancing"
  summary_kafka_topic "lb-heartbeat"
  summary_kafka_topic "lb-routing"
  summary_flink_job
  summary_code_logs "Flink Code" docker logs fdb-flink-taskmanager
  local_smoke_wait_for "Parquet KPI files" "shared_hdfs_exec -find /warehouse/fdb/cell_kpi -name '*.parquet' | grep -q ."
  summary_section "Parquet KPI"
  summary_parquet_kpi "/warehouse/fdb/cell_kpi"
  local_smoke_wait_for "Iceberg metadata" "shared_hdfs_exec -find /warehouse/iceberg/fdb/cell_kpi/metadata -name '*.metadata.json' | grep -q ."
  local_smoke_wait_for "Iceberg data files" "shared_hdfs_exec -find /warehouse/iceberg/fdb/cell_kpi/data -name '*.parquet' | grep -q ."
  summary_section "Iceberg KPI"
  summary_iceberg_kpi "/warehouse/iceberg/fdb/cell_kpi"
  summary_section "Hive/Iceberg Compare"
  summary_hive_iceberg_compare "/warehouse/fdb/cell_kpi" "/warehouse/iceberg/fdb/cell_kpi/data"
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
    docker compose -f docker/docker-compose.yml down -v
    remove_legacy_local_infra
    rm -rf docker/data
  else
    log "stopping project containers"
    docker compose -f docker/docker-compose.yml down
  fi
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
  if [[ -z "${FDB_MYSQL_URL:-}" && -n "${FDB_MYSQL_HOST:-}" ]]; then
    export FDB_MYSQL_URL="jdbc:mysql://$FDB_MYSQL_HOST:${FDB_MYSQL_PORT:-3306}/${FDB_MYSQL_DATABASE:-fdb}"
  fi
  if [[ -z "${FDB_HIVE_WAREHOUSE:-}" && -n "${FDB_HDFS_URI:-}" ]]; then
    export FDB_HIVE_WAREHOUSE
    FDB_HIVE_WAREHOUSE="$(external_hive_warehouse_location)"
  fi
  if [[ -z "${FDB_ICEBERG_WAREHOUSE:-}" && -n "${FDB_HDFS_URI:-}" ]]; then
    export FDB_ICEBERG_WAREHOUSE="${FDB_HDFS_URI%/}$(external_iceberg_warehouse_path)"
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
  local extra=()

  if [[ -n "$retention_ms" && "$cleanup" == "delete" ]]; then
    extra+=(--config "retention.ms=$retention_ms")
  fi

  kafka-topics \
    --bootstrap-server "$FDB_KAFKA_BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "${FDB_KAFKA_REPLICATION_FACTOR:-1}" \
    --config "cleanup.policy=$cleanup" \
    "${extra[@]}"
}

EXTERNAL_FLINK_ENV_ARGS=()

build_external_flink_env_args() {
  local key
  local value
  local env_keys=(
    FDB_KAFKA_BOOTSTRAP
    FDB_MYSQL_URL
    FDB_MYSQL_USER
    FDB_HIVE_WAREHOUSE
    FDB_ICEBERG_ENABLED
    FDB_ICEBERG_WAREHOUSE
    FDB_ICEBERG_CATALOG
    FDB_ICEBERG_DATABASE
    FDB_ICEBERG_TABLE
    FDB_FLINK_CHECKPOINT_DIR
    FDB_FLINK_CHECKPOINT_INTERVAL_MS
    FDB_FLINK_PARALLELISM
    FDB_METRICS_TOPIC
    FDB_E2E_SUMMARY
  )

  EXTERNAL_FLINK_ENV_ARGS=()
  for key in "${env_keys[@]}"; do
    value="${!key:-}"
    if [[ -n "$value" ]]; then
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
  external_require_env FDB_MYSQL_HOST
  external_require_env FDB_MYSQL_PORT
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

  if [[ -n "${FDB_MYSQL_HOST:-}" ]] \
    && external_mysql_select_one "$FDB_MYSQL_HOST" "${FDB_MYSQL_PORT:-3306}" "${FDB_MYSQL_USER:-fdb}" "${FDB_MYSQL_PASSWORD:-fdbpwd}" "${FDB_MYSQL_DATABASE:-fdb}"; then
    ok "MySQL reachable"
  else
    warn_or_fail "MySQL check failed"
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
  require_env FDB_MYSQL_HOST
  local hive_cell_kpi_path
  local hive_warehouse_root
  local iceberg_warehouse_path
  local flink_checkpoint_path
  hive_cell_kpi_path="$(external_hdfs_path_from_location "$(external_hive_cell_kpi_location)")"
  hive_warehouse_root="$(external_hdfs_path_from_location "${FDB_HIVE_WAREHOUSE_ROOT:-${hive_cell_kpi_path%/*}}")"
  iceberg_warehouse_path="$(external_iceberg_warehouse_path)"
  flink_checkpoint_path="$(external_flink_checkpoint_path)"

  log "creating external Kafka topics"
  create_external_topic "${FDB_CHR_TOPIC:-chr-events}" 64 delete 604800000
  create_external_topic "${FDB_PM_TOPIC:-mr-stats}" 16 delete 259200000
  create_external_topic "${FDB_CFG_TOPIC:-cm-config}" 8 compact
  create_external_topic "${FDB_TOPOLOGY_TOPIC:-topology}" 4 compact
  create_external_topic "${FDB_LB_HEARTBEAT_TOPIC:-lb-heartbeat}" 1 delete 3600000
  create_external_topic "${FDB_LB_ROUTING_TOPIC:-lb-routing}" 1 compact
  create_external_topic "${FDB_METRICS_TOPIC:-fdb-stage-metrics}" 1 delete 3600000
  create_external_topic "${FDB_ANOMALY_TOPIC:-anomaly-events}" 16 delete 604800000
  create_external_topic "${FDB_KPI_1M_TOPIC:-cell-kpi-1m}" 8 delete 259200000
  create_external_topic "${FDB_KPI_5M_TOPIC:-cell-kpi-5m}" 8 delete 604800000
  create_external_topic "${FDB_CHR_DLQ_TOPIC:-chr-dlq}" 4 delete 604800000
  create_external_topic "${FDB_PM_DLQ_TOPIC:-mr-dlq}" 4 delete 604800000
  create_external_topic "${FDB_CFG_DLQ_TOPIC:-cm-dlq}" 4 delete 604800000
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

  log "initializing external MySQL tables"
  external_mysql_run_file \
    "$FDB_MYSQL_HOST" \
    "${FDB_MYSQL_PORT:-3306}" \
    "${FDB_MYSQL_USER:-fdb}" \
    "${FDB_MYSQL_PASSWORD:-fdbpwd}" \
    "${FDB_MYSQL_DATABASE:-fdb}" \
    scripts/init-mysql.sql

  if [[ -f scripts/init-starrocks.sql ]]; then
    log "initializing external StarRocks objects"
    external_mysql_run_file \
      "$(external_starrocks_host)" \
      "$(external_starrocks_port)" \
      "${FDB_STARROCKS_USER:-root}" \
      "${FDB_STARROCKS_PASSWORD:-}" \
      "${FDB_STARROCKS_DATABASE:-}" \
      scripts/init-starrocks.sql
  else
    warn "scripts/init-starrocks.sql not found; skipping StarRocks initialization"
  fi

  ok "external-yarn dependencies initialized"
}

external_submit() {
  load_env
  [[ "${FDB_DEPLOY_TARGET:-}" == "external-yarn" ]] || die "FDB_DEPLOY_TARGET must be external-yarn"
  require_env FDB_KAFKA_BOOTSTRAP
  require_env FDB_HDFS_URI
  require_env FDB_HIVE_JDBC_URL
  require_env FLINK_HOME
  require_env HADOOP_CONF_DIR
  require_env YARN_CONF_DIR
  [[ -x "$(external_flink_bin)" ]] || die "flink command not executable: $(external_flink_bin)"
  external_apply_runtime_defaults
  require_env FDB_MYSQL_URL
  require_env FDB_HIVE_WAREHOUSE
  require_env FDB_ICEBERG_WAREHOUSE
  require_env FDB_FLINK_CHECKPOINT_DIR

  log "building project jars"
  mvn package ${FDB_E2E_MAVEN_ARGS:--DskipTests}

  local jar="${FDB_FLINK_JOB_LOCAL_JAR:-flink-job/target/flink-job-0.1.0-SNAPSHOT.jar}"
  [[ -f "$jar" ]] || die "Flink job jar not found: $jar"

  local output_file="${FDB_EXTERNAL_SUBMIT_LOG:-logs-external-yarn-submit.out}"
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
  load_env
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
      return 0
    fi
    warn "Flink cancel failed"
  fi

  if [[ -n "$yarn_app_id" ]]; then
    log "killing YARN application: $yarn_app_id"
    yarn application -kill "$yarn_app_id"
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

dispatch_local() {
  case "$COMMAND" in
    check) local_check ;;
    up) local_up ;;
    init) local_init ;;
    submit) local_submit ;;
    stop) local_stop ;;
    smoke) local_smoke ;;
    down) local_down "${ARGS[@]}" ;;
    *) die "unsupported command for local: $COMMAND" ;;
  esac
}

dispatch_external_yarn() {
  case "$COMMAND" in
    check) external_check ;;
    init) external_init ;;
    submit) external_submit ;;
    stop) external_stop ;;
    smoke) external_smoke ;;
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
