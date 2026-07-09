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
  bash scripts/create-kafka-topics.sh

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

external_check() {
  load_env_optional
  require_command_soft java
  require_command_soft mvn
  require_command_soft yarn
  require_command_soft hdfs
  require_command_soft beeline
  require_command_soft kafka-topics
  require_command_soft kafka-broker-api-versions
  require_command_soft mysql
  require_flink_home_soft
}

external_init() {
  load_env
  die "external-yarn init is not implemented yet; requires Task 7 external environment initialization"
}

external_submit() {
  load_env
  die "external-yarn submit is not implemented yet; requires Task 8 external YARN Flink job submission"
}

external_stop() {
  load_env
  die "external-yarn stop is not implemented yet; requires Task 9 external YARN job lifecycle control"
}

external_smoke() {
  load_env
  die "external-yarn smoke is not implemented yet; requires Task 10 external smoke test workflow"
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
