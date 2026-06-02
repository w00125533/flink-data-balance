#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"
# shellcheck source=scripts/e2e-summary-lib.sh
source "$ROOT_DIR/scripts/e2e-summary-lib.sh"
PIDS=()
summary_init

cleanup() {
  local status=$?
  for pid in "${PIDS[@]:-}"; do kill "$pid" >/dev/null 2>&1 || true; done
  if [ "$status" -eq 0 ] || [ "${FDB_E2E_KEEP_RUNNING_ON_FAIL:-0}" != "1" ]; then
    COMPOSE_PROFILES=e2e bash scripts/dev-down.sh >/dev/null 2>&1 || true
  else
    echo "[e2e] Failed; keeping containers running because FDB_E2E_KEEP_RUNNING_ON_FAIL=1"
    COMPOSE_PROFILES=e2e docker compose -f docker/docker-compose.yml ps || true
  fi
}
trap cleanup EXIT

wait_for() {
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
COMPOSE_PROFILES=e2e bash scripts/dev-up.sh
wait_for "Flink JobManager" "curl -fsS http://localhost:8081/overview >/dev/null"
wait_for "HiveServer2" "docker exec fdb-hive-server beeline -u jdbc:hive2://localhost:10000/default -e 'SELECT 1' >/dev/null 2>&1"
summary_section "Infrastructure"
summary_command "Infrastructure" "running containers" "COMPOSE_PROFILES=e2e docker compose -f docker/docker-compose.yml ps --services --filter status=running | wc -l | tr -d ' '"
summary_command "Infrastructure" "Kafka topics" "docker exec fdb-kafka kafka-topics --bootstrap-server kafka:29092 --list | wc -l | tr -d ' '"

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
FLINK_SUBMIT_OUTPUT="$(docker exec fdb-flink-jobmanager flink run -d /opt/fdb/flink-job-0.1.0-SNAPSHOT.jar)"
echo "$FLINK_SUBMIT_OUTPUT"
summary_section "Flink Submit"
summary_line "Flink Submit" "job id" "$(printf '%s\n' "$FLINK_SUBMIT_OUTPUT" | awk '/JobID/ {print $NF}')"

wait_for "CHR messages" "docker exec fdb-kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:29092 --topic chr-events | grep -Eq ':[1-9][0-9]*$'"
summary_section "Kafka Input"
summary_kafka_topic "cm-config"
summary_kafka_topic "mr-stats"
summary_kafka_topic "chr-events"
wait_for "1m KPI rows in MySQL" "docker exec fdb-mysql mysql -N -ufdb -pfdbpwd fdb -e \"SELECT COUNT(*) FROM cell_kpi WHERE window_kind='MIN_1'\" | grep -Eq '^[1-9][0-9]*$'" 90
summary_section "MySQL KPI"
summary_mysql_kpi
wait_for "heartbeat messages" "docker exec fdb-kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:29092 --topic lb-heartbeat | grep -Eq ':[1-9][0-9]*$'"
summary_section "Load Balancing"
summary_kafka_topic "lb-heartbeat"
summary_kafka_topic "lb-routing"
summary_flink_job
summary_code_logs "Flink Code" docker logs fdb-flink-taskmanager
wait_for "Parquet KPI files" "find docker/data/warehouse/cell_kpi -name '*.parquet' | grep -q ."
summary_section "Parquet KPI"
summary_parquet_kpi "docker/data/warehouse/cell_kpi"

echo "[e2e] Initializing Hive table and verifying query..."
bash scripts/init-hive.sh
docker exec fdb-hive-server beeline -u jdbc:hive2://localhost:10000/default \
  -e 'MSCK REPAIR TABLE fdb.cell_kpi; SELECT COUNT(*) FROM fdb.cell_kpi;'
summary_section "Hive KPI"
summary_hive_kpi

echo "[e2e] Completed successfully"
