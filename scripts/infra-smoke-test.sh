#!/usr/bin/env bash
set -euo pipefail

export MSYS_NO_PATHCONV=1

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SHARED_INFRA_DIR="${SHARED_INFRA_DIR:-../shared-data-infra}"
KAFKA_BOOTSTRAP="${FDB_KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092}"
HDFS_URI="${FDB_HDFS_URI:-hdfs://namenode:8020}"
PASS=0
FAIL=0

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}[OK]${NC} $1"; PASS=$((PASS + 1)); }
fail() { echo -e "  ${RED}[FAIL]${NC} $1"; FAIL=$((FAIL + 1)); }

shared_streaming() {
  docker compose \
    -f "$SHARED_INFRA_DIR/compose.yaml" \
    -f "$SHARED_INFRA_DIR/compose.streaming.yaml" \
    --profile streaming "$@"
}

shared_lakehouse() {
  docker compose \
    -f "$SHARED_INFRA_DIR/compose.yaml" \
    -f "$SHARED_INFRA_DIR/compose.lakehouse.yaml" \
    --profile lakehouse --profile lakehouse-tools "$@"
}

shared_hdfs_exec() {
  shared_lakehouse exec -T namenode hdfs dfs -fs "$HDFS_URI" "$@"
}

echo "=== infra-smoke-test ==="
echo ""

if ! command -v docker >/dev/null 2>&1; then
  echo "FATAL: docker not found in PATH"
  exit 1
fi

if ! docker ps >/dev/null 2>&1; then
  echo "FATAL: docker daemon is not reachable"
  exit 1
fi

if [ ! -d "$ROOT_DIR/$SHARED_INFRA_DIR" ] && [ ! -d "$SHARED_INFRA_DIR" ]; then
  echo "FATAL: shared infra directory not found: $SHARED_INFRA_DIR"
  exit 1
fi

cd "$ROOT_DIR"

echo "--- Phase 1: Clean project stack ---"
bash scripts/dev-down.sh --clean 2>&1 | sed 's/^/  /'
pass "Project-local containers stopped and cleaned"

echo ""
echo "--- Phase 2: Start shared infra ---"
(cd "$SHARED_INFRA_DIR" && sh scripts/infra-up.sh lakehouse lakehouse-tools streaming) 2>&1 | sed 's/^/  /'
pass "Shared lakehouse and streaming profiles started"

echo ""
echo "--- Phase 3: Start project stack ---"
bash scripts/dev-up.sh 2>&1 | sed 's/^/  /'
pass "dev-up.sh completed"

echo ""
echo "--- Phase 4: Verify required containers ---"
PROJECT_EXPECTED=(fdb-mysql fdb-observability-api fdb-frontend fdb-prometheus fdb-flink-jobmanager fdb-flink-taskmanager)
for name in "${PROJECT_EXPECTED[@]}"; do
  if docker ps --format '{{.Names}}' | grep -qx "$name"; then
    pass "Project container $name is running"
  else
    fail "Project container $name is NOT running"
  fi
done

SHARED_EXPECTED=(shared-data-infra-zookeeper-1 shared-data-infra-kafka-1 shared-data-infra-hms-db-1 shared-data-infra-hive-metastore-1 shared-data-infra-hive-server-1 shared-data-infra-namenode-1 shared-data-infra-datanode-1)
for name in "${SHARED_EXPECTED[@]}"; do
  if docker ps --format '{{.Names}}' | grep -qx "$name"; then
    pass "Shared container $name is running"
  else
    fail "Shared container $name is NOT running"
  fi
done

echo ""
echo "--- Phase 5: Verify Kafka (produce -> consume roundtrip) ---"
TOPIC="fdb-smoke-test-$$"
if shared_streaming exec -T kafka kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null 2>&1; then
  pass "Kafka topic $TOPIC created"
else
  fail "Kafka topic creation failed"
fi

MSG="smoke-test-$(date +%s)"
if printf '%s\n' "$MSG" | shared_streaming exec -T kafka kafka-console-producer \
  --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$TOPIC" >/dev/null 2>&1; then
  pass "Kafka message produced"
else
  fail "Kafka message production failed"
fi

CONSUMED=$(shared_streaming exec -T kafka kafka-console-consumer \
  --bootstrap-server "$KAFKA_BOOTSTRAP" --topic "$TOPIC" \
  --from-beginning --max-messages 1 --timeout-ms 10000 2>/dev/null || true)
if [ "$CONSUMED" = "$MSG" ]; then
  pass "Kafka message consumed: '$CONSUMED'"
else
  fail "Kafka consume mismatch. Expected '$MSG', got '$CONSUMED'"
fi

shared_streaming exec -T kafka kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --delete --topic "$TOPIC" >/dev/null 2>&1 || true
pass "Kafka test topic cleaned up"

echo ""
echo "--- Phase 6: Verify Hive and HDFS ---"
if shared_lakehouse exec -T hive-server beeline \
  -u jdbc:hive2://localhost:10000/default -n hive -p hive \
  -e "SELECT 1" >/dev/null 2>&1; then
  pass "HiveServer2 query succeeded"
else
  fail "HiveServer2 query failed"
fi

if shared_hdfs_exec -test -d /warehouse/fdb/cell_kpi; then
  pass "HDFS warehouse path /warehouse/fdb/cell_kpi exists"
else
  fail "HDFS warehouse path /warehouse/fdb/cell_kpi is missing"
fi

echo ""
echo "--- Phase 7: Verify MySQL (CRUD roundtrip) ---"
MYSQL_CMD="docker exec fdb-mysql mysql -ufdb -pfdbpwd fdb"

if $MYSQL_CMD -e "SELECT VERSION();" >/dev/null 2>&1; then
  pass "MySQL connection succeeded"
else
  fail "MySQL version check failed"
fi

$MYSQL_CMD -e "
  CREATE TABLE IF NOT EXISTS smoke_test (
    id INT AUTO_INCREMENT PRIMARY KEY,
    msg VARCHAR(255)
  ) ENGINE=InnoDB CHARSET=utf8mb4;
" >/dev/null 2>&1
pass "MySQL table smoke_test created"

$MYSQL_CMD -e "INSERT INTO smoke_test (msg) VALUES ('hello'), ('world');" >/dev/null 2>&1
ROW_COUNT=$($MYSQL_CMD -N -e "SELECT COUNT(*) FROM smoke_test;" 2>/dev/null)
if [ "$ROW_COUNT" = "2" ]; then
  pass "MySQL inserted 2 rows, count=$ROW_COUNT"
else
  fail "MySQL row count expected 2, got $ROW_COUNT"
fi

$MYSQL_CMD -e "DROP TABLE smoke_test;" >/dev/null 2>&1
pass "MySQL test table cleaned up"

echo ""
echo "--- Phase 8: Verify observability endpoints ---"
if curl -s -o /dev/null -w "%{http_code}" http://localhost:18080/api/metrics/summary 2>/dev/null | grep -q "200"; then
  pass "Observability API metrics endpoint is reachable"
else
  fail "Observability API metrics endpoint is not reachable"
fi

if curl -s -o /dev/null -w "%{http_code}" http://localhost:5173 2>/dev/null | grep -q "200"; then
  pass "Frontend is reachable"
else
  fail "Frontend is not reachable"
fi

echo ""
echo "--- Phase 9: Stop project stack ---"
bash scripts/dev-down.sh 2>&1 | sed 's/^/  /'
sleep 3

for name in "${PROJECT_EXPECTED[@]}"; do
  if docker ps --format '{{.Names}}' | grep -qx "$name"; then
    fail "Project container $name still running after shutdown"
  else
    pass "Project container $name stopped"
  fi
done

echo ""
echo "Shared infrastructure was left running intentionally."
echo "============================"
echo -e "Results: ${GREEN}$PASS passed${NC}, ${RED}$FAIL failed${NC}"
echo "============================"

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
