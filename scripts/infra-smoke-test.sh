#!/usr/bin/env bash
set -euo pipefail

# infra-smoke-test.sh — 验证基础设施完整生命周期：启动 → 功能验证 → 关闭
# 用法: ./scripts/infra-smoke-test.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PASS=0
FAIL=0

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}✓${NC} $1"; PASS=$((PASS + 1)); }
fail() { echo -e "  ${RED}✗${NC} $1"; FAIL=$((FAIL + 1)); }

# --- 前置检查 ---
echo "=== infra-smoke-test ==="
echo ""

if ! command -v docker &>/dev/null; then
  echo "FATAL: docker not found in PATH"
  exit 1
fi

if ! docker ps &>/dev/null; then
  echo "FATAL: docker daemon is not reachable"
  exit 1
fi

echo "--- Phase 1: Clean shutdown ---"
cd "$ROOT_DIR"
bash scripts/dev-down.sh --clean 2>&1 | sed 's/^/  /'
# 确保残余容器已清理
for c in fdb-zookeeper fdb-kafka fdb-kafka-ui fdb-mysql fdb-hms-postgres fdb-hive-metastore; do
  if docker ps -q --filter "name=$c" | grep -q . 2>/dev/null; then
    docker rm -f "$c" >/dev/null 2>&1 || true
  fi
done
pass "Phase 1: All containers stopped and cleaned"

echo ""
echo "--- Phase 2: Startup ---"
bash scripts/dev-up.sh 2>&1 | sed 's/^/  /'
pass "Phase 2: dev-up.sh completed"

echo ""
echo "--- Phase 3: Verify all containers are running ---"
EXPECTED=(fdb-zookeeper fdb-kafka fdb-kafka-ui fdb-mysql fdb-hms-postgres fdb-hive-metastore)
for name in "${EXPECTED[@]}"; do
  if docker ps --format '{{.Names}}' | grep -qx "$name"; then
    pass "Container $name is running"
  else
    fail "Container $name is NOT running"
  fi
done

echo ""
echo "--- Phase 4: Verify Kafka (produce -> consume roundtrip) ---"
TOPIC="fdb-smoke-test-$$"
if docker exec fdb-kafka kafka-topics --bootstrap-server kafka:29092 \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null 2>&1; then
  pass "Kafka topic $TOPIC created"
else
  fail "Kafka topic creation failed"
fi

MSG="smoke-test-$(date +%s)"
if echo "$MSG" | docker exec -i fdb-kafka kafka-console-producer \
  --bootstrap-server kafka:29092 --topic "$TOPIC" >/dev/null 2>&1; then
  pass "Kafka message produced"
else
  fail "Kafka message production failed"
fi

CONSUMED=$(docker exec fdb-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic "$TOPIC" \
  --from-beginning --max-messages 1 --timeout-ms 10000 2>/dev/null)
if [ "$CONSUMED" = "$MSG" ]; then
  pass "Kafka message consumed: '$CONSUMED'"
else
  fail "Kafka consume mismatch. Expected '$MSG', got '$CONSUMED'"
fi

docker exec fdb-kafka kafka-topics --bootstrap-server kafka:29092 \
  --delete --topic "$TOPIC" >/dev/null 2>&1
pass "Kafka test topic cleaned up"

echo ""
echo "--- Phase 5: Verify MySQL (CRUD roundtrip) ---"
MYSQL_CMD="docker exec fdb-mysql mysql -ufdb -pfdbpwd fdb"

if $MYSQL_CMD -e "SELECT VERSION();" 2>/dev/null | grep -q "8.0"; then
  pass "MySQL version 8.0 confirmed"
else
  fail "MySQL version check failed"
fi

$MYSQL_CMD -e "
  CREATE TABLE IF NOT EXISTS smoke_test (
    id INT AUTO_INCREMENT PRIMARY KEY,
    msg VARCHAR(255)
  ) ENGINE=InnoDB CHARSET=utf8mb4;
" 2>/dev/null
pass "MySQL table smoke_test created"

$MYSQL_CMD -e "INSERT INTO smoke_test (msg) VALUES ('hello'), ('world');" 2>/dev/null
ROW_COUNT=$($MYSQL_CMD -N -e "SELECT COUNT(*) FROM smoke_test;" 2>/dev/null)
if [ "$ROW_COUNT" = "2" ]; then
  pass "MySQL inserted 2 rows, count=$ROW_COUNT"
else
  fail "MySQL row count expected 2, got $ROW_COUNT"
fi

$MYSQL_CMD -e "DROP TABLE smoke_test;" 2>/dev/null
pass "MySQL test table cleaned up"

echo ""
echo "--- Phase 6: Verify Kafka-UI is reachable ---"
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080 2>/dev/null | grep -q "200\|302\|401"; then
  pass "Kafka-UI HTTP accessible at localhost:8080"
else
  fail "Kafka-UI not reachable at localhost:8080"
fi

echo ""
echo "--- Phase 7: Shutdown ---"
bash scripts/dev-down.sh 2>&1 | sed 's/^/  /'
sleep 3

for name in "${EXPECTED[@]}"; do
  if docker ps --format '{{.Names}}' | grep -qx "$name"; then
    fail "Container $name still running after shutdown"
  else
    pass "Container $name stopped"
  fi
done

echo ""
echo "============================"
echo -e "Results: ${GREEN}$PASS passed${NC}, ${RED}$FAIL failed${NC}"
echo "============================"

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
