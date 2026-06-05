#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[dev-up] Checking shared lakehouse infrastructure..."
if ! docker network inspect shared-data-infra >/dev/null 2>&1; then
  echo "[dev-up] shared-data-infra network is missing."
  echo "[dev-up] Start shared lakehouse first:"
  echo "[dev-up]   cd ../shared-data-infra && powershell -ExecutionPolicy Bypass -File scripts/infra-up.ps1 -Profiles lakehouse"
  exit 1
fi

echo "[dev-up] Starting local dependency containers (Kafka / MySQL / HiveServer2 / Flink runtime)..."
docker compose -f docker/docker-compose.yml --profile e2e up -d \
  zookeeper \
  kafka \
  kafka-ui \
  mysql \
  hive-server \
  jobmanager \
  taskmanager

echo "[dev-up] Waiting for Kafka to be ready (up to 60s)..."
KAFKA_READY=0
for i in $(seq 1 30); do
  if docker compose -f docker/docker-compose.yml exec -T kafka \
     kafka-broker-api-versions --bootstrap-server kafka:29092 >/dev/null 2>&1; then
    echo "[dev-up] Kafka OK"
    KAFKA_READY=1
    break
  fi
  sleep 2
done
[ "$KAFKA_READY" = 1 ] || { echo "[dev-up] Kafka did not become ready"; exit 1; }

echo "[dev-up] Waiting for MySQL to be ready (up to 60s)..."
MYSQL_READY=0
for i in $(seq 1 30); do
  if docker compose -f docker/docker-compose.yml exec -T mysql \
     mysqladmin ping -h localhost -ufdb -pfdbpwd --silent >/dev/null 2>&1; then
    echo "[dev-up] MySQL OK"
    MYSQL_READY=1
    break
  fi
  sleep 2
done
[ "$MYSQL_READY" = 1 ] || { echo "[dev-up] MySQL did not become ready"; exit 1; }

echo "[dev-up] Waiting for HiveServer2 to reach shared Hive Metastore (up to 90s)..."
HIVE_READY=0
for i in $(seq 1 45); do
  if docker exec fdb-hive-server beeline -u jdbc:hive2://localhost:10000/default -e 'SELECT 1' >/dev/null 2>&1; then
    echo "[dev-up] HiveServer2 OK"
    HIVE_READY=1
    break
  fi
  sleep 2
done
[ "$HIVE_READY" = 1 ] || { echo "[dev-up] HiveServer2 did not become ready"; exit 1; }

echo "[dev-up] Creating Kafka topics..."
bash scripts/create-kafka-topics.sh

echo "[dev-up] Initializing MySQL tables..."
docker exec -i fdb-mysql mysql -ufdb -pfdbpwd fdb < scripts/init-mysql.sql

echo "[dev-up] All dependencies ready. kafka-ui: http://localhost:8080"
docker compose -f docker/docker-compose.yml ps
