#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

# Git Bash rewrites container-internal paths and URI-like arguments unless this is disabled.
export MSYS_NO_PATHCONV=1

SHARED_INFRA_DIR=${SHARED_INFRA_DIR:-../shared-data-infra}
HDFS_URI=${FDB_HDFS_URI:-hdfs://namenode:8020}
KAFKA_BOOTSTRAP=${FDB_KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092}

shared_streaming() {
  docker compose -f "$SHARED_INFRA_DIR/compose.yaml" -f "$SHARED_INFRA_DIR/compose.streaming.yaml" --profile streaming "$@"
}

shared_lakehouse() {
  docker compose -f "$SHARED_INFRA_DIR/compose.yaml" -f "$SHARED_INFRA_DIR/compose.lakehouse.yaml" --profile lakehouse --profile lakehouse-tools "$@"
}

echo "[dev-up] Checking shared infrastructure..."
if ! docker network inspect shared-data-infra >/dev/null 2>&1; then
  echo "[dev-up] shared-data-infra network is missing."
  echo "[dev-up] Start shared infrastructure first:"
  echo "[dev-up]   cd ../shared-data-infra && sh scripts/infra-up.sh lakehouse lakehouse-tools streaming"
  exit 1
fi

echo "[dev-up] Waiting for shared Kafka to be ready (up to 60s)..."
KAFKA_READY=0
for i in $(seq 1 30); do
  if shared_streaming exec -T kafka \
     kafka-broker-api-versions --bootstrap-server "$KAFKA_BOOTSTRAP" >/dev/null 2>&1; then
    echo "[dev-up] Shared Kafka OK"
    KAFKA_READY=1
    break
  fi
  sleep 2
done
[ "$KAFKA_READY" = 1 ] || { echo "[dev-up] Shared Kafka did not become ready"; exit 1; }

echo "[dev-up] Waiting for shared HiveServer2 to be ready (up to 90s)..."
HIVE_READY=0
for i in $(seq 1 45); do
  if shared_lakehouse exec -T hive-server \
     beeline -u jdbc:hive2://localhost:10000/default -e 'SELECT 1' >/dev/null 2>&1; then
    echo "[dev-up] Shared HiveServer2 OK"
    HIVE_READY=1
    break
  fi
  sleep 2
done
[ "$HIVE_READY" = 1 ] || { echo "[dev-up] Shared HiveServer2 did not become ready"; exit 1; }

echo "[dev-up] Preparing shared HDFS warehouse directories..."
shared_lakehouse exec -T namenode \
  hdfs dfs -fs "$HDFS_URI" -mkdir -p /warehouse/fdb/cell_kpi /warehouse/iceberg
shared_lakehouse exec -T namenode \
  hdfs dfs -fs "$HDFS_URI" -chmod -R 777 /warehouse/fdb /warehouse/iceberg

echo "[dev-up] Starting local project containers (MySQL / Flink runtime / observability)..."
docker compose -f docker/docker-compose.yml --profile e2e up -d \
  mysql \
  observability-api \
  prometheus \
  frontend \
  jobmanager \
  taskmanager

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

echo "[dev-up] Creating Kafka topics..."
bash scripts/create-kafka-topics.sh

echo "[dev-up] Initializing MySQL tables..."
docker exec -i fdb-mysql mysql -ufdb -pfdbpwd fdb < scripts/init-mysql.sql

echo "[dev-up] Initializing shared Hive table..."
bash scripts/init-hive.sh

echo "[dev-up] All dependencies ready. Shared Kafka: localhost:9092; frontend: http://localhost:5173"
docker compose -f docker/docker-compose.yml ps
