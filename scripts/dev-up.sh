#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[dev-up] Starting local dependency containers (Kafka / MySQL / HMS / Postgres)..."
docker compose -f docker/docker-compose.yml up -d

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

echo "[dev-up] Waiting for Hive Metastore to be ready (up to 90s)..."
HMS_READY=0
for i in $(seq 1 45); do
  STATUS=$(docker inspect -f '{{.State.Status}}' fdb-hive-metastore 2>/dev/null || echo "")
  if [ "$STATUS" = "running" ]; then
    echo "[dev-up] Hive Metastore OK"
    HMS_READY=1
    break
  fi
  sleep 2
done
[ "$HMS_READY" = 1 ] || { echo "[dev-up] Hive Metastore did not become ready"; exit 1; }

echo "[dev-up] Creating Kafka topics..."
bash scripts/create-kafka-topics.sh

echo "[dev-up] Initializing MySQL tables..."
docker exec -i fdb-mysql mysql -ufdb -pfdbpwd fdb < scripts/init-mysql.sql

echo "[dev-up] All dependencies ready. kafka-ui: http://localhost:8080"
docker compose -f docker/docker-compose.yml ps
