#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[dev-up] Starting local dependency containers (Kafka / MySQL / HMS / Postgres)..."
docker compose -f docker/docker-compose.yml up -d

echo "[dev-up] Waiting for Kafka to be ready (up to 60s)..."
for i in $(seq 1 30); do
  if docker compose -f docker/docker-compose.yml exec -T kafka \
     kafka-broker-api-versions --bootstrap-server kafka:29092 >/dev/null 2>&1; then
    echo "[dev-up] Kafka OK"
    break
  fi
  sleep 2
done

echo "[dev-up] Waiting for MySQL to be ready (up to 60s)..."
for i in $(seq 1 30); do
  if docker compose -f docker/docker-compose.yml exec -T mysql \
     mysqladmin ping -h localhost -ufdb -pfdbpwd --silent >/dev/null 2>&1; then
    echo "[dev-up] MySQL OK"
    break
  fi
  sleep 2
done

echo "[dev-up] All dependencies ready. kafka-ui: http://localhost:8080"
docker compose -f docker/docker-compose.yml ps
