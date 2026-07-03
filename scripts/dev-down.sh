#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

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
  for name in "${legacy[@]}"; do
    if docker ps -aq --filter "name=^/${name}$" | grep -q .; then
      docker rm -f "$name" >/dev/null 2>&1 || true
    fi
  done
}

if [[ "${1:-}" == "--clean" ]]; then
  echo "[dev-down] Stopping and removing project containers + data volumes (./docker/data)..."
  docker compose -f docker/docker-compose.yml down -v
  remove_legacy_local_infra
  rm -rf docker/data
else
  echo "[dev-down] Stopping containers (keeping ./docker/data)..."
  docker compose -f docker/docker-compose.yml down
fi
