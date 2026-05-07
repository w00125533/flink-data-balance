#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ "${1:-}" == "--clean" ]]; then
  echo "[dev-down] Stopping and removing containers + data volumes (./docker/data)..."
  docker compose -f docker/docker-compose.yml down -v
  rm -rf docker/data
else
  echo "[dev-down] Stopping containers (keeping ./docker/data)..."
  docker compose -f docker/docker-compose.yml down
fi
