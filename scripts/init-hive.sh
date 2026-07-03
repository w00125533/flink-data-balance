#!/usr/bin/env bash
set -euo pipefail

export MSYS_NO_PATHCONV=1

SHARED_INFRA_DIR=${SHARED_INFRA_DIR:-../shared-data-infra}

docker compose -f "$SHARED_INFRA_DIR/compose.yaml" -f "$SHARED_INFRA_DIR/compose.lakehouse.yaml" \
  --profile lakehouse --profile lakehouse-tools exec -T hive-server beeline \
  -u 'jdbc:hive2://localhost:10000/default' \
  -f /dev/stdin < docs/hive-schema.q
