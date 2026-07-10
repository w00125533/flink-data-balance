#!/usr/bin/env bash
set -euo pipefail

SHARED_INFRA_DIR=${SHARED_INFRA_DIR:-../shared-data-infra}
INTERNAL_BOOTSTRAP=${FDB_KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092}

shared_kafka() {
  docker compose -f "$SHARED_INFRA_DIR/compose.yaml" -f "$SHARED_INFRA_DIR/compose.streaming.yaml" --profile streaming \
    exec -T kafka "$@"
}

create_topic() {
  local name=$1
  local partitions=$2
  local cleanup=$3
  local retention_ms=${4:-}

  local extra=""
  if [[ -n "$retention_ms" && "$cleanup" == "delete" ]]; then
    extra="--config retention.ms=$retention_ms"
  fi

  echo "[create] $name partitions=$partitions cleanup=$cleanup retention=${retention_ms:-default}"
  shared_kafka kafka-topics \
    --bootstrap-server "$INTERNAL_BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config "cleanup.policy=$cleanup" \
    $extra
}

# Business topics
create_topic chr-events       64 delete  604800000     # 7d
create_topic mr-stats         16 delete  259200000     # 3d
create_topic cm-config         8 compact
create_topic topology          4 compact

# Load balancing control flow
create_topic lb-heartbeat      1 delete  3600000       # 1h
create_topic lb-routing        1 compact
create_topic fdb-stage-metrics 1 delete  3600000       # 1h

# Flink output
create_topic anomaly-events   16 delete  604800000     # 7d
create_topic cell-kpi-1m       8 delete  259200000     # 3d
create_topic cell-kpi-5m       8 delete  604800000     # 7d

# DLQ / late events
create_topic chr-dlq           4 delete  604800000
create_topic mr-dlq            4 delete  604800000
create_topic cm-dlq            4 delete  604800000
create_topic enrichment-late   4 delete  604800000

echo
echo "[done] Current topic list:"
shared_kafka kafka-topics \
  --bootstrap-server "$INTERNAL_BOOTSTRAP" --list | sort
