#!/usr/bin/env bash
set -euo pipefail

KAFKA_CONTAINER=${FDB_KAFKA_CONTAINER:-fdb-kafka}
INTERNAL_BOOTSTRAP=${FDB_KAFKA_INTERNAL_BOOTSTRAP:-kafka:29092}

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
  docker exec "$KAFKA_CONTAINER" kafka-topics \
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
docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server "$INTERNAL_BOOTSTRAP" --list | sort
