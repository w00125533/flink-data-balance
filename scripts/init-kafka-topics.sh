#!/usr/bin/env bash
set -euo pipefail

SHARED_INFRA_DIR=${SHARED_INFRA_DIR:-../shared-data-infra}
INTERNAL_BOOTSTRAP=${FDB_KAFKA_INTERNAL_BOOTSTRAP:-kafka:9092}

shared_kafka() {
  if docker compose -f "$SHARED_INFRA_DIR/compose.yaml" -f "$SHARED_INFRA_DIR/compose.streaming.yaml" --profile streaming \
    exec -T kafka "$@" >/tmp/fdb-shared-kafka-exec.out 2>/tmp/fdb-shared-kafka-exec.err; then
    cat /tmp/fdb-shared-kafka-exec.out
    return 0
  fi

  docker exec shared-data-infra-kafka-1 "$@"
}

create_topic() {
  local name=$1
  local partitions=$2
  local cleanup=$3
  local retention_ms=${4:-}
  local segment_ms=${5:-}

  local extra=()
  local alter_config="cleanup.policy=$cleanup"
  if [[ -n "$retention_ms" && "$cleanup" == "delete" ]]; then
    segment_ms="${segment_ms:-${FDB_KAFKA_SEGMENT_MS:-600000}}"
    extra+=(--config "retention.ms=$retention_ms")
    extra+=(--config "segment.ms=$segment_ms")
    alter_config+=",retention.ms=$retention_ms,segment.ms=$segment_ms"
  fi

  echo "[create] $name partitions=$partitions cleanup=$cleanup retention=${retention_ms:-default} segment=${segment_ms:-default}"
  shared_kafka kafka-topics \
    --bootstrap-server "$INTERNAL_BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config "cleanup.policy=$cleanup" \
    "${extra[@]}"

  shared_kafka kafka-configs \
    --bootstrap-server "$INTERNAL_BOOTSTRAP" \
    --alter \
    --entity-type topics \
    --entity-name "$name" \
    --add-config "$alter_config" >/dev/null
}

# Business topics
create_topic chr-events       64 delete  "${FDB_CHR_RETENTION_MS:-604800000}"      # default 7d
create_topic pm-stats         16 delete  "${FDB_PM_RETENTION_MS:-259200000}"       # default 3d
create_topic cfg-config        8 compact
create_topic topology          4 compact

# Load balancing control flow
create_topic lb-heartbeat      1 delete  3600000       # 1h
create_topic lb-routing        1 compact
create_topic fdb-stage-metrics 1 delete  "${FDB_METRICS_RETENTION_MS:-3600000}"    # default 1h

# Flink output
create_topic anomaly-events   16 delete  "${FDB_ANOMALY_RETENTION_MS:-604800000}"  # default 7d
create_topic cell-kpi-1m       8 delete  "${FDB_KPI_1M_RETENTION_MS:-259200000}"   # default 3d
create_topic cell-kpi-5m       8 delete  "${FDB_KPI_5M_RETENTION_MS:-604800000}"   # default 7d

# DLQ / late events
create_topic chr-dlq           4 delete  604800000
create_topic mr-dlq            4 delete  604800000
create_topic cm-dlq            4 delete  604800000
create_topic enrichment-late   4 delete  604800000

echo
echo "[done] Current topic list:"
shared_kafka kafka-topics \
  --bootstrap-server "$INTERNAL_BOOTSTRAP" --list | sort
