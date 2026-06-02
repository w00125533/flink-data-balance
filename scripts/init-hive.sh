#!/usr/bin/env bash
set -euo pipefail

docker exec -i fdb-hive-server beeline \
  -u 'jdbc:hive2://localhost:10000/default' \
  -f /dev/stdin < docs/hive-schema.q
