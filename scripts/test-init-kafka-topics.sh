#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

TEST_TMP_DIR="$(mktemp -d)"
FAKE_BIN_DIR="$TEST_TMP_DIR/bin"
FAKE_DOCKER_LOG="$TEST_TMP_DIR/docker.log"
trap 'rm -rf "$TEST_TMP_DIR"' EXIT

mkdir -p "$FAKE_BIN_DIR"
cat > "$FAKE_BIN_DIR/docker" <<'SH'
#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' "$*" >> "${FAKE_DOCKER_LOG:?}"

if [[ "$*" == *" kafka-topics "*" --list"* ]]; then
  printf 'chr-events\n'
fi

exit 0
SH
chmod +x "$FAKE_BIN_DIR/docker"

export PATH="$FAKE_BIN_DIR:$PATH"
export FAKE_DOCKER_LOG

fail() {
  echo "[test-fail] $*" >&2
  exit 1
}

FDB_CHR_RETENTION_MS=3600000 FDB_KAFKA_SEGMENT_MS=600000 \
  bash scripts/init-kafka-topics.sh >/dev/null

grep -F -- "--add-config cleanup.policy=delete,retention.ms=3600000,segment.ms=600000" "$FAKE_DOCKER_LOG" >/dev/null \
  || fail "delete topics with retention should alter segment.ms"

grep -F -- "--config segment.ms=600000" "$FAKE_DOCKER_LOG" >/dev/null \
  || fail "delete topics with retention should create with segment.ms"

echo "[test-ok] init kafka topics"
