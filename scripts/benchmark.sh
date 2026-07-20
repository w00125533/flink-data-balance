#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

usage() {
  echo "Usage: scripts/benchmark.sh <local|external-yarn> [runner args]"
  echo "Examples:"
  echo "  FDB_ENV_FILE=.env.local bash scripts/benchmark.sh local"
  echo "  bash scripts/benchmark.sh local --env benchmark-runner/conf/.env.benchmark-starrocks-30eps-scale-20260719-1148.tmp --dry-run"
  echo "  bash scripts/benchmark.sh local --env benchmark-runner/conf/.env.benchmark-starrocks-30eps-scale-20260719-1148.tmp --set FDB_BENCHMARK_CELL_LEVELS=2000 --dry-run"
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  usage
  exit 0
fi

TARGET="${1:-}"
if [[ -z "$TARGET" ]]; then
  usage >&2
  echo "[ERROR] missing target" >&2
  exit 1
fi
case "$TARGET" in
  local | external-yarn) ;;
  *)
    echo "[ERROR] unsupported target: $TARGET" >&2
    exit 1
    ;;
esac
shift

ENV_FILE="${FDB_ENV_FILE:-.env}"
RUNNER_HAS_ENV=0
RUNNER_ARGS=("$@")
for ((i = 0; i < ${#RUNNER_ARGS[@]}; i++)); do
  if [[ "${RUNNER_ARGS[$i]}" == "--env" ]]; then
    if (( i + 1 >= ${#RUNNER_ARGS[@]} )); then
      echo "[ERROR] --env requires a file path" >&2
      exit 1
    fi
    ENV_FILE="${RUNNER_ARGS[$((i + 1))]}"
    RUNNER_HAS_ENV=1
    i=$((i + 1))
  fi
done

if [[ ! -f "$ENV_FILE" ]]; then
  echo "[ERROR] env file not found: $ENV_FILE" >&2
  exit 1
fi

if [[ -z "${FDB_BENCHMARK_BASH:-}" ]] && command -v cygpath >/dev/null 2>&1; then
  BASH_BIN="$(command -v bash || true)"
  if [[ -n "$BASH_BIN" ]]; then
    FDB_BENCHMARK_BASH="$(cygpath -w "$BASH_BIN")"
    export FDB_BENCHMARK_BASH
  fi
fi

env_file_lookup() {
  local key="$1"
  local line value
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^[[:space:]]*$ ]] && continue
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ "$line" == "$key="* ]] || continue
    value="${line#*=}"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    if [[ "$value" == \"*\" && "$value" == *\" ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "$value" == \'* && "$value" == *\' ]]; then
      value="${value:1:${#value}-2}"
    fi
    printf '%s\n' "$value"
    return 0
  done < "$ENV_FILE"
  return 1
}

JAR="${FDB_BENCHMARK_RUNNER_JAR:-$(env_file_lookup FDB_BENCHMARK_RUNNER_JAR || true)}"
JAR="${JAR:-benchmark-runner/target/benchmark-runner-0.1.0-SNAPSHOT.jar}"
if [[ ! -f "$JAR" ]]; then
  echo "[ERROR] benchmark-runner jar not found: $JAR" >&2
  echo "[ERROR] build it with: mvn -pl benchmark-runner -am package" >&2
  exit 1
fi

if [[ "$RUNNER_HAS_ENV" == "1" ]]; then
  exec java -jar "$JAR" "$TARGET" "$@"
fi

exec java -jar "$JAR" "$TARGET" --env "$ENV_FILE" "$@"
