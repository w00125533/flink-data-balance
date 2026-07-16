# Benchmark Runner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Java `benchmark-runner` that performs sink upper-bound benchmarks with cell-count pressure levels, Flink REST observations, storage probes, and static HTML reports.

**Architecture:** Add a new Maven module for benchmark orchestration and keep shell as a thin launcher. The runner expands a `sink x cellLevel` matrix, drives existing `deploy.sh submit/stop`, starts topology and simulator processes on the runner host, samples Flink REST plus fdb/storage metrics, stops escalation on unstable runs, and writes HTML/JSON/CSV artifacts under `benchmark-runner/output/benchmark-runs/<benchmarkId>/`.

**Tech Stack:** Java 17, Maven, Jackson, Java `HttpClient`, JUnit 5, AssertJ, Bash launcher tests.

---

## Scope And Guardrails

- Implement the approved design in `docs/superpowers/specs/2026-04-29-flink-data-balance-design.md`, section `7.4` and `12.3`.
- This plan creates a new Java module and a new shell launcher. It should not move existing Flink job classes.
- If an implementation step modifies an existing Java class or method outside the new `benchmark-runner` module, run GitNexus impact analysis before editing that symbol.
- Do not write benchmark outputs into Docker directories. The fixed output root is `benchmark-runner/output/benchmark-runs`.
- Keep `deploy.sh` deployment behavior centralized. The runner calls `scripts/deploy.sh <target> submit/stop/status`; it does not reimplement Flink submit commands.
- Use TDD for Java logic. Shell tests only cover `scripts/benchmark.sh` launcher behavior.

## File Structure

### New Module

- `benchmark-runner/pom.xml`: Maven module with `common`, Jackson, SLF4J, and test dependencies; shaded executable jar with main class `com.fdb.benchmark.BenchmarkRunnerMain`.
- `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkRunnerMain.java`: CLI entry, `.env` loading, main orchestration call.
- `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkConfig.java`: parses target, sinks, cell levels, EPS per cell, durations, thresholds, URLs, and paths.
- `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkSink.java`: enum for `none`, `starrocks`, `kafka`, `hive`, `iceberg`.
- `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkMatrix.java`: expands ordered `sink x cellLevel` plans.
- `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkRunPlan.java`: one planned run with sink, cell count, target CHR EPS, run id, and run label.
- `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkRunResult.java`: one run result with status, metrics summary, snapshots, timings, and bottleneck reason.
- `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkStatus.java`: enum `STABLE`, `UNSTABLE`, `FAILED`.
- `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkThresholds.java`: threshold values and defaults.
- `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkDecisionEngine.java`: converts sampled observations into stable/unstable/failed.
- `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkOrchestrator.java`: end-to-end matrix loop and escalation stopping.
- `benchmark-runner/src/main/java/com/fdb/benchmark/CommandRunner.java`: small interface for process execution.
- `benchmark-runner/src/main/java/com/fdb/benchmark/DeployCommandClient.java`: invokes `scripts/deploy.sh`.
- `benchmark-runner/src/main/java/com/fdb/benchmark/SimulatorProcessManager.java`: starts/stops topology-service and cfg/pm/chr simulators.
- `benchmark-runner/src/main/java/com/fdb/benchmark/FlinkRestClient.java`: fetches job, vertex, checkpoint, and metric observations from Flink REST.
- `benchmark-runner/src/main/java/com/fdb/benchmark/ObservabilityClient.java`: fetches fdb runtime/stage/sink summaries.
- `benchmark-runner/src/main/java/com/fdb/benchmark/StorageProbe.java`: interface for sink-specific storage checks.
- `benchmark-runner/src/main/java/com/fdb/benchmark/KafkaStorageProbe.java`: Kafka offsets and output topic growth probe.
- `benchmark-runner/src/main/java/com/fdb/benchmark/StarRocksStorageProbe.java`: StarRocks row count and recent window probe.
- `benchmark-runner/src/main/java/com/fdb/benchmark/HiveStorageProbe.java`: Hive/HDFS file, small-file, and in-progress probe.
- `benchmark-runner/src/main/java/com/fdb/benchmark/IcebergStorageProbe.java`: Iceberg file, metadata, snapshot, and partition probe.
- `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkResultWriter.java`: writes `benchmark-config.json`, `benchmark-results.json`, and `benchmark-summary.csv`.
- `benchmark-runner/src/main/java/com/fdb/benchmark/HtmlReportWriter.java`: writes `index.html` and per-run `report.html`.

### Tests

- `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkConfigTest.java`
- `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkMatrixTest.java`
- `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkDecisionEngineTest.java`
- `benchmark-runner/src/test/java/com/fdb/benchmark/FlinkRestClientTest.java`
- `benchmark-runner/src/test/java/com/fdb/benchmark/ObservabilityClientTest.java`
- `benchmark-runner/src/test/java/com/fdb/benchmark/StorageProbeTest.java`
- `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkOrchestratorTest.java`
- `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkResultWriterTest.java`
- `benchmark-runner/src/test/java/com/fdb/benchmark/HtmlReportWriterTest.java`
- `scripts/benchmark.sh`
- `scripts/test-benchmark-dispatch.sh`

---

### Task 1: Maven Module And Thin Launcher

**Files:**
- Modify: `pom.xml`
- Create: `benchmark-runner/pom.xml`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkRunnerMain.java`
- Create: `scripts/benchmark.sh`
- Create: `scripts/test-benchmark-dispatch.sh`

- [ ] **Step 1: Add failing launcher test**

Create `scripts/test-benchmark-dispatch.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

TEST_TMP_DIR="$(mktemp -d)"
OUT_FILE="$TEST_TMP_DIR/benchmark-test.out"
ERR_FILE="$TEST_TMP_DIR/benchmark-test.err"
FAKE_BIN_DIR="$TEST_TMP_DIR/bin"
FAKE_JAVA_LOG="$TEST_TMP_DIR/java.log"
trap 'rm -rf "$TEST_TMP_DIR"' EXIT

mkdir -p "$FAKE_BIN_DIR"
cat > "$FAKE_BIN_DIR/java" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "${FAKE_JAVA_LOG:?}"
SH
chmod +x "$FAKE_BIN_DIR/java"
export FAKE_JAVA_LOG

fail() {
  echo "[test-fail] $*" >&2
  exit 1
}

run_expect_success() {
  local description=$1
  shift
  if ! "$@" >"$OUT_FILE" 2>"$ERR_FILE"; then
    cat "$OUT_FILE" >&2 || true
    cat "$ERR_FILE" >&2 || true
    fail "$description should succeed"
  fi
}

run_expect_failure() {
  local description=$1
  shift
  if "$@" >"$OUT_FILE" 2>"$ERR_FILE"; then
    cat "$OUT_FILE" >&2 || true
    cat "$ERR_FILE" >&2 || true
    fail "$description should fail"
  fi
}

run_expect_success "help" bash scripts/benchmark.sh --help
grep -F "Usage: scripts/benchmark.sh <local|external-yarn>" "$OUT_FILE" \
  || fail "help should show target usage"

run_expect_failure "missing jar" env PATH="$FAKE_BIN_DIR:$PATH" \
  bash scripts/benchmark.sh local
grep -F "benchmark-runner jar not found" "$ERR_FILE" \
  || fail "missing jar error should be explicit"

mkdir -p benchmark-runner/target
: > benchmark-runner/target/benchmark-runner-0.1.0-SNAPSHOT.jar
cat > "$TEST_TMP_DIR/test.env" <<'ENV'
FDB_BENCHMARK_SINKS=none starrocks
FDB_BENCHMARK_CELL_LEVELS=1000 3000
ENV

run_expect_success "passes target and env file" env PATH="$FAKE_BIN_DIR:$PATH" \
  FDB_ENV_FILE="$TEST_TMP_DIR/test.env" \
  bash scripts/benchmark.sh local --dry-run
grep -F -- "-jar benchmark-runner/target/benchmark-runner-0.1.0-SNAPSHOT.jar local --env $TEST_TMP_DIR/test.env --dry-run" "$FAKE_JAVA_LOG" \
  || fail "benchmark.sh should pass target, env path, and arguments to java"

echo "[test-ok] benchmark dispatch"
```

- [ ] **Step 2: Run launcher test and verify failure**

Run:

```bash
bash scripts/test-benchmark-dispatch.sh
```

Expected: FAIL because `scripts/benchmark.sh` does not exist.

- [ ] **Step 3: Add Maven module declaration**

Modify root `pom.xml` and add the module after `observability-api`:

```xml
<module>benchmark-runner</module>
```

Create `benchmark-runner/pom.xml`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.fdb</groupId>
        <artifactId>flink-data-balance-parent</artifactId>
        <version>0.1.0-SNAPSHOT</version>
    </parent>

    <artifactId>benchmark-runner</artifactId>
    <name>flink-data-balance-benchmark-runner</name>

    <dependencies>
        <dependency>
            <groupId>com.fdb</groupId>
            <artifactId>common</artifactId>
            <version>${project.version}</version>
        </dependency>
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <scope>runtime</scope>
        </dependency>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-jar-plugin</artifactId>
                <version>3.5.0</version>
                <configuration>
                    <forceCreation>true</forceCreation>
                </configuration>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <version>3.5.2</version>
                <executions>
                    <execution>
                        <phase>package</phase>
                        <goals><goal>shade</goal></goals>
                        <configuration>
                            <createDependencyReducedPom>false</createDependencyReducedPom>
                            <transformers>
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                                    <mainClass>com.fdb.benchmark.BenchmarkRunnerMain</mainClass>
                                </transformer>
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 4: Add minimal main class**

Create `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkRunnerMain.java`:

```java
package com.fdb.benchmark;

import java.util.Arrays;

public final class BenchmarkRunnerMain {
  private BenchmarkRunnerMain() {
  }

  public static void main(String[] args) {
    if (args.length == 0 || "--help".equals(args[0]) || "-h".equals(args[0])) {
      System.out.println("Usage: benchmark-runner <local|external-yarn> [--env <file>] [--dry-run]");
      return;
    }
    String target = args[0];
    if (!"local".equals(target) && !"external-yarn".equals(target)) {
      System.err.println("Unsupported target: " + target);
      System.exit(2);
    }
    System.out.println("benchmark-runner target=" + target + " args=" + Arrays.toString(args));
  }
}
```

- [ ] **Step 5: Add `scripts/benchmark.sh`**

Create `scripts/benchmark.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

usage() {
  echo "Usage: scripts/benchmark.sh <local|external-yarn> [runner args]"
  echo "Examples:"
  echo "  FDB_ENV_FILE=.env.local bash scripts/benchmark.sh local"
  echo "  FDB_ENV_FILE=.env.external bash scripts/benchmark.sh external-yarn --dry-run"
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
if [[ ! -f "$ENV_FILE" ]]; then
  echo "[ERROR] env file not found: $ENV_FILE" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

JAR="${FDB_BENCHMARK_RUNNER_JAR:-benchmark-runner/target/benchmark-runner-0.1.0-SNAPSHOT.jar}"
if [[ ! -f "$JAR" ]]; then
  echo "[ERROR] benchmark-runner jar not found: $JAR" >&2
  echo "[ERROR] build it with: mvn -pl benchmark-runner -am package" >&2
  exit 1
fi

exec java -jar "$JAR" "$TARGET" --env "$ENV_FILE" "$@"
```

- [ ] **Step 6: Run tests**

Run:

```bash
bash scripts/test-benchmark-dispatch.sh
mvn -pl benchmark-runner test
```

Expected: both PASS.

- [ ] **Step 7: Commit**

Run:

```bash
git add pom.xml benchmark-runner/pom.xml benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkRunnerMain.java scripts/benchmark.sh scripts/test-benchmark-dispatch.sh
git commit -m "feat(benchmark): add runner module and launcher"
```

---

### Task 2: Configuration And Matrix Expansion

**Files:**
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkSink.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkThresholds.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkConfig.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkRunPlan.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkMatrix.java`
- Test: `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkConfigTest.java`
- Test: `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkMatrixTest.java`

- [ ] **Step 1: Write config tests**

Create `BenchmarkConfigTest.java`:

```java
package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BenchmarkConfigTest {
  @Test
  void parses_sink_levels_and_threshold_defaults() {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_SINKS", "none starrocks,kafka",
        "FDB_BENCHMARK_CELL_LEVELS", "1000 3000",
        "FDB_BENCHMARK_CHR_EPS_PER_CELL", "0.25",
        "FDB_BENCHMARK_WARMUP_SEC", "30",
        "FDB_BENCHMARK_DURATION_SEC", "120"));

    assertThat(config.target()).isEqualTo("local");
    assertThat(config.sinks()).containsExactly(BenchmarkSink.NONE, BenchmarkSink.STARROCKS, BenchmarkSink.KAFKA);
    assertThat(config.cellLevels()).containsExactly(1000, 3000);
    assertThat(config.chrEpsPerCell()).isEqualTo(0.25);
    assertThat(config.targetChrEps(3000)).isEqualTo(750);
    assertThat(config.warmupSec()).isEqualTo(30);
    assertThat(config.durationSec()).isEqualTo(120);
    assertThat(config.thresholds().maxCheckpointDurationMs()).isEqualTo(120_000L);
    assertThat(config.outputRoot()).isEqualTo(Path.of("benchmark-runner/output/benchmark-runs"));
  }

  @Test
  void rejects_output_dir_override() {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_OUTPUT_DIR", "/tmp/not-used"));

    assertThat(config.outputRoot()).isEqualTo(Path.of("benchmark-runner/output/benchmark-runs"));
  }

  @Test
  void rejects_invalid_sink_and_non_positive_level() {
    assertThatThrownBy(() -> BenchmarkConfig.from("local", Map.of("FDB_BENCHMARK_SINKS", "bad")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("unsupported benchmark sink");

    assertThatThrownBy(() -> BenchmarkConfig.from("local", Map.of("FDB_BENCHMARK_CELL_LEVELS", "1000 0")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cell levels must be positive");
  }
}
```

- [ ] **Step 2: Write matrix tests**

Create `BenchmarkMatrixTest.java`:

```java
package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class BenchmarkMatrixTest {
  @Test
  void expands_sink_then_cell_level_order_with_readable_run_ids() {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-20260716-153000",
        "FDB_BENCHMARK_SINKS", "none starrocks",
        "FDB_BENCHMARK_CELL_LEVELS", "1000 3000",
        "FDB_BENCHMARK_CHR_EPS_PER_CELL", "0.3"));

    assertThat(BenchmarkMatrix.expand(config))
        .extracting(BenchmarkRunPlan::runId)
        .containsExactly(
            "bench-20260716-153000-none-cells1000-eps300",
            "bench-20260716-153000-none-cells3000-eps900",
            "bench-20260716-153000-starrocks-cells1000-eps300",
            "bench-20260716-153000-starrocks-cells3000-eps900");
  }
}
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```bash
mvn -pl benchmark-runner -Dtest=BenchmarkConfigTest,BenchmarkMatrixTest test
```

Expected: FAIL because config and matrix classes do not exist.

- [ ] **Step 4: Implement config and matrix classes**

Implement the classes with these public APIs and rules:

```java
public enum BenchmarkSink {
  NONE("none"), STARROCKS("starrocks"), KAFKA("kafka"), HIVE("hive"), ICEBERG("iceberg");

  public static BenchmarkSink parse(String raw);
  public String value();
}

public record BenchmarkThresholds(
    double maxBackpressureRatio,
    long maxCheckpointDurationMs,
    int maxConsecutiveCheckpointFailures,
    long maxKpiAvailabilityP95Ms,
    long maxSinkP95Ms,
    long maxWatermarkLagMs) {
  public static BenchmarkThresholds from(Map<String, String> env);
}

public record BenchmarkConfig(
    String target,
    String benchmarkId,
    List<BenchmarkSink> sinks,
    List<Integer> cellLevels,
    double chrEpsPerCell,
    long warmupSec,
    long durationSec,
    long pollIntervalSec,
    URI flinkRestUrl,
    URI observabilityApiUrl,
    Path outputRoot,
    BenchmarkThresholds thresholds) {
  public static BenchmarkConfig from(String target, Map<String, String> env);
  public long targetChrEps(int cellLevel);
}

public record BenchmarkRunPlan(
    String benchmarkId,
    BenchmarkSink sink,
    int cellLevel,
    long targetChrEps,
    String runId,
    String runLabel) {
}

public final class BenchmarkMatrix {
  public static List<BenchmarkRunPlan> expand(BenchmarkConfig config);
}
```

Rules:

- Default sinks: `none starrocks kafka hive iceberg`.
- Default cell levels: `10000 20000 40000`.
- Default EPS per cell: `0.3`.
- Default output root: `benchmark-runner/output/benchmark-runs`.
- Ignore `FDB_BENCHMARK_OUTPUT_DIR`.
- Sanitize benchmarkId/runId to `[A-Za-z0-9._:-]`, replacing other characters with `-`.

- [ ] **Step 5: Run tests**

Run:

```bash
mvn -pl benchmark-runner -Dtest=BenchmarkConfigTest,BenchmarkMatrixTest test
```

Expected: PASS.

- [ ] **Step 6: Commit**

Run:

```bash
git add benchmark-runner/src/main/java/com/fdb/benchmark benchmark-runner/src/test/java/com/fdb/benchmark
git commit -m "feat(benchmark): parse config and expand matrix"
```

---

### Task 3: Decision Engine And Result Model

**Files:**
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkStatus.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/FlinkSnapshot.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/FdbMetricsSnapshot.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/StorageSnapshot.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/RunObservation.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkRunResult.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkDecisionEngine.java`
- Test: `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkDecisionEngineTest.java`

- [ ] **Step 1: Write decision tests**

Create `BenchmarkDecisionEngineTest.java`:

```java
package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BenchmarkDecisionEngineTest {
  private final BenchmarkThresholds thresholds = new BenchmarkThresholds(
      0.2, 120_000, 2, 180_000, 180_000, 180_000);
  private final BenchmarkDecisionEngine engine = new BenchmarkDecisionEngine(thresholds);

  @Test
  void failed_job_marks_run_failed() {
    RunObservation observation = healthy().withFlink(healthy().flink().withJobStatus("FAILED"));

    BenchmarkRunResult result = engine.decide(plan(), observation);

    assertThat(result.status()).isEqualTo(BenchmarkStatus.FAILED);
    assertThat(result.bottleneckReason()).contains("Flink job status FAILED");
  }

  @Test
  void sustained_backpressure_marks_unstable() {
    RunObservation observation = healthy().withFlink(healthy().flink().withBackpressureRatio(0.45));

    BenchmarkRunResult result = engine.decide(plan(), observation);

    assertThat(result.status()).isEqualTo(BenchmarkStatus.UNSTABLE);
    assertThat(result.bottleneckReason()).contains("backpressure");
  }

  @Test
  void high_kpi_or_sink_latency_marks_unstable() {
    assertThat(engine.decide(plan(), healthy().withFdb(healthy().fdb().withKpi1mP95Ms(181_000))).status())
        .isEqualTo(BenchmarkStatus.UNSTABLE);
    assertThat(engine.decide(plan(), healthy().withFdb(healthy().fdb().withSinkP95Ms(181_000))).status())
        .isEqualTo(BenchmarkStatus.UNSTABLE);
  }

  @Test
  void healthy_observation_is_stable() {
    BenchmarkRunResult result = engine.decide(plan(), healthy());

    assertThat(result.status()).isEqualTo(BenchmarkStatus.STABLE);
    assertThat(result.bottleneckReason()).isEqualTo("all thresholds healthy");
  }

  private static BenchmarkRunPlan plan() {
    return new BenchmarkRunPlan("bench-a", BenchmarkSink.NONE, 1000, 300, "bench-a-none-cells1000-eps300", "benchmark-none");
  }

  private static RunObservation healthy() {
    return new RunObservation(
        new FlinkSnapshot("RUNNING", 0.05, 30_000, 0, 10_000, 9_900, 4, 4),
        new FdbMetricsSnapshot(2_000, 40_000, 45_000, 5_000, 0, 20_000),
        new StorageSnapshot(true, "healthy", 100, 0, 0));
  }
}
```

- [ ] **Step 2: Run decision tests and verify failure**

Run:

```bash
mvn -pl benchmark-runner -Dtest=BenchmarkDecisionEngineTest test
```

Expected: FAIL because result model and engine do not exist.

- [ ] **Step 3: Implement decision model**

Create records with the exact `withJobStatus`, `withBackpressureRatio`,
`withKpi1mP95Ms`, and `withSinkP95Ms` helper methods used by tests:

```java
public enum BenchmarkStatus { STABLE, UNSTABLE, FAILED }

public record FlinkSnapshot(
    String jobStatus,
    double backpressureRatio,
    long checkpointDurationMs,
    int consecutiveCheckpointFailures,
    double recordsInPerSec,
    double recordsOutPerSec,
    int taskManagers,
    int slots) {
  public FlinkSnapshot withJobStatus(String value) { return new FlinkSnapshot(value, backpressureRatio, checkpointDurationMs, consecutiveCheckpointFailures, recordsInPerSec, recordsOutPerSec, taskManagers, slots); }
  public FlinkSnapshot withBackpressureRatio(double value) { return new FlinkSnapshot(jobStatus, value, checkpointDurationMs, consecutiveCheckpointFailures, recordsInPerSec, recordsOutPerSec, taskManagers, slots); }
}

public record FdbMetricsSnapshot(
    long sourceDelayP95Ms,
    long kpi1mP95Ms,
    long kpi5mP95Ms,
    long sinkP95Ms,
    long sinkFailures,
    long watermarkLagMs) {
  public FdbMetricsSnapshot withKpi1mP95Ms(long value) { return new FdbMetricsSnapshot(sourceDelayP95Ms, value, kpi5mP95Ms, sinkP95Ms, sinkFailures, watermarkLagMs); }
  public FdbMetricsSnapshot withSinkP95Ms(long value) { return new FdbMetricsSnapshot(sourceDelayP95Ms, kpi1mP95Ms, kpi5mP95Ms, value, sinkFailures, watermarkLagMs); }
}

public record StorageSnapshot(boolean healthy, String summary, long records, long smallFiles, long inProgressFiles) {
}

public record RunObservation(FlinkSnapshot flink, FdbMetricsSnapshot fdb, StorageSnapshot storage) {
  public RunObservation withFlink(FlinkSnapshot value) { return new RunObservation(value, fdb, storage); }
  public RunObservation withFdb(FdbMetricsSnapshot value) { return new RunObservation(flink, value, storage); }
  public RunObservation withStorage(StorageSnapshot value) { return new RunObservation(flink, fdb, value); }
}

public record BenchmarkRunResult(
    BenchmarkRunPlan plan,
    BenchmarkStatus status,
    String bottleneckReason,
    FlinkSnapshot flink,
    FdbMetricsSnapshot fdb,
    StorageSnapshot storage) {
}
```

Create `BenchmarkDecisionEngine`:

```java
public final class BenchmarkDecisionEngine {
  private final BenchmarkThresholds thresholds;

  public BenchmarkDecisionEngine(BenchmarkThresholds thresholds) {
    this.thresholds = thresholds;
  }

  public BenchmarkRunResult decide(BenchmarkRunPlan plan, RunObservation observation) {
    FlinkSnapshot flink = observation.flink();
    FdbMetricsSnapshot fdb = observation.fdb();
    StorageSnapshot storage = observation.storage();

    if (!"RUNNING".equalsIgnoreCase(flink.jobStatus())) {
      return result(plan, BenchmarkStatus.FAILED, "Flink job status " + flink.jobStatus(), observation);
    }
    if (flink.backpressureRatio() > thresholds.maxBackpressureRatio()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "sustained backpressure ratio " + flink.backpressureRatio(), observation);
    }
    if (flink.consecutiveCheckpointFailures() >= thresholds.maxConsecutiveCheckpointFailures()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "checkpoint failures " + flink.consecutiveCheckpointFailures(), observation);
    }
    if (flink.checkpointDurationMs() > thresholds.maxCheckpointDurationMs()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "checkpoint duration " + flink.checkpointDurationMs() + " ms", observation);
    }
    if (Math.max(fdb.kpi1mP95Ms(), fdb.kpi5mP95Ms()) > thresholds.maxKpiAvailabilityP95Ms()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "KPI availability p95 over threshold", observation);
    }
    if (fdb.sinkP95Ms() > thresholds.maxSinkP95Ms()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "sink p95 over threshold", observation);
    }
    if (fdb.sinkFailures() > 0) {
      return result(plan, BenchmarkStatus.UNSTABLE, "sink failures " + fdb.sinkFailures(), observation);
    }
    if (fdb.watermarkLagMs() > thresholds.maxWatermarkLagMs()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "watermark lag " + fdb.watermarkLagMs() + " ms", observation);
    }
    if (!storage.healthy()) {
      return result(plan, BenchmarkStatus.UNSTABLE, "storage probe unhealthy: " + storage.summary(), observation);
    }
    return result(plan, BenchmarkStatus.STABLE, "all thresholds healthy", observation);
  }

  private static BenchmarkRunResult result(BenchmarkRunPlan plan, BenchmarkStatus status, String reason, RunObservation observation) {
    return new BenchmarkRunResult(plan, status, reason, observation.flink(), observation.fdb(), observation.storage());
  }
}
```

- [ ] **Step 4: Run decision tests**

Run:

```bash
mvn -pl benchmark-runner -Dtest=BenchmarkDecisionEngineTest test
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add benchmark-runner/src/main/java/com/fdb/benchmark benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkDecisionEngineTest.java
git commit -m "feat(benchmark): add run decision engine"
```

---

### Task 4: Flink REST And Observability Clients

**Files:**
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/HttpGateway.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/JdkHttpGateway.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/FlinkRestClient.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/ObservabilityClient.java`
- Test: `benchmark-runner/src/test/java/com/fdb/benchmark/FlinkRestClientTest.java`
- Test: `benchmark-runner/src/test/java/com/fdb/benchmark/ObservabilityClientTest.java`

- [ ] **Step 1: Write client tests**

Create `FlinkRestClientTest.java`:

```java
package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FlinkRestClientTest {
  @Test
  void reads_running_job_checkpoint_and_metrics_summary() {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/jobs/overview", "{\"jobs\":[{\"jid\":\"job-a\",\"state\":\"RUNNING\"}]}",
        "/jobs/job-a/checkpoints", "{\"latest\":{\"completed\":{\"duration\":42000}},\"counts\":{\"failed\":1}}",
        "/jobs/job-a/vertices", "{\"vertices\":[{\"id\":\"v1\",\"name\":\"sink\",\"metrics\":{\"read-records\":1000,\"write-records\":950}}]}"));

    FlinkSnapshot snapshot = new FlinkRestClient(URI.create("http://flink:8081"), http).snapshot();

    assertThat(snapshot.jobStatus()).isEqualTo("RUNNING");
    assertThat(snapshot.checkpointDurationMs()).isEqualTo(42_000);
    assertThat(snapshot.recordsInPerSec()).isEqualTo(1000);
    assertThat(snapshot.recordsOutPerSec()).isEqualTo(950);
  }
}
```

Create `ObservabilityClientTest.java`:

```java
package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ObservabilityClientTest {
  @Test
  void reads_stage_and_sink_latency_summaries() {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/api/flow/stages", "[{\"stageId\":\"kpi-1m\",\"latencyP95Ms\":70000,\"watermarkLagMs\":12000}]",
        "/api/results/sink-latency", "[{\"sinkName\":\"starrocks-kpi-1m\",\"records\":100,\"latencyP95Ms\":90000,\"failureCount\":0}]"));

    FdbMetricsSnapshot snapshot = new ObservabilityClient(URI.create("http://api:18080"), http).snapshot();

    assertThat(snapshot.kpi1mP95Ms()).isEqualTo(70_000);
    assertThat(snapshot.sinkP95Ms()).isEqualTo(90_000);
    assertThat(snapshot.sinkFailures()).isZero();
    assertThat(snapshot.watermarkLagMs()).isEqualTo(12_000);
  }
}
```

Add package-private fake in test source:

```java
package com.fdb.benchmark;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

final class FakeHttpGateway implements HttpGateway {
  private final Map<String, String> responses;

  FakeHttpGateway(Map<String, String> responses) {
    this.responses = responses;
  }

  @Override
  public String get(URI uri) throws IOException {
    String body = responses.get(uri.getPath());
    if (body == null) {
      throw new IOException("missing fake response for " + uri.getPath());
    }
    return body;
  }
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
mvn -pl benchmark-runner -Dtest=FlinkRestClientTest,ObservabilityClientTest test
```

Expected: FAIL because clients do not exist.

- [ ] **Step 3: Implement HTTP gateway and clients**

Create `HttpGateway`:

```java
package com.fdb.benchmark;

import java.io.IOException;
import java.net.URI;

public interface HttpGateway {
  String get(URI uri) throws IOException, InterruptedException;
}
```

Create `JdkHttpGateway` using Java `HttpClient`.

Implement `FlinkRestClient.snapshot()` with defensive parsing:

- Query `/jobs/overview`; pick the first non-terminal job, otherwise first job.
- Query `/jobs/{jobId}/checkpoints`.
- Query `/jobs/{jobId}/vertices`.
- Return healthy zero defaults if a secondary endpoint is missing.

Implement `ObservabilityClient.snapshot()`:

- Query `/api/flow/stages`.
- Query `/api/results/sink-latency`.
- Use max p95 across matching KPI/sink entries.
- Sum sink failure counts.
- Use max watermark lag.

- [ ] **Step 4: Run client tests**

Run:

```bash
mvn -pl benchmark-runner -Dtest=FlinkRestClientTest,ObservabilityClientTest test
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add benchmark-runner/src/main/java/com/fdb/benchmark benchmark-runner/src/test/java/com/fdb/benchmark
git commit -m "feat(benchmark): read flink and observability metrics"
```

---

### Task 5: Storage Probes

**Files:**
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/StorageProbe.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/NoopStorageProbe.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/KafkaStorageProbe.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/StarRocksStorageProbe.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/HiveStorageProbe.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/IcebergStorageProbe.java`
- Test: `benchmark-runner/src/test/java/com/fdb/benchmark/StorageProbeTest.java`

- [ ] **Step 1: Write probe factory tests**

Create `StorageProbeTest.java`:

```java
package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StorageProbeTest {
  @Test
  void none_sink_uses_noop_probe() throws Exception {
    StorageProbe probe = StorageProbe.forSink(BenchmarkSink.NONE, command -> new CommandResult(0, "", ""));

    assertThat(probe.snapshot().healthy()).isTrue();
    assertThat(probe.snapshot().summary()).contains("no business sink");
  }

  @Test
  void starrocks_probe_marks_command_failure_unhealthy() throws Exception {
    StorageProbe probe = StorageProbe.forSink(BenchmarkSink.STARROCKS,
        command -> new CommandResult(1, "", "mysql unavailable"));

    StorageSnapshot snapshot = probe.snapshot();

    assertThat(snapshot.healthy()).isFalse();
    assertThat(snapshot.summary()).contains("mysql unavailable");
  }
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
mvn -pl benchmark-runner -Dtest=StorageProbeTest test
```

Expected: FAIL because probe classes do not exist.

- [ ] **Step 3: Implement command result and probes**

Create:

```java
public record CommandResult(int exitCode, String stdout, String stderr) {
  public boolean success() { return exitCode == 0; }
}

@FunctionalInterface
public interface CommandRunner {
  CommandResult run(List<String> command) throws IOException, InterruptedException;
}

public interface StorageProbe {
  StorageSnapshot snapshot() throws Exception;

  static StorageProbe forSink(BenchmarkSink sink, CommandRunner commandRunner) {
    return switch (sink) {
      case NONE -> new NoopStorageProbe();
      case KAFKA -> new KafkaStorageProbe(commandRunner);
      case STARROCKS -> new StarRocksStorageProbe(commandRunner);
      case HIVE -> new HiveStorageProbe(commandRunner);
      case ICEBERG -> new IcebergStorageProbe(commandRunner);
    };
  }
}
```

First-version probe behavior:

- `NoopStorageProbe`: healthy summary `no business sink`.
- `KafkaStorageProbe`: runs a Kafka offset command through `CommandRunner`; unhealthy on non-zero exit.
- `StarRocksStorageProbe`: runs `scripts/deploy.sh <target> status` or a focused mysql command through `CommandRunner`; unhealthy on non-zero exit.
- `HiveStorageProbe`: runs an HDFS listing/count command through `CommandRunner`; unhealthy on non-zero exit or in-progress count above zero.
- `IcebergStorageProbe`: runs an HDFS listing/count command for the Iceberg warehouse through `CommandRunner`; unhealthy on non-zero exit or in-progress count above zero.

Keep command construction centralized in each probe so orchestrator only depends on `StorageProbe`.

- [ ] **Step 4: Run probe tests**

Run:

```bash
mvn -pl benchmark-runner -Dtest=StorageProbeTest test
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add benchmark-runner/src/main/java/com/fdb/benchmark benchmark-runner/src/test/java/com/fdb/benchmark/StorageProbeTest.java
git commit -m "feat(benchmark): add storage probes"
```

---

### Task 6: Orchestrator With Fake Clients

**Files:**
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkOrchestrator.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/DeployCommandClient.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/SimulatorProcessManager.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkClients.java`
- Test: `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkOrchestratorTest.java`

- [ ] **Step 1: Write orchestration tests**

Create `BenchmarkOrchestratorTest.java`:

```java
package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BenchmarkOrchestratorTest {
  @Test
  void stops_higher_levels_after_first_unstable_run_for_a_sink() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-a",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000 3000 9000",
        "FDB_BENCHMARK_CHR_EPS_PER_CELL", "0.1",
        "FDB_BENCHMARK_WARMUP_SEC", "0",
        "FDB_BENCHMARK_DURATION_SEC", "0"));
    RecordingDeploy deploy = new RecordingDeploy();
    RecordingSimulators simulators = new RecordingSimulators();
    FakeObservationSource observations = new FakeObservationSource(List.of(
        healthy(),
        healthy().withFlink(healthy().flink().withBackpressureRatio(0.5))));

    List<BenchmarkRunResult> results = new BenchmarkOrchestrator(
        config,
        deploy,
        simulators,
        observations,
        new BenchmarkDecisionEngine(config.thresholds())).run();

    assertThat(results).hasSize(2);
    assertThat(results).extracting(result -> result.plan().cellLevel()).containsExactly(1000, 3000);
    assertThat(results).extracting(BenchmarkRunResult::status).containsExactly(BenchmarkStatus.STABLE, BenchmarkStatus.UNSTABLE);
    assertThat(deploy.actions).containsExactly("submit:bench-a-none-cells1000-eps100", "stop:bench-a-none-cells1000-eps100",
        "submit:bench-a-none-cells3000-eps300", "stop:bench-a-none-cells3000-eps300");
    assertThat(simulators.actions).contains("start:1000:100", "start:3000:300", "stop", "stop");
  }

  private static RunObservation healthy() {
    return new RunObservation(
        new FlinkSnapshot("RUNNING", 0.0, 20_000, 0, 1000, 1000, 1, 4),
        new FdbMetricsSnapshot(1000, 2000, 3000, 4000, 0, 1000),
        new StorageSnapshot(true, "healthy", 100, 0, 0));
  }

  static final class RecordingDeploy implements DeployCommandClient {
    final List<String> actions = new ArrayList<>();
    @Override public void submit(BenchmarkRunPlan plan) { actions.add("submit:" + plan.runId()); }
    @Override public void stop(BenchmarkRunPlan plan) { actions.add("stop:" + plan.runId()); }
  }

  static final class RecordingSimulators implements SimulatorProcessManager {
    final List<String> actions = new ArrayList<>();
    @Override public void start(BenchmarkRunPlan plan) { actions.add("start:" + plan.cellLevel() + ":" + plan.targetChrEps()); }
    @Override public void stop() { actions.add("stop"); }
  }

  static final class FakeObservationSource implements BenchmarkClients {
    private final List<RunObservation> observations;
    private int index;
    FakeObservationSource(List<RunObservation> observations) { this.observations = observations; }
    @Override public RunObservation observe(BenchmarkRunPlan plan) { return observations.get(index++); }
  }
}
```

- [ ] **Step 2: Run orchestration tests and verify failure**

Run:

```bash
mvn -pl benchmark-runner -Dtest=BenchmarkOrchestratorTest test
```

Expected: FAIL because orchestrator interfaces do not exist.

- [ ] **Step 3: Implement orchestrator interfaces**

Create interfaces:

```java
public interface DeployCommandClient {
  void submit(BenchmarkRunPlan plan) throws Exception;
  void stop(BenchmarkRunPlan plan) throws Exception;
}

public interface SimulatorProcessManager {
  void start(BenchmarkRunPlan plan) throws Exception;
  void stop() throws Exception;
}

public interface BenchmarkClients {
  RunObservation observe(BenchmarkRunPlan plan) throws Exception;
}
```

Create `BenchmarkOrchestrator.run()`:

```java
public List<BenchmarkRunResult> run() throws Exception {
  List<BenchmarkRunResult> results = new ArrayList<>();
  BenchmarkSink currentSink = null;
  boolean skipCurrentSink = false;

  for (BenchmarkRunPlan plan : BenchmarkMatrix.expand(config)) {
    if (currentSink != plan.sink()) {
      currentSink = plan.sink();
      skipCurrentSink = false;
    }
    if (skipCurrentSink) {
      continue;
    }
    try {
      simulators.start(plan);
      deploy.submit(plan);
      RunObservation observation = clients.observe(plan);
      BenchmarkRunResult result = decisionEngine.decide(plan, observation);
      results.add(result);
      if (result.status() != BenchmarkStatus.STABLE) {
        skipCurrentSink = true;
      }
    } finally {
      try {
        deploy.stop(plan);
      } finally {
        simulators.stop();
      }
    }
  }
  return results;
}
```

- [ ] **Step 4: Run orchestration tests**

Run:

```bash
mvn -pl benchmark-runner -Dtest=BenchmarkOrchestratorTest test
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add benchmark-runner/src/main/java/com/fdb/benchmark benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkOrchestratorTest.java
git commit -m "feat(benchmark): orchestrate sink escalation"
```

---

### Task 7: JSON, CSV, And HTML Writers

**Files:**
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkResultWriter.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/HtmlReportWriter.java`
- Test: `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkResultWriterTest.java`
- Test: `benchmark-runner/src/test/java/com/fdb/benchmark/HtmlReportWriterTest.java`

- [ ] **Step 1: Write writer tests**

Create `BenchmarkResultWriterTest.java`:

```java
package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BenchmarkResultWriterTest {
  @TempDir Path tempDir;

  @Test
  void writes_config_results_and_csv() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-a",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000"));
    BenchmarkRunResult result = sampleResult(config);

    new BenchmarkResultWriter(tempDir).write(config, List.of(result));

    assertThat(tempDir.resolve("bench-a/benchmark-config.json")).exists();
    assertThat(tempDir.resolve("bench-a/benchmark-results.json")).exists();
    String csv = Files.readString(tempDir.resolve("bench-a/benchmark-summary.csv"));
    assertThat(csv).contains("sink,cellLevel,targetChrEps,status");
    assertThat(csv).contains("none,1000,300,STABLE");
  }

  static BenchmarkRunResult sampleResult(BenchmarkConfig config) {
    BenchmarkRunPlan plan = BenchmarkMatrix.expand(config).get(0);
    return new BenchmarkRunResult(plan, BenchmarkStatus.STABLE, "all thresholds healthy",
        new FlinkSnapshot("RUNNING", 0, 10_000, 0, 300, 300, 1, 4),
        new FdbMetricsSnapshot(1, 2, 3, 4, 0, 5),
        new StorageSnapshot(true, "healthy", 10, 0, 0));
  }
}
```

Create `HtmlReportWriterTest.java`:

```java
package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HtmlReportWriterTest {
  @TempDir Path tempDir;

  @Test
  void writes_index_and_run_report_with_links_and_recommendations() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-a",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000"));
    BenchmarkRunResult result = BenchmarkResultWriterTest.sampleResult(config);

    new HtmlReportWriter(tempDir).write(config, List.of(result));

    Path index = tempDir.resolve("bench-a/index.html");
    Path report = tempDir.resolve("bench-a/runs/" + result.plan().runId() + "/report.html");
    assertThat(index).exists();
    assertThat(report).exists();
    String html = Files.readString(index);
    assertThat(html).contains("Sink Upper-Bound Benchmark");
    assertThat(html).contains("Stable upper bounds");
    assertThat(html).contains("runs/" + result.plan().runId() + "/report.html");
    assertThat(html).contains("Recommendations");
    assertThat(Files.readString(report)).contains("Flink Snapshot").contains("Storage Snapshot");
  }
}
```

- [ ] **Step 2: Run writer tests and verify failure**

Run:

```bash
mvn -pl benchmark-runner -Dtest=BenchmarkResultWriterTest,HtmlReportWriterTest test
```

Expected: FAIL because writer classes do not exist.

- [ ] **Step 3: Implement writers**

`BenchmarkResultWriter`:

- Create `<outputRoot>/<benchmarkId>/`.
- Write `benchmark-config.json` with Jackson pretty printer.
- Write `benchmark-results.json` with Jackson pretty printer.
- Write `benchmark-summary.csv` with header:

```text
sink,cellLevel,targetChrEps,status,recordsInPerSec,recordsOutPerSec,kpi1mP95Ms,kpi5mP95Ms,sinkP95Ms,checkpointDurationMs,backpressureRatio,bottleneckReason,runId
```

`HtmlReportWriter`:

- Write one static `index.html`.
- Write `runs/<runId>/report.html` per result.
- Use inline CSS only.
- Use relative links only.
- Escape dynamic HTML values with a small helper that replaces `&`, `<`, `>`, `"`, and `'`.
- Add status classes `stable`, `unstable`, `failed`.

- [ ] **Step 4: Run writer tests**

Run:

```bash
mvn -pl benchmark-runner -Dtest=BenchmarkResultWriterTest,HtmlReportWriterTest test
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add benchmark-runner/src/main/java/com/fdb/benchmark benchmark-runner/src/test/java/com/fdb/benchmark
git commit -m "feat(benchmark): write html and result artifacts"
```

---

### Task 8: Real Command Clients And Main Wiring

**Files:**
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkRunnerMain.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/ProcessCommandRunner.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/ShellDeployCommandClient.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/JavaSimulatorProcessManager.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/DefaultBenchmarkClients.java`
- Test: `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkRunnerMainTest.java`

- [ ] **Step 1: Write main dry-run test**

Create `BenchmarkRunnerMainTest.java`:

```java
package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BenchmarkRunnerMainTest {
  @TempDir Path tempDir;

  @Test
  void dry_run_writes_artifacts_without_starting_external_processes() throws Exception {
    Path env = tempDir.resolve(".env");
    Files.writeString(env, """
        FDB_BENCHMARK_ID=bench-dry
        FDB_BENCHMARK_SINKS=none
        FDB_BENCHMARK_CELL_LEVELS=1000
        FDB_BENCHMARK_CHR_EPS_PER_CELL=0.1
        """);

    int exit = BenchmarkRunnerMain.run(new String[] {"local", "--env", env.toString(), "--dry-run"});

    assertThat(exit).isZero();
    assertThat(Path.of("benchmark-runner/output/benchmark-runs/bench-dry/index.html")).exists();
  }
}
```

- [ ] **Step 2: Run main test and verify failure**

Run:

```bash
mvn -pl benchmark-runner -Dtest=BenchmarkRunnerMainTest test
```

Expected: FAIL because `BenchmarkRunnerMain.run` and dry-run wiring are not implemented.

- [ ] **Step 3: Implement main wiring**

Update `BenchmarkRunnerMain`:

```java
public static void main(String[] args) {
  System.exit(run(args));
}

static int run(String[] args) {
  try {
    ParsedArgs parsed = ParsedArgs.parse(args);
    Map<String, String> env = EnvFile.load(parsed.envFile(), System.getenv());
    BenchmarkConfig config = BenchmarkConfig.from(parsed.target(), env);
    List<BenchmarkRunResult> results;
    if (parsed.dryRun()) {
      results = BenchmarkMatrix.expand(config).stream()
          .map(plan -> new BenchmarkDecisionEngine(config.thresholds()).decide(plan, dryRunObservation()))
          .toList();
    } else {
      results = defaultOrchestrator(config).run();
    }
    new BenchmarkResultWriter(config.outputRoot()).write(config, results);
    new HtmlReportWriter(config.outputRoot()).write(config, results);
    return 0;
  } catch (Exception e) {
    System.err.println("[ERROR] " + e.getMessage());
    return 1;
  }
}
```

Create `EnvFile.load(Path, Map<String, String>)` as package-private helper:

- Read non-empty lines not starting with `#`.
- Parse `KEY=value`.
- Keep existing inherited env values unless env file has the key.
- Do not execute shell syntax.
- Strip one pair of matching single or double quotes.

Implement real clients:

- `ProcessCommandRunner`: uses `ProcessBuilder`, captures stdout/stderr, returns `CommandResult`.
- `ShellDeployCommandClient`: calls `bash scripts/deploy.sh <target> submit` and `bash scripts/deploy.sh <target> stop` with per-run env:
  - `FDB_RUN_ID`
  - `FDB_RUN_LABEL`
  - `FDB_RESULT_SINK`
  - `FDB_SITES_COUNT`
  - `FDB_RATE_EPS`
- `JavaSimulatorProcessManager`: starts:
  - `java -jar topology-service/target/topology-service-0.1.0-SNAPSHOT.jar`
  - `java -jar simulator/target/simulator-0.1.0-SNAPSHOT.jar cfg`
  - `java -jar simulator/target/simulator-0.1.0-SNAPSHOT.jar pm`
  - `java -jar simulator/target/simulator-0.1.0-SNAPSHOT.jar chr`
  with `FDB_SITES_COUNT`, `FDB_RATE_EPS`, `FDB_KAFKA_BOOTSTRAP`, and `FDB_E2E_SUMMARY=1`.
- `DefaultBenchmarkClients`: composes `FlinkRestClient`, `ObservabilityClient`, and `StorageProbe`.

- [ ] **Step 4: Run main test**

Run:

```bash
mvn -pl benchmark-runner -Dtest=BenchmarkRunnerMainTest test
```

Expected: PASS.

- [ ] **Step 5: Run runner package**

Run:

```bash
mvn -pl benchmark-runner -am package
bash scripts/benchmark.sh local --dry-run
```

Expected: PASS and writes `benchmark-runner/output/benchmark-runs/<benchmarkId>/index.html`.

- [ ] **Step 6: Commit**

Run:

```bash
git add benchmark-runner/src/main/java/com/fdb/benchmark benchmark-runner/src/test/java/com/fdb/benchmark
git commit -m "feat(benchmark): wire runner main flow"
```

---

### Task 9: Documentation And Environment Examples

**Files:**
- Modify: `README.md`
- Modify: `.env.example.local`
- Modify: `.env.example.external-yarn`
- Modify: `docs/superpowers/specs/2026-04-29-flink-data-balance-design.md` if implementation details differ from the approved design.

- [ ] **Step 1: Update README benchmark section**

Replace old `deploy.sh benchmark` examples with:

```markdown
Benchmark runner:

```bash
mvn -pl benchmark-runner -am package
FDB_ENV_FILE=.env.local bash scripts/benchmark.sh local
FDB_ENV_FILE=.env.external bash scripts/benchmark.sh external-yarn
```

The runner writes static HTML and machine-readable artifacts under:

```text
benchmark-runner/output/benchmark-runs/<benchmarkId>/
```

Open `index.html` for the batch summary and follow per-run links to
`runs/<runId>/report.html`.
```

- [ ] **Step 2: Update env examples**

In both `.env.example.local` and `.env.example.external-yarn`, remove these old keys if present:

```dotenv
FDB_BENCHMARK_OUTPUT_DIR=
FDB_BENCHMARK_REPORT_ROOT=
FDB_BENCHMARK_STOP_ON_FINISH=
FDB_BENCHMARK_CONTINUE_ON_ERROR=
```

Add these keys:

```dotenv
FDB_BENCHMARK_SINKS=none starrocks kafka hive iceberg
FDB_BENCHMARK_CELL_LEVELS=10000 20000 40000
FDB_BENCHMARK_CHR_EPS_PER_CELL=0.3
FDB_BENCHMARK_WARMUP_SEC=60
FDB_BENCHMARK_DURATION_SEC=300
FDB_BENCHMARK_POLL_INTERVAL_SEC=10
FDB_BENCHMARK_MAX_BACKPRESSURE_RATIO=0.2
FDB_BENCHMARK_MAX_CHECKPOINT_DURATION_MS=120000
FDB_BENCHMARK_MAX_CONSECUTIVE_CHECKPOINT_FAILURES=2
FDB_BENCHMARK_MAX_KPI_AVAILABILITY_P95_MS=180000
FDB_BENCHMARK_MAX_SINK_P95_MS=180000
FDB_BENCHMARK_MAX_WATERMARK_LAG_MS=180000
```

- [ ] **Step 3: Run docs-related checks**

Run:

```bash
bash scripts/test-benchmark-dispatch.sh
mvn -pl benchmark-runner test
```

Expected: PASS.

- [ ] **Step 4: Commit**

Run:

```bash
git add README.md .env.example.local .env.example.external-yarn docs/superpowers/specs/2026-04-29-flink-data-balance-design.md
git commit -m "docs(benchmark): document java runner workflow"
```

---

### Task 10: Final Verification

**Files:**
- No planned source changes unless verification finds a defect.

- [ ] **Step 1: Run Java tests for the new module**

Run:

```bash
mvn -pl benchmark-runner test
```

Expected: PASS.

- [ ] **Step 2: Run launcher test**

Run:

```bash
bash scripts/test-benchmark-dispatch.sh
```

Expected: PASS.

- [ ] **Step 3: Run existing deployment dispatch test**

Run:

```bash
bash scripts/test-deploy-dispatch.sh
```

Expected: PASS.

- [ ] **Step 4: Run full Maven tests if time allows**

Run:

```bash
mvn test
```

Expected: PASS.

- [ ] **Step 5: Run frontend tests because README/Spec references frontend report status**

Run:

```bash
npm --prefix frontend test
```

Expected: PASS.

- [ ] **Step 6: Run dry-run package flow**

Run:

```bash
mvn -pl benchmark-runner -am package
FDB_ENV_FILE=.env.local bash scripts/benchmark.sh local --dry-run
```

Expected: PASS and `benchmark-runner/output/benchmark-runs/<benchmarkId>/index.html` exists.

- [ ] **Step 7: Run GitNexus detect changes before final commit**

Run:

```bash
npx gitnexus detect_changes --repo flink-data-balance --scope unstaged
```

Expected: Review any HIGH or CRITICAL results before committing.

- [ ] **Step 8: Commit verification fixes**

If verification required fixes, commit them:

```bash
git add benchmark-runner scripts README.md .env.example.local .env.example.external-yarn docs/superpowers/specs/2026-04-29-flink-data-balance-design.md
git commit -m "test(benchmark): verify runner workflow"
```

If no files changed, omit this commit.

---

## Self-Review

- Spec coverage: The plan covers `scripts/benchmark.sh`, Java `benchmark-runner`, fixed output path, `sink x cellLevel`, `targetChrEps`, Flink REST, Observability API, storage probes, stable/unstable/failed, HTML-only reports, JSON/CSV artifacts, and Java plus Bash tests.
- Placeholder scan: No unresolved placeholder markers or open-ended "add tests" steps remain. Every task has concrete files, commands, and expected outcomes.
- Type consistency: `BenchmarkConfig`, `BenchmarkMatrix`, `BenchmarkRunPlan`, `BenchmarkRunResult`, `BenchmarkDecisionEngine`, `FlinkSnapshot`, `FdbMetricsSnapshot`, `StorageSnapshot`, `BenchmarkResultWriter`, and `HtmlReportWriter` names are consistent across tasks.
- Scope check: Parallelism and checkpoint matrices are intentionally excluded from first-version implementation and only recorded from environment/config.
