# Benchmark Pressure Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 按已确认 Spec 修正 benchmark 压力模型、source 节奏、异常注入、稳定性判定和 HTML 报告展示。

**Architecture:** `benchmark-runner` 负责压测配置、矩阵、运行观测、稳定性判定和报告；`topology-service` 负责按目标小区数生成拓扑；`simulator` 负责严格按全局 CHR EPS 生产、输出 source 指标并按 5% cohort 注入异常；`flink-job` 只修正 metrics 空样本语义，不改变业务检测规则。

**Tech Stack:** Java 17、Maven、JUnit 5、AssertJ、Flink REST API、Kafka、现有 JSON/HTML 报告生成器、GitNexus。

---

## Current Context

当前工作区已有未提交实现改动。执行本计划时，每个任务只暂存该任务列出的文件，提交前运行 `git diff --cached --name-only` 确认范围。

GitNexus 预分析结果：

- `ChrSimulator` upstream risk `LOW`，直接影响 `SimulatorMain.main`。
- `PmSimulator` upstream risk `LOW`，直接影响 `SimulatorMain.main`。
- `TopologyGenerator` upstream risk `MEDIUM`，影响 `TopologyMain.main` 和现有 `TopologyGeneratorTest`。
- `FlinkRestClient` upstream risk `LOW`，影响 `DefaultBenchmarkClients` 和 `FlinkRestClientTest`。
- `BenchmarkDecisionEngine` upstream risk `MEDIUM`，影响 `BenchmarkRunnerMain`、`BenchmarkOrchestrator` 和相关测试。
- `BenchmarkConfig` 在当前索引中未命中；执行阶段仍需对改动后的 staged 范围跑 `mcp__gitnexus.detect_changes`。

## File Structure

- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkConfig.java`
  - 移除 `topologyCellsPerSite`，把 `cellLevel` 定义为目标小区数。
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkThresholds.java`
  - 增加 producer delivery 和 source backlog 阈值。
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/SourceMetricsSnapshot.java`
  - 读取 CHR/PM/CFG simulator metrics 文件，提供 source density 和 producer delivery ratio。
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/RunObservation.java`
  - 携带 source metrics。
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkRunResult.java`
  - 报告结果携带 source metrics。
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/DefaultBenchmarkClients.java`
  - 从 run 目录读取 source metrics。
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkDecisionEngine.java`
  - producer under-delivery 和 source backlog 进入 `unstable` 判定。
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/FlinkOperatorSnapshot.java`
  - 增加 operator 级 pending records。
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/FlinkSnapshot.java`
  - 增加 job/source backlog 聚合值。
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/FlinkRestClient.java`
  - 从 vertex metrics 读取 `pendingRecords`。
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/JavaSimulatorProcessManager.java`
  - 传递目标小区数、source metrics 文件路径和异常比例。
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/HtmlReportWriter.java`
  - 删除旧 `Estimated Cells/Site`，增加 source density、delivery ratio、backlog、N/A latency 和 checkpoint cadence 展示。
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkResultWriter.java`
  - 写出 source metrics snapshot。
- Modify: `topology-service/src/main/java/com/fdb/topology/TopologyConfig.java`
  - 增加 `sites.targetCells`。
- Modify: `topology-service/src/main/java/com/fdb/topology/TopologyMain.java`
  - 解析 `FDB_TOPOLOGY_TARGET_CELLS` 映射出的 `topology.target.cells`。
- Modify: `topology-service/src/main/java/com/fdb/topology/TopologyGenerator.java`
  - 生成达到目标小区数即停止。
- Modify: `simulator/src/main/java/com/fdb/simulator/ChrSimulator.java`
  - 严格 EPS pacing、source metrics、5% user/cell 异常 cohort。
- Modify: `simulator/src/main/java/com/fdb/simulator/PmSimulator.java`
  - source metrics、5% cell 异常 cohort。
- Modify: `simulator/src/main/java/com/fdb/simulator/CfgSimulator.java`
  - source metrics。
- Create: `simulator/src/main/java/com/fdb/simulator/SourceMetricsWriter.java`
  - simulator 侧轻量 JSON 指标文件写入。
- Modify: `flink-job/src/main/java/com/fdb/job/metrics/LatencyStats.java`
  - 无样本返回 `-1`，让报告展示 `N/A`。
- Modify tests under:
  - `benchmark-runner/src/test/java/com/fdb/benchmark/`
  - `topology-service/src/test/java/com/fdb/topology/`
  - `simulator/src/test/java/com/fdb/simulator/`
  - `flink-job/src/test/java/com/fdb/job/metrics/`
- Modify docs:
  - `README.md`
  - `docs/superpowers/specs/2026-04-29-flink-data-balance-design.md`

---

### Task 0: Pre-Change Guardrails

**Files:**
- Read only: Java files listed in File Structure

- [ ] **Step 1: Re-run GitNexus impact before Java edits**

Use these tool calls before changing Java code:

```json
{"repo":"flink-data-balance","target":"ChrSimulator","file_path":"simulator/src/main/java/com/fdb/simulator/ChrSimulator.java","direction":"upstream","maxDepth":2,"includeTests":true,"relationTypes":["CALLS","IMPORTS","HAS_METHOD","HAS_PROPERTY","ACCESSES"]}
{"repo":"flink-data-balance","target":"TopologyGenerator","file_path":"topology-service/src/main/java/com/fdb/topology/TopologyGenerator.java","direction":"upstream","maxDepth":2,"includeTests":true,"relationTypes":["CALLS","IMPORTS","HAS_METHOD","HAS_PROPERTY","ACCESSES"]}
{"repo":"flink-data-balance","target":"FlinkRestClient","file_path":"benchmark-runner/src/main/java/com/fdb/benchmark/FlinkRestClient.java","direction":"upstream","maxDepth":2,"includeTests":true,"relationTypes":["CALLS","IMPORTS","HAS_METHOD","HAS_PROPERTY","ACCESSES"]}
{"repo":"flink-data-balance","target":"BenchmarkDecisionEngine","file_path":"benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkDecisionEngine.java","direction":"upstream","maxDepth":2,"includeTests":true,"relationTypes":["CALLS","IMPORTS","HAS_METHOD","HAS_PROPERTY","ACCESSES"]}
```

Expected: no `HIGH` or `CRITICAL` risk. If `HIGH` or `CRITICAL` appears, inspect d=1 affected symbols and add a focused test for each direct caller before editing.

- [ ] **Step 2: Snapshot current dirty worktree**

Run:

```powershell
git status --short
```

Expected: existing dirty files may remain. This plan must not reset or revert them.

---

### Task 1: Benchmark Config And Threshold Semantics

**Files:**
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkConfig.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkThresholds.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/HtmlReportWriter.java`
- Modify: `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkConfigTest.java`
- Modify: `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkResultWriterTest.java`
- Modify: `benchmark-runner/src/test/java/com/fdb/benchmark/HtmlReportWriterTest.java`

- [ ] **Step 1: Write failing config tests**

Replace the old `BenchmarkConfigTest` EPS assertions with:

```java
assertThat(config.chrEpsPerCell()).isEqualTo(0.25);
assertThat(config.targetChrEps(3000)).isEqualTo(750);
assertThat(config.thresholds().minProducerDeliveryRatio()).isEqualTo(0.98);
assertThat(config.thresholds().maxSourceBacklogRecords()).isEqualTo(0L);
```

Replace the `allows_explicit_topology_cells_per_site_for_chr_eps_estimate` test with:

```java
@Test
void ignores_removed_topology_cells_per_site_for_chr_eps_estimate() {
  BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
      "FDB_BENCHMARK_CELL_LEVELS", "1000",
      "FDB_BENCHMARK_CHR_EPS_PER_CELL", "0.3",
      "FDB_BENCHMARK_TOPOLOGY_CELLS_PER_SITE", "4.5",
      "FDB_BENCHMARK_MIN_PRODUCER_DELIVERY_RATIO", "0.97",
      "FDB_BENCHMARK_MAX_SOURCE_BACKLOG_RECORDS", "123"));

  assertThat(config.targetChrEps(1000)).isEqualTo(300);
  assertThat(config.thresholds().minProducerDeliveryRatio()).isEqualTo(0.97);
  assertThat(config.thresholds().maxSourceBacklogRecords()).isEqualTo(123L);
}
```

- [ ] **Step 2: Run config tests and verify failure**

Run:

```powershell
mvn -pl benchmark-runner -Dtest=BenchmarkConfigTest test
```

Expected: FAIL because `targetChrEps` still multiplies by `topologyCellsPerSite` and thresholds do not expose the new fields.

- [ ] **Step 3: Update `BenchmarkConfig`**

Change the record header to remove `topologyCellsPerSite`:

```java
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
```

Remove this constructor argument:

```java
positiveDouble(env, "FDB_BENCHMARK_TOPOLOGY_CELLS_PER_SITE", 6.0),
```

Replace `targetChrEps` with:

```java
public long targetChrEps(int cellLevel) {
  return Math.round(cellLevel * chrEpsPerCell);
}
```

Remove the private `positiveDouble` method if it is no longer used.

- [ ] **Step 4: Update `BenchmarkThresholds`**

Change the record header and `from` factory:

```java
public record BenchmarkThresholds(
    double maxBackpressureRatio,
    long maxCheckpointDurationMs,
    int maxConsecutiveCheckpointFailures,
    long maxKpiAvailabilityP95Ms,
    long maxSinkP95Ms,
    long maxWatermarkLagMs,
    double minProducerDeliveryRatio,
    long maxSourceBacklogRecords) {

  public static BenchmarkThresholds from(Map<String, String> env) {
    return new BenchmarkThresholds(
        doubleValue(env, "FDB_BENCHMARK_MAX_BACKPRESSURE_RATIO", 0.2),
        longValue(env, "FDB_BENCHMARK_MAX_CHECKPOINT_DURATION_MS", 120_000),
        intValue(env, "FDB_BENCHMARK_MAX_CONSECUTIVE_CHECKPOINT_FAILURES", 2),
        longValue(env, "FDB_BENCHMARK_MAX_KPI_AVAILABILITY_P95_MS", 180_000),
        longValue(env, "FDB_BENCHMARK_MAX_SINK_P95_MS", 180_000),
        longValue(env, "FDB_BENCHMARK_MAX_WATERMARK_LAG_MS", 180_000),
        doubleValue(env, "FDB_BENCHMARK_MIN_PRODUCER_DELIVERY_RATIO", 0.98),
        longValue(env, "FDB_BENCHMARK_MAX_SOURCE_BACKLOG_RECORDS", 0));
  }
}
```

- [ ] **Step 5: Update existing test fixtures**

Update direct `new BenchmarkThresholds(...)` calls to include the new fields:

```java
new BenchmarkThresholds(0.2, 120_000, 2, 180_000, 180_000, 180_000, 0.98, 0)
```

Update benchmark result fixture expectations from the old EPS values:

```java
assertThat(csv).contains("none,1000,300,STABLE");
```

Remove this row from `HtmlReportWriter.runSummaryTable`:

```java
row("Estimated Cells/Site", String.valueOf(config.topologyCellsPerSite())),
```

- [ ] **Step 6: Run focused tests**

Run:

```powershell
mvn -pl benchmark-runner -Dtest=BenchmarkConfigTest,BenchmarkResultWriterTest,HtmlReportWriterTest test
```

Expected: PASS.

- [ ] **Step 7: Commit Task 1**

Run:

```powershell
git add benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkConfig.java `
  benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkThresholds.java `
  benchmark-runner/src/main/java/com/fdb/benchmark/HtmlReportWriter.java `
  benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkConfigTest.java `
  benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkResultWriterTest.java `
  benchmark-runner/src/test/java/com/fdb/benchmark/HtmlReportWriterTest.java
git diff --cached --name-only
git commit -m "fix: align benchmark pressure formula"
```

Expected staged files: exactly the six files above.

---

### Task 2: Topology Target Cell Generation

**Files:**
- Modify: `topology-service/src/main/java/com/fdb/topology/TopologyConfig.java`
- Modify: `topology-service/src/main/java/com/fdb/topology/TopologyMain.java`
- Modify: `topology-service/src/main/java/com/fdb/topology/TopologyGenerator.java`
- Modify: `topology-service/src/test/java/com/fdb/topology/TopologyGeneratorTest.java`
- Modify: `topology-service/src/test/java/com/fdb/topology/TopologyMainTest.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/JavaSimulatorProcessManager.java`
- Modify: `benchmark-runner/src/test/java/com/fdb/benchmark/JavaSimulatorProcessManagerTest.java`

- [ ] **Step 1: Write failing topology tests**

Add to `TopologyGeneratorTest`:

```java
@Test
void stops_after_target_cell_count_when_configured() {
    TopologyConfig config = new TopologyConfig();
    config.setSeed(42);
    config.getSites().setCount(100);
    config.getSites().setTargetCells(123);

    List<TopologyRecord> records = new TopologyGenerator(config).generate();

    assertThat(records).hasSize(123);
    assertThat(records.stream().map(TopologyRecord::getSiteId).distinct().count()).isLessThan(100);
}
```

Add to `TopologyMainTest`:

```java
@Test
void parses_target_cells_from_topology_env_style_key() {
    ConfigLoader.Config raw = ConfigLoader.builder()
        .defaultResource("topology-default.yaml")
        .envPrefix("FDB_")
        .envSource(Map.of("FDB_TOPOLOGY_TARGET_CELLS", "1234"))
        .build()
        .load();

    TopologyConfig config = TopologyMain.parseConfig(raw);

    assertThat(config.getSites().getTargetCells()).isEqualTo(1234);
}
```

Add to `JavaSimulatorProcessManagerTest`:

```java
assertThat(env).containsEntry("FDB_TOPOLOGY_TARGET_CELLS", "1000");
assertThat(env).containsEntry("FDB_SITES_COUNT", "1000");
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```powershell
mvn -pl topology-service,benchmark-runner -am -Dtest=TopologyGeneratorTest,TopologyMainTest,JavaSimulatorProcessManagerTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because `targetCells` and `FDB_TOPOLOGY_TARGET_CELLS` are not implemented.

- [ ] **Step 3: Add `targetCells` to topology config**

Add to `TopologyConfig.SitesConfig`:

```java
private int targetCells = 0;

public int getTargetCells() { return targetCells; }
public void setTargetCells(int targetCells) { this.targetCells = Math.max(0, targetCells); }
```

Add to `TopologyMain.parseConfig` after `sites.count`:

```java
int targetCells = raw.getInt("topology.target.cells",
    raw.getInt("sites.target.cells", config.getSites().getTargetCells()));
config.getSites().setTargetCells(targetCells);
```

- [ ] **Step 4: Stop generation at target cell count**

In `TopologyGenerator.generate`, derive the target and break the inner loop when reached:

```java
int targetCells = sc.getTargetCells();
for (int siteIdx = 1; siteIdx <= sc.getCount(); siteIdx++) {
    if (targetCells > 0 && records.size() >= targetCells) {
        break;
    }
    // existing site generation
    for (int cellIdx = 1; cellIdx <= numCells; cellIdx++) {
        if (targetCells > 0 && records.size() >= targetCells) {
            break;
        }
        // existing cell record creation
    }
}
```

- [ ] **Step 5: Pass target cells from benchmark runner**

In `JavaSimulatorProcessManager.envFor`, keep `FDB_SITES_COUNT` high enough and add the explicit target:

```java
env.put("FDB_SITES_COUNT", String.valueOf(plan.cellLevel()));
env.put("FDB_TOPOLOGY_TARGET_CELLS", String.valueOf(plan.cellLevel()));
```

- [ ] **Step 6: Run focused tests**

Run:

```powershell
mvn -pl topology-service,benchmark-runner -am -Dtest=TopologyGeneratorTest,TopologyMainTest,JavaSimulatorProcessManagerTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 7: Commit Task 2**

Run:

```powershell
git add topology-service/src/main/java/com/fdb/topology/TopologyConfig.java `
  topology-service/src/main/java/com/fdb/topology/TopologyMain.java `
  topology-service/src/main/java/com/fdb/topology/TopologyGenerator.java `
  topology-service/src/test/java/com/fdb/topology/TopologyGeneratorTest.java `
  topology-service/src/test/java/com/fdb/topology/TopologyMainTest.java `
  benchmark-runner/src/main/java/com/fdb/benchmark/JavaSimulatorProcessManager.java `
  benchmark-runner/src/test/java/com/fdb/benchmark/JavaSimulatorProcessManagerTest.java
git diff --cached --name-only
git commit -m "fix: generate benchmark topology by target cells"
```

Expected staged files: exactly the seven files above.

---

### Task 3: Source Pacing And Source Metrics

**Files:**
- Create: `simulator/src/main/java/com/fdb/simulator/SourceMetricsWriter.java`
- Modify: `simulator/src/main/java/com/fdb/simulator/ChrSimulator.java`
- Modify: `simulator/src/main/java/com/fdb/simulator/PmSimulator.java`
- Modify: `simulator/src/main/java/com/fdb/simulator/CfgSimulator.java`
- Modify: `simulator/src/test/java/com/fdb/simulator/ChrSimulatorTest.java`
- Create: `simulator/src/test/java/com/fdb/simulator/SourceMetricsWriterTest.java`
- Create: `benchmark-runner/src/main/java/com/fdb/benchmark/SourceMetricsSnapshot.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/RunObservation.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkRunResult.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/DefaultBenchmarkClients.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkResultWriter.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/JavaSimulatorProcessManager.java`
- Create: `benchmark-runner/src/test/java/com/fdb/benchmark/SourceMetricsSnapshotTest.java`
- Modify: `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkResultWriterTest.java`

- [ ] **Step 1: Write failing CHR pacing tests**

Add to `ChrSimulatorTest`:

```java
@Test
void calculates_due_events_from_global_target_eps() {
    assertThat(ChrSimulator.dueEvents(15_000, 1_000L, 2_000L, 0L)).isEqualTo(15_000);
    assertThat(ChrSimulator.dueEvents(15_000, 1_000L, 2_000L, 14_900L)).isEqualTo(100);
    assertThat(ChrSimulator.dueEvents(15_000, 1_000L, 1_500L, 10_000L)).isZero();
}
```

- [ ] **Step 2: Write failing source metrics snapshot tests**

Create `SourceMetricsSnapshotTest`:

```java
package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SourceMetricsSnapshotTest {
  @TempDir Path tempDir;

  @Test
  void reads_chr_pm_cfg_metrics_and_computes_delivery() throws Exception {
    BenchmarkRunPlan plan = new BenchmarkRunPlan("bench", BenchmarkSink.STARROCKS, 1000, 300,
        "bench-starrocks-cells1000-eps300", "starrocks");
    Path runDir = SourceMetricsSnapshot.runDir(tempDir, plan);
    Files.createDirectories(runDir);
    Files.writeString(runDir.resolve("chr-source-metrics.json"),
        "{\"source\":\"chr\",\"targetEps\":300,\"published\":600,\"observedEps\":294.0}");
    Files.writeString(runDir.resolve("pm-source-metrics.json"),
        "{\"source\":\"pm\",\"published\":1000,\"observedEps\":100.0}");
    Files.writeString(runDir.resolve("cfg-source-metrics.json"),
        "{\"source\":\"cfg\",\"published\":1000,\"observedEps\":1000.0}");

    SourceMetricsSnapshot snapshot = SourceMetricsSnapshot.read(tempDir, plan);

    assertThat(snapshot.present()).isTrue();
    assertThat(snapshot.chrTargetEps()).isEqualTo(300);
    assertThat(snapshot.chrPublished()).isEqualTo(600);
    assertThat(snapshot.chrObservedEps()).isEqualTo(294.0);
    assertThat(snapshot.producerDeliveryRatio()).isEqualTo(0.98);
    assertThat(snapshot.pmTotalPerCell(1000)).isEqualTo(1.0);
    assertThat(snapshot.cfgTotalPerCell(1000)).isEqualTo(1.0);
  }
}
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```powershell
mvn -pl simulator,benchmark-runner -am -Dtest=ChrSimulatorTest,SourceMetricsSnapshotTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because `dueEvents`, `SourceMetricsSnapshot`, and metrics files are not implemented.

- [ ] **Step 4: Create simulator metrics writer**

Create `SourceMetricsWriter`:

```java
package com.fdb.simulator;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

final class SourceMetricsWriter {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Path path;

    SourceMetricsWriter(String envKey) {
        String value = System.getenv(envKey);
        this.path = value == null || value.isBlank() ? null : Path.of(value);
    }

    void write(String source, long targetEps, long published, double observedEps) {
        if (path == null) {
            return;
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("source", source);
        payload.put("targetEps", targetEps);
        payload.put("published", published);
        payload.put("observedEps", observedEps);
        payload.put("updatedAtEpochMs", System.currentTimeMillis());
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            JSON.writeValue(path.toFile(), payload);
        } catch (IOException e) {
            throw new IllegalStateException("failed to write source metrics " + path, e);
        }
    }
}
```

- [ ] **Step 5: Implement strict CHR pacing**

Add helper to `ChrSimulator`:

```java
static long dueEvents(long targetEps, long startMs, long nowMs, long alreadyPublished) {
    if (targetEps <= 0 || nowMs <= startMs) {
        return 0L;
    }
    long expected = Math.floorDiv((nowMs - startMs) * targetEps, 1000L);
    return Math.max(0L, expected - alreadyPublished);
}
```

Change the publish loop to use due events instead of per-cell probability:

```java
long due = dueEvents(baseEps, startTime, now, counter);
for (long i = 0; i < due; i++) {
    TopologyRecord cell = cells.get(rng.nextInt(cells.size()));
    List<String> users = cellUsers.get(cell.getCellId().toString());
    if (users == null || users.isEmpty()) {
        continue;
    }
    String imsi = users.get(rng.nextInt(users.size()));
    ChrEvent event = generateEvent(cell, imsi, now,
        config.getDouble("rate.outOfOrderProb", 0.05),
        config.getLong("rate.maxOutOfOrderLagMs", 5000));
    publisher.publish(cell.getSiteId().toString(), event);
    counter++;
}
```

Write CHR metrics after each loop:

```java
double observedEps = counter / Math.max((System.currentTimeMillis() - startTime) / 1000.0d, 0.001d);
sourceMetrics.write("chr", baseEps, counter, observedEps);
```

- [ ] **Step 6: Write PM and CFG metrics**

In `PmSimulator`, after each flushed 10s window:

```java
long totalPublished = 0L;
long metricsStartMs = System.currentTimeMillis();
// increment totalPublished by published each window
double observedEps = totalPublished / Math.max((System.currentTimeMillis() - metricsStartMs) / 1000.0d, 0.001d);
sourceMetrics.write("pm", 0L, totalPublished, observedEps);
```

In `CfgSimulator`, after baseline and each update batch:

```java
long totalPublished = 0L;
long metricsStartMs = System.currentTimeMillis();
// increment totalPublished for baseline, update, and tombstone records
double observedEps = totalPublished / Math.max((System.currentTimeMillis() - metricsStartMs) / 1000.0d, 0.001d);
sourceMetrics.write("cfg", 0L, totalPublished, observedEps);
```

- [ ] **Step 7: Add benchmark source metrics snapshot**

Create `SourceMetricsSnapshot` with this public shape:

```java
package com.fdb.benchmark;

public record SourceMetricsSnapshot(
    boolean present,
    long chrTargetEps,
    long chrPublished,
    double chrObservedEps,
    long pmPublished,
    double pmObservedEps,
    long cfgPublished,
    double cfgObservedEps) {

  public static SourceMetricsSnapshot empty() {
    return new SourceMetricsSnapshot(false, 0, 0, 0, 0, 0, 0, 0);
  }

  public double producerDeliveryRatio() {
    return chrTargetEps <= 0 ? 1.0d : chrObservedEps / chrTargetEps;
  }

  public double chrTotalPerCell(int cellLevel) {
    return perCell(chrPublished, cellLevel);
  }

  public double chrPerSecondPerCell(int cellLevel) {
    return perCell(chrObservedEps, cellLevel);
  }

  public double pmTotalPerCell(int cellLevel) {
    return perCell(pmPublished, cellLevel);
  }

  public double pmPerSecondPerCell(int cellLevel) {
    return perCell(pmObservedEps, cellLevel);
  }

  public double cfgTotalPerCell(int cellLevel) {
    return perCell(cfgPublished, cellLevel);
  }

  public double cfgPerSecondPerCell(int cellLevel) {
    return perCell(cfgObservedEps, cellLevel);
  }

  private static double perCell(double value, int cellLevel) {
    return cellLevel <= 0 ? 0.0d : value / cellLevel;
  }
}
```

Add static `read(Path outputRoot, BenchmarkRunPlan plan)` and `runDir(Path outputRoot, BenchmarkRunPlan plan)` methods that read `chr-source-metrics.json`, `pm-source-metrics.json`, and `cfg-source-metrics.json` from the run directory.

- [ ] **Step 8: Wire source metrics into benchmark observations**

Add `SourceMetricsSnapshot source` to `RunObservation` and `BenchmarkRunResult`, with constructors defaulting to `SourceMetricsSnapshot.empty()`.

In `DefaultBenchmarkClients.observe`:

```java
SourceMetricsSnapshot source = SourceMetricsSnapshot.read(config.outputRoot(), plan);
return new RunObservation(flink.snapshot(), observability.snapshot(), storageProbe.snapshot(), topology, source);
```

In `JavaSimulatorProcessManager.envFor`:

```java
Path runDir = SourceMetricsSnapshot.runDir(outputRoot, plan);
env.put("FDB_CHR_METRICS_FILE", portablePath(runDir.resolve("chr-source-metrics.json")));
env.put("FDB_PM_METRICS_FILE", portablePath(runDir.resolve("pm-source-metrics.json")));
env.put("FDB_CFG_METRICS_FILE", portablePath(runDir.resolve("cfg-source-metrics.json")));
```

- [ ] **Step 9: Write source metrics artifacts**

In `BenchmarkResultWriter`, write `source-metrics.json` under each run directory:

```java
MAPPER.writeValue(runDir.resolve("source-metrics.json").toFile(), result.source());
```

- [ ] **Step 10: Run focused tests**

Run:

```powershell
mvn -pl simulator,benchmark-runner -am -Dtest=ChrSimulatorTest,SourceMetricsWriterTest,SourceMetricsSnapshotTest,BenchmarkResultWriterTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 11: Commit Task 3**

Run:

```powershell
git add simulator/src/main/java/com/fdb/simulator/SourceMetricsWriter.java `
  simulator/src/main/java/com/fdb/simulator/ChrSimulator.java `
  simulator/src/main/java/com/fdb/simulator/PmSimulator.java `
  simulator/src/main/java/com/fdb/simulator/CfgSimulator.java `
  simulator/src/test/java/com/fdb/simulator/ChrSimulatorTest.java `
  simulator/src/test/java/com/fdb/simulator/SourceMetricsWriterTest.java `
  benchmark-runner/src/main/java/com/fdb/benchmark/SourceMetricsSnapshot.java `
  benchmark-runner/src/main/java/com/fdb/benchmark/RunObservation.java `
  benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkRunResult.java `
  benchmark-runner/src/main/java/com/fdb/benchmark/DefaultBenchmarkClients.java `
  benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkResultWriter.java `
  benchmark-runner/src/main/java/com/fdb/benchmark/JavaSimulatorProcessManager.java `
  benchmark-runner/src/test/java/com/fdb/benchmark/SourceMetricsSnapshotTest.java `
  benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkResultWriterTest.java
git diff --cached --name-only
git commit -m "feat: capture benchmark source metrics"
```

Expected staged files: exactly the listed simulator and benchmark-runner files.

---

### Task 4: Deterministic 5 Percent Anomaly Injection

**Files:**
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkConfig.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/JavaSimulatorProcessManager.java`
- Modify: `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkConfigTest.java`
- Modify: `benchmark-runner/src/test/java/com/fdb/benchmark/JavaSimulatorProcessManagerTest.java`
- Modify: `simulator/src/main/java/com/fdb/simulator/ChrSimulator.java`
- Modify: `simulator/src/main/java/com/fdb/simulator/PmSimulator.java`
- Modify: `simulator/src/test/java/com/fdb/simulator/ChrSimulatorTest.java`
- Create: `simulator/src/test/java/com/fdb/simulator/PmSimulatorTest.java`

- [ ] **Step 1: Write failing config and env tests**

Add `double anomalyInjectionRatio` to expected `BenchmarkConfig` assertions:

```java
BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
    "FDB_BENCHMARK_ANOMALY_INJECTION_RATIO", "0.05"));

assertThat(config.anomalyInjectionRatio()).isEqualTo(0.05);
```

Add to `JavaSimulatorProcessManagerTest`:

```java
assertThat(env).containsEntry("FDB_BENCHMARK_ANOMALY_INJECTION_RATIO", "0.05");
```

- [ ] **Step 2: Write failing cohort tests**

Add to `ChrSimulatorTest`:

```java
@Test
void anomaly_cohort_is_deterministic_and_ratio_bounded() {
    int selected = 0;
    for (int i = 0; i < 10_000; i++) {
        if (ChrSimulator.inAnomalyCohort("USER-" + i, 0.05)) {
            selected++;
        }
    }
    assertThat(selected).isBetween(350, 650);
    assertThat(ChrSimulator.inAnomalyCohort("USER-123", 0.05))
        .isEqualTo(ChrSimulator.inAnomalyCohort("USER-123", 0.05));
}
```

Add to `PmSimulatorTest`:

```java
@Test
void anomalous_pm_values_cross_cell_thresholds() {
    PmSimulator.AnomalyValues values = PmSimulator.anomalousValues();

    assertThat(values.avgRsrp()).isLessThan(-110.0f);
    assertThat(values.avgSinr()).isLessThan(-3.0f);
    assertThat(values.attachSuccessRate()).isLessThan(0.95f);
    assertThat(values.dropRate()).isGreaterThan(0.05f);
}
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```powershell
mvn -pl simulator,benchmark-runner -am -Dtest=ChrSimulatorTest,PmSimulatorTest,BenchmarkConfigTest,JavaSimulatorProcessManagerTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because anomaly ratio and cohort helpers are not implemented.

- [ ] **Step 4: Add anomaly ratio config**

Add `double anomalyInjectionRatio` to `BenchmarkConfig` record after `chrEpsPerCell`.

In `BenchmarkConfig.from`:

```java
BenchmarkThresholds.doubleValue(env, "FDB_BENCHMARK_ANOMALY_INJECTION_RATIO", 0.05),
```

In `JavaSimulatorProcessManager.envFor`:

```java
env.put("FDB_BENCHMARK_ANOMALY_INJECTION_RATIO", String.valueOf(baseEnv.getOrDefault(
    "FDB_BENCHMARK_ANOMALY_INJECTION_RATIO", "0.05")));
```

If `JavaSimulatorProcessManager` has access to `BenchmarkConfig`, prefer `String.valueOf(config.anomalyInjectionRatio())`; otherwise keep the env default above and pass benchmark env through `baseEnv`.

- [ ] **Step 5: Implement deterministic cohort helper**

Add to `ChrSimulator`:

```java
static boolean inAnomalyCohort(String id, double ratio) {
    if (id == null || id.isBlank() || ratio <= 0.0d) {
        return false;
    }
    if (ratio >= 1.0d) {
        return true;
    }
    int bucket = Math.floorMod(id.hashCode(), 10_000);
    return bucket < Math.round(ratio * 10_000);
}
```

Add the same helper to `PmSimulator` or extract it to package-private `AnomalyInjection` in `simulator/src/main/java/com/fdb/simulator/AnomalyInjection.java` if both simulators need the same code.

Add the package-visible helper shape used by `PmSimulatorTest`:

```java
record AnomalyValues(float avgRsrp, float avgSinr, float attachSuccessRate, float dropRate) {
}

static AnomalyValues anomalousValues() {
    int attempts = 100;
    int successes = 40;
    int dropped = 25;
    int totalConnections = 220;
    return new AnomalyValues(-125.0f, -8.0f,
        successes / (float) attempts,
        dropped / (float) totalConnections);
}
```

- [ ] **Step 6: Inject user and cell anomalies through ordinary fields**

In `ChrSimulator.generateEvent`, after building normal field values, apply:

```java
boolean anomalousUser = inAnomalyCohort(imsi, anomalyRatio);
boolean anomalousCell = inAnomalyCohort(cell.getCellId().toString(), anomalyRatio);
if (anomalousUser || anomalousCell) {
    eventType = ChrEventType.RRC_SETUP_FAIL;
    rsrp = -125.0f;
    sinr = -8.0f;
    rsrq = -18.0f;
    cqi = 1;
    mcs = 1;
}
int resultCode = anomalousUser ? 1
    : (eventType == ChrEventType.RRC_SETUP_FAIL || eventType == ChrEventType.DETACH ? 1 : 0);
Float latencyMs = anomalousUser ? 1_000.0f : null;
```

Set `resultCode` and `latencyMs` on the builder:

```java
.setResultCode(resultCode)
.setLatencyMs(latencyMs)
```

In `PmSimulator.generatePmStat`, if the cell is in the anomaly cohort, force:

```java
activeUsers = 200;
totalConnections = 220;
dropped = 25;
rrcAttempt = 100;
rrcSuccess = 40;
latency = 900.0f;
```

Set radio fields to poor values:

```java
float avgRsrp = anomalous ? -125.0f : (float) (-80 - (1 - (float) load) * 40 + rng.nextFloat() * 5);
float avgSinr = anomalous ? -8.0f : (float) (load * 20 + rng.nextFloat() * 5);
```

- [ ] **Step 7: Run focused tests**

Run:

```powershell
mvn -pl simulator,benchmark-runner -am -Dtest=ChrSimulatorTest,PmSimulatorTest,BenchmarkConfigTest,JavaSimulatorProcessManagerTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 8: Commit Task 4**

Run:

```powershell
git add benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkConfig.java `
  benchmark-runner/src/main/java/com/fdb/benchmark/JavaSimulatorProcessManager.java `
  benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkConfigTest.java `
  benchmark-runner/src/test/java/com/fdb/benchmark/JavaSimulatorProcessManagerTest.java `
  simulator/src/main/java/com/fdb/simulator/ChrSimulator.java `
  simulator/src/main/java/com/fdb/simulator/PmSimulator.java `
  simulator/src/test/java/com/fdb/simulator/ChrSimulatorTest.java `
  simulator/src/test/java/com/fdb/simulator/PmSimulatorTest.java
git diff --cached --name-only
git commit -m "feat: inject deterministic benchmark anomalies"
```

Expected staged files: exactly the eight files above, plus `AnomalyInjection.java` if extracted.

---

### Task 5: Source Backlog And Stability Decisions

**Files:**
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/FlinkOperatorSnapshot.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/FlinkSnapshot.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/FlinkRestClient.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkDecisionEngine.java`
- Modify: `benchmark-runner/src/test/java/com/fdb/benchmark/FlinkRestClientTest.java`
- Modify: `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkDecisionEngineTest.java`

- [ ] **Step 1: Write failing Flink pending records test**

In `FlinkRestClientTest`, add `pendingRecords` to the v1 metric response:

```json
{"id":"pendingRecords","value":"42"}
```

Add assertions:

```java
assertThat(snapshot.sourceBacklogRecords()).isEqualTo(42);
assertThat(snapshot.operators().get(0).pendingRecords()).isEqualTo(42);
```

- [ ] **Step 2: Write failing decision tests**

Add to `BenchmarkDecisionEngineTest`:

```java
@Test
void producer_under_delivery_marks_unstable() {
  RunObservation observation = healthy().withSource(
      new SourceMetricsSnapshot(true, 1000, 900, 900.0, 100, 10.0, 100, 100.0));

  BenchmarkRunResult result = engine.decide(plan(), observation);

  assertThat(result.status()).isEqualTo(BenchmarkStatus.UNSTABLE);
  assertThat(result.bottleneckReason()).contains("producer delivery");
}

@Test
void source_backlog_marks_unstable() {
  RunObservation observation = healthy().withFlink(healthy().flink().withSourceBacklogRecords(1));

  BenchmarkRunResult result = engine.decide(plan(), observation);

  assertThat(result.status()).isEqualTo(BenchmarkStatus.UNSTABLE);
  assertThat(result.bottleneckReason()).contains("source backlog");
}
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```powershell
mvn -pl benchmark-runner -Dtest=FlinkRestClientTest,BenchmarkDecisionEngineTest test
```

Expected: FAIL because backlog fields and decision checks are not implemented.

- [ ] **Step 4: Add pending records to Flink snapshots**

Add `long pendingRecords` to `FlinkOperatorSnapshot`.

Add `long sourceBacklogRecords` to `FlinkSnapshot` and add:

```java
public FlinkSnapshot withSourceBacklogRecords(long value) {
  return new FlinkSnapshot(jobStatus, backpressureRatio, checkpointDurationMs, consecutiveCheckpointFailures,
      recordsInPerSec, recordsOutPerSec, recordsInTotal, recordsOutTotal, taskManagers, slots,
      operators, operatorEdges, value);
}
```

Keep existing constructors by defaulting `sourceBacklogRecords` to `0L`.

- [ ] **Step 5: Read `pendingRecords` from Flink REST metrics**

Add `pendingRecords` to `VERTEX_RATE_METRICS`:

```java
"pendingRecords"
```

In `operatorSnapshot`, pass:

```java
(long) metricNumber(rateMetrics, "pendingRecords")
```

In `snapshot`, aggregate source backlog:

```java
long sourceBacklogRecords = 0L;
// inside vertex loop
if (operator.name().toLowerCase().contains("source")) {
  sourceBacklogRecords = Math.max(sourceBacklogRecords, operator.pendingRecords());
}
```

Return it in `FlinkSnapshot`.

- [ ] **Step 6: Add decision checks**

In `BenchmarkDecisionEngine.decide`, after job status and before latency checks:

```java
if (observation.source().present()
    && observation.source().producerDeliveryRatio() < thresholds.minProducerDeliveryRatio()) {
  return result(plan, BenchmarkStatus.UNSTABLE,
      "producer delivery ratio " + observation.source().producerDeliveryRatio(), observation);
}
if (flink.sourceBacklogRecords() > thresholds.maxSourceBacklogRecords()) {
  return result(plan, BenchmarkStatus.UNSTABLE,
      "source backlog " + flink.sourceBacklogRecords() + " records", observation);
}
```

- [ ] **Step 7: Run focused tests**

Run:

```powershell
mvn -pl benchmark-runner -Dtest=FlinkRestClientTest,BenchmarkDecisionEngineTest test
```

Expected: PASS.

- [ ] **Step 8: Commit Task 5**

Run:

```powershell
git add benchmark-runner/src/main/java/com/fdb/benchmark/FlinkOperatorSnapshot.java `
  benchmark-runner/src/main/java/com/fdb/benchmark/FlinkSnapshot.java `
  benchmark-runner/src/main/java/com/fdb/benchmark/FlinkRestClient.java `
  benchmark-runner/src/main/java/com/fdb/benchmark/BenchmarkDecisionEngine.java `
  benchmark-runner/src/test/java/com/fdb/benchmark/FlinkRestClientTest.java `
  benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkDecisionEngineTest.java
git diff --cached --name-only
git commit -m "feat: classify source backlog benchmark instability"
```

Expected staged files: exactly the six files above.

---

### Task 6: Report Rendering, N/A Latency, Docs, And Verification

**Files:**
- Modify: `flink-job/src/main/java/com/fdb/job/metrics/LatencyStats.java`
- Modify: `flink-job/src/test/java/com/fdb/job/metrics/LatencyStatsTest.java`
- Modify: `benchmark-runner/src/main/java/com/fdb/benchmark/HtmlReportWriter.java`
- Modify: `benchmark-runner/src/test/java/com/fdb/benchmark/HtmlReportWriterTest.java`
- Modify: `benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkResultWriterTest.java`
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-04-29-flink-data-balance-design.md`

- [ ] **Step 1: Write failing latency empty-sample test**

Create or update `LatencyStatsTest`:

```java
package com.fdb.job.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LatencyStatsTest {
    @Test
    void empty_snapshot_uses_negative_one_for_missing_percentiles() {
        LatencyStats.Snapshot snapshot = new LatencyStats().snapshotAndReset();

        assertThat(snapshot.p50Ms()).isEqualTo(-1L);
        assertThat(snapshot.p95Ms()).isEqualTo(-1L);
        assertThat(snapshot.p99Ms()).isEqualTo(-1L);
    }
}
```

- [ ] **Step 2: Write failing report tests**

In `HtmlReportWriterTest`, assert:

```java
assertThat(Files.readString(report))
    .contains("Source Density")
    .contains("CHR total")
    .contains("PM total")
    .contains("CFG total")
    .contains("Delivery Ratio")
    .contains("Source Backlog")
    .contains("Checkpoint Interval")
    .contains("N/A");
```

Adjust the sample result in `BenchmarkResultWriterTest.sampleResult` so one stage has `-1` latency:

```java
new StageLatencySnapshot("empty-stage", -1, -1, -1, 0)
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```powershell
mvn -pl flink-job,benchmark-runner -am -Dtest=LatencyStatsTest,HtmlReportWriterTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: FAIL because empty latency still returns `0` and the report does not include source density.

- [ ] **Step 4: Change empty latency semantics**

In `LatencyStats.Snapshot.empty`:

```java
static Snapshot empty() {
    return new Snapshot(-1L, -1L, -1L, -1L);
}
```

In `BenchmarkDecisionEngine`, ignore negative latency values:

```java
private static long positiveMax(long left, long right) {
  return Math.max(Math.max(left, 0L), Math.max(right, 0L));
}
```

Use `positiveMax(fdb.kpi1mP95Ms(), fdb.kpi5mP95Ms())` for KPI latency comparison, and compare sink latency only when `fdb.sinkP95Ms() >= 0`.

- [ ] **Step 5: Render source density and N/A values**

In `HtmlReportWriter.formatMs`:

```java
private static String formatMs(long value) {
  return value < 0 ? "N/A" : value + " ms";
}
```

Add `sourceDensityTable(result)` to `runReport` between `Run Summary` and `Run Notes`:

```java
<h2>Source Density</h2>
%s
```

Implement:

```java
private static String sourceDensityTable(BenchmarkRunResult result) {
  SourceMetricsSnapshot source = result.source();
  int cells = result.plan().cellLevel();
  return """
      <table>
        <thead><tr><th>Source</th><th>Total</th><th>Records/s</th><th>Total/cell</th><th>Records/s/cell</th></tr></thead>
        <tbody>
          <tr><td>CHR total</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>
          <tr><td>PM total</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>
          <tr><td>CFG total</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>
        </tbody>
      </table>
      """.formatted(
          formatNumber(source.chrPublished()), formatNumber(source.chrObservedEps()),
          formatNumber(source.chrTotalPerCell(cells)), formatNumber(source.chrPerSecondPerCell(cells)),
          formatNumber(source.pmPublished()), formatNumber(source.pmObservedEps()),
          formatNumber(source.pmTotalPerCell(cells)), formatNumber(source.pmPerSecondPerCell(cells)),
          formatNumber(source.cfgPublished()), formatNumber(source.cfgObservedEps()),
          formatNumber(source.cfgTotalPerCell(cells)), formatNumber(source.cfgPerSecondPerCell(cells)));
}
```

Add run summary rows:

```java
row("Delivery Ratio", formatRatio(result.source().producerDeliveryRatio())),
row("Source Backlog", String.valueOf(result.flink().sourceBacklogRecords())),
row("Checkpoint Interval", config.target().equals("local") ? "30s default" : "30s default, max 180s for file sinks")
```

- [ ] **Step 6: Update README and Spec config references**

Add or update these entries in README and the existing Spec:

```text
FDB_BENCHMARK_CHR_EPS_PER_CELL=0.3
FDB_BENCHMARK_ANOMALY_INJECTION_RATIO=0.05
FDB_BENCHMARK_MIN_PRODUCER_DELIVERY_RATIO=0.98
FDB_BENCHMARK_MAX_SOURCE_BACKLOG_RECORDS=0
```

State in Chinese:

```text
Target CHR EPS 是整轮压测的全局 CHR EPS，计算公式为 cellLevel * FDB_BENCHMARK_CHR_EPS_PER_CELL。
cellLevel 表示目标生成小区数，不再乘以站点或每站小区估计。
```

- [ ] **Step 7: Run focused tests**

Run:

```powershell
mvn -pl flink-job,benchmark-runner -am -Dtest=LatencyStatsTest,HtmlReportWriterTest,BenchmarkResultWriterTest,BenchmarkDecisionEngineTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Expected: PASS.

- [ ] **Step 8: Run full affected test set**

Run:

```powershell
mvn -pl benchmark-runner,simulator,topology-service,flink-job -am test
```

Expected: PASS.

- [ ] **Step 9: Run GitNexus detect_changes before final commit**

Run tool:

```json
{"repo":"flink-data-balance","scope":"all"}
```

Expected: changed files match this plan. If risk is `HIGH` or `CRITICAL`, inspect affected processes and add or run the matching tests before committing.

- [ ] **Step 10: Commit Task 6**

Run:

```powershell
git add flink-job/src/main/java/com/fdb/job/metrics/LatencyStats.java `
  flink-job/src/test/java/com/fdb/job/metrics/LatencyStatsTest.java `
  benchmark-runner/src/main/java/com/fdb/benchmark/HtmlReportWriter.java `
  benchmark-runner/src/test/java/com/fdb/benchmark/HtmlReportWriterTest.java `
  benchmark-runner/src/test/java/com/fdb/benchmark/BenchmarkResultWriterTest.java `
  README.md `
  docs/superpowers/specs/2026-04-29-flink-data-balance-design.md
git diff --cached --name-only
git commit -m "feat: report benchmark source density and latency gaps"
```

Expected staged files: exactly the seven files above.

---

## Final Verification

- [ ] Run all affected tests:

```powershell
mvn -pl benchmark-runner,simulator,topology-service,flink-job -am test
```

Expected: PASS.

- [ ] Generate a dry-run report:

```powershell
scripts/benchmark.sh local --dry-run
```

Expected: `benchmark-runner/output/benchmark-runs/<benchmarkId>/index.html` exists and a single-run `report.html` includes Source Density, Delivery Ratio, Source Backlog, Operator Throughput, Latency, and Sink & Storage.

- [ ] Check docs for old pressure formula:

```powershell
rg -n "TOPOLOGY_CELLS_PER_SITE|cellsPerSiteEstimate|cellLevel \\* FDB_BENCHMARK_TOPOLOGY" README.md docs/superpowers/specs/2026-04-29-flink-data-balance-design.md
```

Expected: no matches.

- [ ] Run final GitNexus staged check before any final push:

```json
{"repo":"flink-data-balance","scope":"staged"}
```

Expected: no unexpected HIGH/CRITICAL impact. If the final commit spans multiple tasks, summarize the impact in the commit message body.
