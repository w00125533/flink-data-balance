# Sink Benchmarking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the Flink job into clearer packages, run each benchmark with exactly one business result sink type, persist lightweight runtime metrics history, generate benchmark reports, and update the frontend flow overview to show the actual running topology.

**Architecture:** Keep `FlinkJobMain` as the topology entry point, but move focused classes into `config`, `source`, `model`, `enrich`, `kpi`, `anomaly`, `balance`, `sink`, and `metrics` packages. Add a small `ResultSinks` facade that attaches exactly one selected business result sink branch for KPI 1m, KPI 5m, cell anomalies, and grid anomalies. Metrics remain Kafka-backed, with the Observability API maintaining latest in-memory snapshots and appending per-run JSONL history for report generation.

**Tech Stack:** Java 21, Flink 1.20, Kafka, StarRocks JDBC, Hive/HDFS FileSink, Iceberg HiveCatalog, Maven, React 18, TypeScript, Ant Design, AntV X6, Bash deploy scripts.

---

## Scope And Execution Notes

- This plan implements the approved spec in `docs/superpowers/specs/2026-04-29-flink-data-balance-design.md`.
- Java symbol changes require GitNexus impact analysis before editing Java classes.
- Before every commit run GitNexus `detect_changes`.
- Do not commit `.superpowers/`, local logs, Docker data, or generated target artifacts.
- Use `git mv` for package moves so history remains readable.
- Use focused commits after each task.

## File Structure

### Flink Job Packages

- `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`: topology assembly only.
- `flink-job/src/main/java/com/fdb/job/config/ResultSinkType.java`: enum for `starrocks`, `iceberg`, `hive`, `kafka`, `none`.
- `flink-job/src/main/java/com/fdb/job/config/ResultSinkConfig.java`: resolves `FDB_RESULT_SINK`, `FDB_DLQ_ENABLED`, metrics switches, checkpoint policy, run metadata.
- `flink-job/src/main/java/com/fdb/job/source/*`: Avro Kafka serializer/deserializer.
- `flink-job/src/main/java/com/fdb/job/model/*`: internal envelopes and minute facts.
- `flink-job/src/main/java/com/fdb/job/enrich/*`: enrichment state and DLQ output logic.
- `flink-job/src/main/java/com/fdb/job/kpi/*`: CHR/PM minute fact windows, 1m join, 5m rollup.
- `flink-job/src/main/java/com/fdb/job/anomaly/*`: cell and grid anomaly detectors.
- `flink-job/src/main/java/com/fdb/job/balance/*`: routing assigner, vbucket meter, and existing `coordinator/*`.
- `flink-job/src/main/java/com/fdb/job/sink/*`: `ResultSinks`, StarRocks, Hive, Iceberg, Kafka sink helpers and mappers.
- `flink-job/src/main/java/com/fdb/job/metrics/*`: probes and metrics publisher.
- `flink-job/src/test/java/com/fdb/job/**`: mirror production package layout.

### Common Metrics

- `common/src/main/java/com/fdb/common/metrics/StageMetricSample.java`: add run metadata fields while keeping JSON compatibility.
- `common/src/test/java/com/fdb/common/metrics/StageMetricSampleTest.java`: compatibility and JSON round-trip tests.

### Observability API

- `observability-api/src/main/java/com/fdb/observability/model/RuntimeConfig.java`: runtime config response.
- `observability-api/src/main/java/com/fdb/observability/model/ReportStatus.java`: report status response.
- `observability-api/src/main/java/com/fdb/observability/service/ObservabilitySnapshotService.java`: latest snapshots and runtime config.
- `observability-api/src/main/java/com/fdb/observability/service/MetricsHistoryService.java`: append JSONL metrics by run.
- `observability-api/src/main/java/com/fdb/observability/service/BenchmarkReportService.java`: summarize JSONL into `report.md`.
- `observability-api/src/main/java/com/fdb/observability/ObservabilityApiMain.java`: endpoints for runtime config, report generation and report content.

### Frontend

- `frontend/src/types/observability.ts`: runtime config, report status, sink type fields.
- `frontend/src/api/client.ts`: fetch runtime config and report status.
- `frontend/src/pages/FlowOverview.tsx`: run bar, actual DAG, bottleneck summary, report status.
- `frontend/src/components/StreamingFlowGraph.tsx`: node positions for selected sink nodes.
- `frontend/src/components/flowEdges.ts`: edges filtered by actual stage IDs and selected sink.
- `frontend/src/components/flowEdges.test.ts`: sink-specific edge tests.

### Scripts And Docs

- `scripts/deploy.sh`: add `report` command and pass run/report env.
- `.env.example.local`, `.env.example.external-yarn`: add result sink, metrics, DLQ, report, file sink settings when these example files exist in the repo.
- `README.md`: document sink benchmark usage and report command.

---

### Task 1: Run Impact Analysis And Repackage Flink Classes

**Files:**
- Move: `flink-job/src/main/java/com/fdb/job/*.java`
- Move: `flink-job/src/main/java/com/fdb/job/coordinator/*`
- Move: `flink-job/src/test/java/com/fdb/job/*.java`
- Move: `flink-job/src/test/java/com/fdb/job/coordinator/*`
- Modify: all moved Java files, package declarations and imports.

- [ ] **Step 1: Run GitNexus impact analysis before Java edits**

Use the available GitNexus impact-analysis tool for these symbols:

```text
FlinkJobMain
EnrichmentProcessFunction
MinuteKpiJoinFunction
StageMetricsProbe
SinkLatencyProbe
ResultSinks
IcebergSinks
HiveSinks
```

Expected: no HIGH or CRITICAL risk that blocks a package move plus focused sink refactor.

- [ ] **Step 2: Move production files with `git mv`**

Run:

```powershell
New-Item -ItemType Directory -Force `
  flink-job/src/main/java/com/fdb/job/config,`
  flink-job/src/main/java/com/fdb/job/source,`
  flink-job/src/main/java/com/fdb/job/model,`
  flink-job/src/main/java/com/fdb/job/enrich,`
  flink-job/src/main/java/com/fdb/job/kpi,`
  flink-job/src/main/java/com/fdb/job/anomaly,`
  flink-job/src/main/java/com/fdb/job/balance,`
  flink-job/src/main/java/com/fdb/job/sink,`
  flink-job/src/main/java/com/fdb/job/metrics

git mv flink-job/src/main/java/com/fdb/job/JobConfig.java flink-job/src/main/java/com/fdb/job/config/JobConfig.java
git mv flink-job/src/main/java/com/fdb/job/RuleConfig.java flink-job/src/main/java/com/fdb/job/config/RuleConfig.java

git mv flink-job/src/main/java/com/fdb/job/FlinkAvroDeserializer.java flink-job/src/main/java/com/fdb/job/source/FlinkAvroDeserializer.java
git mv flink-job/src/main/java/com/fdb/job/FlinkAvroSerializationSchema.java flink-job/src/main/java/com/fdb/job/source/FlinkAvroSerializationSchema.java

git mv flink-job/src/main/java/com/fdb/job/InputEnvelope.java flink-job/src/main/java/com/fdb/job/model/InputEnvelope.java
git mv flink-job/src/main/java/com/fdb/job/RoutedEnvelope.java flink-job/src/main/java/com/fdb/job/model/RoutedEnvelope.java
git mv flink-job/src/main/java/com/fdb/job/EnrichedChr.java flink-job/src/main/java/com/fdb/job/model/EnrichedChr.java
git mv flink-job/src/main/java/com/fdb/job/ChrMinuteFact.java flink-job/src/main/java/com/fdb/job/model/ChrMinuteFact.java
git mv flink-job/src/main/java/com/fdb/job/PmMinuteFact.java flink-job/src/main/java/com/fdb/job/model/PmMinuteFact.java
git mv flink-job/src/main/java/com/fdb/job/MinuteFactEnvelope.java flink-job/src/main/java/com/fdb/job/model/MinuteFactEnvelope.java

git mv flink-job/src/main/java/com/fdb/job/EnrichmentProcessFunction.java flink-job/src/main/java/com/fdb/job/enrich/EnrichmentProcessFunction.java

git mv flink-job/src/main/java/com/fdb/job/ChrMinuteFactWindowFunction.java flink-job/src/main/java/com/fdb/job/kpi/ChrMinuteFactWindowFunction.java
git mv flink-job/src/main/java/com/fdb/job/PmMinuteFactWindowFunction.java flink-job/src/main/java/com/fdb/job/kpi/PmMinuteFactWindowFunction.java
git mv flink-job/src/main/java/com/fdb/job/MinuteKpiJoinFunction.java flink-job/src/main/java/com/fdb/job/kpi/MinuteKpiJoinFunction.java
git mv flink-job/src/main/java/com/fdb/job/CellKpiRollupAggregator.java flink-job/src/main/java/com/fdb/job/kpi/CellKpiRollupAggregator.java
git mv flink-job/src/main/java/com/fdb/job/CellKpiWindowFunction.java flink-job/src/main/java/com/fdb/job/kpi/CellKpiWindowFunction.java
git mv flink-job/src/main/java/com/fdb/job/KpiAggregator.java flink-job/src/main/java/com/fdb/job/kpi/KpiAggregator.java

git mv flink-job/src/main/java/com/fdb/job/AnomalyDetector.java flink-job/src/main/java/com/fdb/job/anomaly/AnomalyDetector.java
git mv flink-job/src/main/java/com/fdb/job/CoverageHoleDetector.java flink-job/src/main/java/com/fdb/job/anomaly/CoverageHoleDetector.java

git mv flink-job/src/main/java/com/fdb/job/RoutingAssigner.java flink-job/src/main/java/com/fdb/job/balance/RoutingAssigner.java
git mv flink-job/src/main/java/com/fdb/job/VBucketLoadMeter.java flink-job/src/main/java/com/fdb/job/balance/VBucketLoadMeter.java
git mv flink-job/src/main/java/com/fdb/job/coordinator flink-job/src/main/java/com/fdb/job/balance/coordinator

git mv flink-job/src/main/java/com/fdb/job/HiveSinks.java flink-job/src/main/java/com/fdb/job/sink/HiveSinks.java
git mv flink-job/src/main/java/com/fdb/job/IcebergConfig.java flink-job/src/main/java/com/fdb/job/sink/IcebergConfig.java
git mv flink-job/src/main/java/com/fdb/job/IcebergSinks.java flink-job/src/main/java/com/fdb/job/sink/IcebergSinks.java
git mv flink-job/src/main/java/com/fdb/job/JdbcSinks.java flink-job/src/main/java/com/fdb/job/sink/JdbcSinks.java
git mv flink-job/src/main/java/com/fdb/job/StarRocksSinks.java flink-job/src/main/java/com/fdb/job/sink/StarRocksSinks.java
git mv flink-job/src/main/java/com/fdb/job/CellKpiIcebergMapper.java flink-job/src/main/java/com/fdb/job/sink/CellKpiIcebergMapper.java

git mv flink-job/src/main/java/com/fdb/job/StageMetricsProbe.java flink-job/src/main/java/com/fdb/job/metrics/StageMetricsProbe.java
git mv flink-job/src/main/java/com/fdb/job/SinkLatencyProbe.java flink-job/src/main/java/com/fdb/job/metrics/SinkLatencyProbe.java
git mv flink-job/src/main/java/com/fdb/job/MetricSamplePublisher.java flink-job/src/main/java/com/fdb/job/metrics/MetricSamplePublisher.java
```

Expected: `git status --short` shows renamed Java files, not delete/add pairs for moved files where Git detects renames.

- [ ] **Step 3: Move tests with `git mv`**

Run:

```powershell
New-Item -ItemType Directory -Force `
  flink-job/src/test/java/com/fdb/job/config,`
  flink-job/src/test/java/com/fdb/job/source,`
  flink-job/src/test/java/com/fdb/job/model,`
  flink-job/src/test/java/com/fdb/job/enrich,`
  flink-job/src/test/java/com/fdb/job/kpi,`
  flink-job/src/test/java/com/fdb/job/anomaly,`
  flink-job/src/test/java/com/fdb/job/balance,`
  flink-job/src/test/java/com/fdb/job/sink,`
  flink-job/src/test/java/com/fdb/job/metrics

git mv flink-job/src/test/java/com/fdb/job/CellKpiIcebergMapperTest.java flink-job/src/test/java/com/fdb/job/sink/CellKpiIcebergMapperTest.java
git mv flink-job/src/test/java/com/fdb/job/HiveSinksTest.java flink-job/src/test/java/com/fdb/job/sink/HiveSinksTest.java
git mv flink-job/src/test/java/com/fdb/job/IcebergConfigTest.java flink-job/src/test/java/com/fdb/job/sink/IcebergConfigTest.java
git mv flink-job/src/test/java/com/fdb/job/IcebergSinksTest.java flink-job/src/test/java/com/fdb/job/sink/IcebergSinksTest.java
git mv flink-job/src/test/java/com/fdb/job/JdbcSinksTest.java flink-job/src/test/java/com/fdb/job/sink/JdbcSinksTest.java
git mv flink-job/src/test/java/com/fdb/job/StarRocksSinksTest.java flink-job/src/test/java/com/fdb/job/sink/StarRocksSinksTest.java

git mv flink-job/src/test/java/com/fdb/job/CellKpiRollupAggregatorTest.java flink-job/src/test/java/com/fdb/job/kpi/CellKpiRollupAggregatorTest.java
git mv flink-job/src/test/java/com/fdb/job/ChrMinuteFactWindowFunctionTest.java flink-job/src/test/java/com/fdb/job/kpi/ChrMinuteFactWindowFunctionTest.java
git mv flink-job/src/test/java/com/fdb/job/MinuteKpiJoinFunctionTest.java flink-job/src/test/java/com/fdb/job/kpi/MinuteKpiJoinFunctionTest.java
git mv flink-job/src/test/java/com/fdb/job/PmMinuteFactWindowFunctionTest.java flink-job/src/test/java/com/fdb/job/kpi/PmMinuteFactWindowFunctionTest.java

git mv flink-job/src/test/java/com/fdb/job/RoutingAssignerTest.java flink-job/src/test/java/com/fdb/job/balance/RoutingAssignerTest.java
git mv flink-job/src/test/java/com/fdb/job/VBucketLoadMeterTest.java flink-job/src/test/java/com/fdb/job/balance/VBucketLoadMeterTest.java
git mv flink-job/src/test/java/com/fdb/job/coordinator flink-job/src/test/java/com/fdb/job/balance/coordinator

git mv flink-job/src/test/java/com/fdb/job/SinkLatencyProbeTest.java flink-job/src/test/java/com/fdb/job/metrics/SinkLatencyProbeTest.java
git mv flink-job/src/test/java/com/fdb/job/StageMetricsProbeTest.java flink-job/src/test/java/com/fdb/job/metrics/StageMetricsProbeTest.java
```

Expected: tests mirror the target package structure.

- [ ] **Step 4: Update package declarations and imports**

Use IDE-safe replace or targeted edits. The required package declarations are:

```java
package com.fdb.job.config;
package com.fdb.job.source;
package com.fdb.job.model;
package com.fdb.job.enrich;
package com.fdb.job.kpi;
package com.fdb.job.anomaly;
package com.fdb.job.balance;
package com.fdb.job.balance.coordinator;
package com.fdb.job.sink;
package com.fdb.job.metrics;
```

Update `FlinkJobMain.java` imports to include:

```java
import com.fdb.job.anomaly.AnomalyDetector;
import com.fdb.job.anomaly.CoverageHoleDetector;
import com.fdb.job.balance.RoutingAssigner;
import com.fdb.job.balance.VBucketLoadMeter;
import com.fdb.job.balance.coordinator.HeartbeatParser;
import com.fdb.job.balance.coordinator.HeartbeatPayload;
import com.fdb.job.balance.coordinator.LoadCoordinator;
import com.fdb.job.balance.coordinator.RoutingCsvSerializationSchema;
import com.fdb.job.balance.coordinator.RoutingEntry;
import com.fdb.job.config.JobConfig;
import com.fdb.job.config.RuleConfig;
import com.fdb.job.enrich.EnrichmentProcessFunction;
import com.fdb.job.kpi.CellKpiRollupAggregator;
import com.fdb.job.kpi.ChrMinuteFactWindowFunction;
import com.fdb.job.kpi.MinuteKpiJoinFunction;
import com.fdb.job.kpi.PmMinuteFactWindowFunction;
import com.fdb.job.metrics.SinkLatencyProbe;
import com.fdb.job.metrics.StageMetricsProbe;
import com.fdb.job.model.ChrMinuteFact;
import com.fdb.job.model.EnrichedChr;
import com.fdb.job.model.InputEnvelope;
import com.fdb.job.model.MinuteFactEnvelope;
import com.fdb.job.model.PmMinuteFact;
import com.fdb.job.model.RoutedEnvelope;
import com.fdb.job.sink.HiveSinks;
import com.fdb.job.sink.IcebergConfig;
import com.fdb.job.sink.IcebergSinks;
import com.fdb.job.sink.JdbcSinks;
import com.fdb.job.sink.StarRocksSinks;
import com.fdb.job.source.FlinkAvroDeserializer;
import com.fdb.job.source.FlinkAvroSerializationSchema;
```

- [ ] **Step 5: Run package compile tests**

Run:

```powershell
mvn -pl flink-job -am test -DskipITs
```

Expected: PASS. Failures should be missing imports or package-private access only.

- [ ] **Step 6: Run GitNexus detect_changes and commit**

Run:

```powershell
npx gitnexus detect_changes --repo flink-data-balance --scope unstaged
git add flink-job/src/main/java flink-job/src/test/java
git commit -m "refactor(flink): organize job packages"
```

Expected: GitNexus risk is low or medium with package moves only; commit succeeds.

---

### Task 2: Add Result Sink And Runtime Configuration

**Files:**
- Create: `flink-job/src/main/java/com/fdb/job/config/ResultSinkType.java`
- Create: `flink-job/src/main/java/com/fdb/job/config/ResultSinkConfig.java`
- Create: `flink-job/src/test/java/com/fdb/job/config/ResultSinkConfigTest.java`
- Modify: `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`
- Modify: `flink-job/src/test/java/com/fdb/job/FlinkJobMainTest.java`

- [ ] **Step 1: Write tests for result sink config**

Create `flink-job/src/test/java/com/fdb/job/config/ResultSinkConfigTest.java`:

```java
package com.fdb.job.config;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ResultSinkConfigTest {

    @Test
    void defaults_to_starrocks_with_dlq_and_metrics_enabled() {
        ResultSinkConfig config = ResultSinkConfig.resolve(Map.of(), new Properties());

        assertThat(config.resultSink()).isEqualTo(ResultSinkType.STARROCKS);
        assertThat(config.dlqEnabled()).isTrue();
        assertThat(config.metricsEnabled()).isTrue();
        assertThat(config.metricsHistoryEnabled()).isTrue();
        assertThat(config.metricsEmitIntervalMs()).isEqualTo(5_000L);
        assertThat(config.reportOnStop()).isFalse();
        assertThat(config.runId()).isNotBlank();
    }

    @Test
    void resolves_environment_over_properties() {
        Properties properties = new Properties();
        properties.setProperty("fdb.result.sink", "hive");
        properties.setProperty("fdb.dlq.enabled", "true");

        ResultSinkConfig config = ResultSinkConfig.resolve(Map.of(
            "FDB_RESULT_SINK", "iceberg",
            "FDB_DLQ_ENABLED", "false",
            "FDB_METRICS_ENABLED", "false",
            "FDB_METRICS_HISTORY_ENABLED", "false",
            "FDB_METRICS_EMIT_INTERVAL_MS", "7000",
            "FDB_REPORT_ON_STOP", "true",
            "FDB_RUN_ID", "run-a",
            "FDB_RUN_LABEL", "iceberg-p4"), properties);

        assertThat(config.resultSink()).isEqualTo(ResultSinkType.ICEBERG);
        assertThat(config.dlqEnabled()).isFalse();
        assertThat(config.metricsEnabled()).isFalse();
        assertThat(config.metricsHistoryEnabled()).isFalse();
        assertThat(config.metricsEmitIntervalMs()).isEqualTo(7_000L);
        assertThat(config.reportOnStop()).isTrue();
        assertThat(config.runId()).isEqualTo("run-a");
        assertThat(config.runLabel()).isEqualTo("iceberg-p4");
    }

    @Test
    void invalid_values_fall_back_to_safe_defaults() {
        ResultSinkConfig config = ResultSinkConfig.resolve(Map.of(
            "FDB_RESULT_SINK", "not-a-sink",
            "FDB_METRICS_EMIT_INTERVAL_MS", "0"), new Properties());

        assertThat(config.resultSink()).isEqualTo(ResultSinkType.STARROCKS);
        assertThat(config.metricsEmitIntervalMs()).isEqualTo(5_000L);
    }

    @Test
    void file_sink_checkpoint_interval_is_capped_at_three_minutes() {
        assertThat(ResultSinkConfig.effectiveCheckpointIntervalMs(ResultSinkType.HIVE, 240_000L))
            .isEqualTo(180_000L);
        assertThat(ResultSinkConfig.effectiveCheckpointIntervalMs(ResultSinkType.ICEBERG, 240_000L))
            .isEqualTo(180_000L);
        assertThat(ResultSinkConfig.effectiveCheckpointIntervalMs(ResultSinkType.STARROCKS, 240_000L))
            .isEqualTo(240_000L);
    }
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```powershell
mvn -pl flink-job -Dtest=ResultSinkConfigTest test
```

Expected: FAIL because `ResultSinkConfig` and `ResultSinkType` do not exist.

- [ ] **Step 3: Add `ResultSinkType`**

Create `flink-job/src/main/java/com/fdb/job/config/ResultSinkType.java`:

```java
package com.fdb.job.config;

import java.util.Locale;

public enum ResultSinkType {
    STARROCKS("starrocks", false),
    ICEBERG("iceberg", true),
    HIVE("hive", true),
    KAFKA("kafka", false),
    NONE("none", false);

    private final String configValue;
    private final boolean fileBased;

    ResultSinkType(String configValue, boolean fileBased) {
        this.configValue = configValue;
        this.fileBased = fileBased;
    }

    public String configValue() {
        return configValue;
    }

    public boolean fileBased() {
        return fileBased;
    }

    public static ResultSinkType parse(String value) {
        if (value == null || value.isBlank()) {
            return STARROCKS;
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        for (ResultSinkType type : values()) {
            if (type.configValue.equals(normalized)) {
                return type;
            }
        }
        return STARROCKS;
    }
}
```

- [ ] **Step 4: Add `ResultSinkConfig`**

Create `flink-job/src/main/java/com/fdb/job/config/ResultSinkConfig.java`:

```java
package com.fdb.job.config;

import java.time.Clock;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Properties;

public record ResultSinkConfig(
    ResultSinkType resultSink,
    boolean dlqEnabled,
    boolean metricsEnabled,
    boolean metricsHistoryEnabled,
    long metricsEmitIntervalMs,
    boolean reportOnStop,
    String runId,
    String runLabel
) {
    private static final long DEFAULT_METRICS_INTERVAL_MS = 5_000L;
    private static final long FILE_SINK_MAX_CHECKPOINT_INTERVAL_MS = 180_000L;

    public static ResultSinkConfig resolve(Map<String, String> env, Properties properties) {
        return new ResultSinkConfig(
            ResultSinkType.parse(resolveString(env, properties, "FDB_RESULT_SINK", "fdb.result.sink", "starrocks")),
            resolveBoolean(env, properties, "FDB_DLQ_ENABLED", "fdb.dlq.enabled", true),
            resolveBoolean(env, properties, "FDB_METRICS_ENABLED", "fdb.metrics.enabled", true),
            resolveBoolean(env, properties, "FDB_METRICS_HISTORY_ENABLED", "fdb.metrics.history.enabled", true),
            resolvePositiveLong(env, properties, "FDB_METRICS_EMIT_INTERVAL_MS", "fdb.metrics.emit.interval.ms",
                DEFAULT_METRICS_INTERVAL_MS),
            resolveBoolean(env, properties, "FDB_REPORT_ON_STOP", "fdb.report.on.stop", false),
            resolveString(env, properties, "FDB_RUN_ID", "fdb.run.id", defaultRunId()),
            resolveString(env, properties, "FDB_RUN_LABEL", "fdb.run.label", "")
        );
    }

    public static long effectiveCheckpointIntervalMs(ResultSinkType sinkType, long configuredMs) {
        if (sinkType.fileBased() && configuredMs > FILE_SINK_MAX_CHECKPOINT_INTERVAL_MS) {
            return FILE_SINK_MAX_CHECKPOINT_INTERVAL_MS;
        }
        return configuredMs;
    }

    private static String defaultRunId() {
        return DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(Clock.systemUTC().getZone())
            .format(Clock.systemUTC().instant());
    }

    private static String resolveString(Map<String, String> env, Properties properties, String envName,
                                        String propertyName, String defaultValue) {
        String value = env.get(envName);
        if (value == null || value.isBlank()) {
            value = properties.getProperty(propertyName);
        }
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    private static boolean resolveBoolean(Map<String, String> env, Properties properties, String envName,
                                          String propertyName, boolean defaultValue) {
        String value = resolveString(env, properties, envName, propertyName, Boolean.toString(defaultValue));
        return "true".equalsIgnoreCase(value) || "1".equals(value);
    }

    private static long resolvePositiveLong(Map<String, String> env, Properties properties, String envName,
                                            String propertyName, long defaultValue) {
        String value = resolveString(env, properties, envName, propertyName, Long.toString(defaultValue));
        try {
            long parsed = Long.parseLong(value);
            return parsed > 0L ? parsed : defaultValue;
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
```

If `defaultRunId()` fails because the formatter lacks zone handling, replace it with:

```java
return DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
    .withZone(java.time.ZoneOffset.UTC)
    .format(java.time.Instant.now());
```

- [ ] **Step 5: Wire checkpoint interval policy into `FlinkJobMain`**

Modify the top of `main` in `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`:

```java
ResultSinkConfig resultSinkConfig = ResultSinkConfig.resolve(System.getenv(), System.getProperties());
long checkpointIntervalMs = ResultSinkConfig.effectiveCheckpointIntervalMs(
    resultSinkConfig.resultSink(),
    resolveCheckpointIntervalMs(System.getenv(), System.getProperties()));

env.enableCheckpointing(checkpointIntervalMs);
```

Keep `IcebergConfig icebergConfig = resolveIcebergConfig(...)` for the next task if the existing code still needs it during incremental refactor.

- [ ] **Step 6: Update checkpoint default tests**

In `flink-job/src/test/java/com/fdb/job/FlinkJobMainTest.java`, change:

```java
void resolve_checkpoint_interval_defaults_to_one_minute()
```

to:

```java
void resolve_checkpoint_interval_defaults_to_thirty_seconds() {
    assertThat(FlinkJobMain.resolveCheckpointIntervalMs(Map.of(), new Properties()))
        .isEqualTo(30_000L);
}
```

Update `resolve_checkpoint_interval_falls_back_for_invalid_values` expected values from `60_000L` to `30_000L`.

- [ ] **Step 7: Update `resolveCheckpointIntervalMs` default**

In `FlinkJobMain.resolveCheckpointIntervalMs`, set the default to `30_000L`.

Expected implementation shape:

```java
static long resolveCheckpointIntervalMs(Map<String, String> env, Properties properties) {
    return resolvePositiveLong(env, properties, "FDB_FLINK_CHECKPOINT_INTERVAL_MS",
        "fdb.flink.checkpoint.interval.ms", 30_000L);
}
```

- [ ] **Step 8: Run config tests**

Run:

```powershell
mvn -pl flink-job -Dtest=ResultSinkConfigTest,FlinkJobMainTest test
```

Expected: PASS.

- [ ] **Step 9: Run GitNexus detect_changes and commit**

Run:

```powershell
npx gitnexus detect_changes --repo flink-data-balance --scope unstaged
git add flink-job/src/main/java/com/fdb/job/config flink-job/src/main/java/com/fdb/job/FlinkJobMain.java flink-job/src/test/java/com/fdb/job/config flink-job/src/test/java/com/fdb/job/FlinkJobMainTest.java
git commit -m "feat(flink): add result sink runtime config"
```

Expected: commit succeeds.

---

### Task 3: Introduce `ResultSinks` And Build Exactly One Business Sink Branch

**Files:**
- Create: `flink-job/src/main/java/com/fdb/job/sink/ResultSinks.java`
- Create: `flink-job/src/test/java/com/fdb/job/sink/ResultSinksTest.java`
- Modify: `flink-job/src/main/java/com/fdb/job/FlinkJobMain.java`
- Modify: `flink-job/src/main/java/com/fdb/job/sink/JdbcSinks.java`
- Modify: `flink-job/src/main/java/com/fdb/job/sink/StarRocksSinks.java`

- [ ] **Step 1: Add unit tests for branch naming**

Create `flink-job/src/test/java/com/fdb/job/sink/ResultSinksTest.java`:

```java
package com.fdb.job.sink;

import com.fdb.job.config.ResultSinkType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ResultSinksTest {

    @Test
    void stage_ids_are_scoped_to_selected_sink() {
        assertThat(ResultSinks.kpiStageId(ResultSinkType.STARROCKS, "1m")).isEqualTo("starrocks-kpi-1m");
        assertThat(ResultSinks.kpiStageId(ResultSinkType.ICEBERG, "5m")).isEqualTo("iceberg-kpi-5m");
        assertThat(ResultSinks.anomalyStageId(ResultSinkType.HIVE, "cell")).isEqualTo("hive-cell-anomaly");
        assertThat(ResultSinks.anomalyStageId(ResultSinkType.KAFKA, "grid")).isEqualTo("kafka-grid-anomaly");
    }

    @Test
    void none_sink_has_no_business_stage_ids() {
        assertThat(ResultSinks.businessStageIds(ResultSinkType.NONE)).isEmpty();
    }

    @Test
    void selected_sink_returns_four_business_stage_ids() {
        assertThat(ResultSinks.businessStageIds(ResultSinkType.STARROCKS))
            .containsExactly("starrocks-kpi-1m", "starrocks-kpi-5m",
                "starrocks-cell-anomaly", "starrocks-grid-anomaly");
    }
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```powershell
mvn -pl flink-job -Dtest=ResultSinksTest test
```

Expected: FAIL because `ResultSinks` does not exist.

- [ ] **Step 3: Create `ResultSinks` helper skeleton**

Create `flink-job/src/main/java/com/fdb/job/sink/ResultSinks.java`:

```java
package com.fdb.job.sink;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.CellKpi;
import com.fdb.job.config.ResultSinkConfig;
import com.fdb.job.config.ResultSinkType;
import com.fdb.job.metrics.SinkLatencyProbe;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

public final class ResultSinks {
    private ResultSinks() {}

    public static List<String> businessStageIds(ResultSinkType sinkType) {
        if (sinkType == ResultSinkType.NONE) {
            return List.of();
        }
        return List.of(
            kpiStageId(sinkType, "1m"),
            kpiStageId(sinkType, "5m"),
            anomalyStageId(sinkType, "cell"),
            anomalyStageId(sinkType, "grid"));
    }

    public static String kpiStageId(ResultSinkType sinkType, String window) {
        return sinkType.configValue() + "-kpi-" + window;
    }

    public static String anomalyStageId(ResultSinkType sinkType, String scope) {
        return sinkType.configValue() + "-" + scope + "-anomaly";
    }

    public static void attachBusinessResultSinks(
        DataStream<CellKpi> cellKpi1m,
        DataStream<CellKpi> cellKpi5m,
        DataStream<AnomalyEvent> cellAnomalies,
        DataStream<AnomalyEvent> gridAnomalies,
        ResultSinkConfig config,
        String bootstrap,
        IcebergConfig icebergConfig) {

        switch (config.resultSink()) {
            case STARROCKS -> attachStarRocks(cellKpi1m, cellKpi5m, cellAnomalies, gridAnomalies);
            case ICEBERG -> attachIceberg(cellKpi1m, cellKpi5m, cellAnomalies, gridAnomalies, icebergConfig);
            case HIVE -> attachHive(cellKpi1m, cellKpi5m, cellAnomalies, gridAnomalies);
            case KAFKA -> attachKafka(cellKpi1m, cellKpi5m, cellAnomalies, gridAnomalies, bootstrap);
            case NONE -> {
            }
        }
    }

    private static void attachStarRocks(DataStream<CellKpi> cellKpi1m, DataStream<CellKpi> cellKpi5m,
                                        DataStream<AnomalyEvent> cellAnomalies,
                                        DataStream<AnomalyEvent> gridAnomalies) {
        cellKpi1m.process(new SinkLatencyProbe<>("starrocks-kpi-1m", "Cell KPI 1m StarRocks Sink",
                "starrocks", "kpi_1m", "MIN_1", 100), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain().name("starrocks-kpi-1m")
            .sinkTo(JdbcSinks.cellKpiSink()).name("cell-kpi-jdbc-sink");
        cellKpi5m.process(new SinkLatencyProbe<>("starrocks-kpi-5m", "Cell KPI 5m StarRocks Sink",
                "starrocks", "kpi_5m", "MIN_5", 100), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain().name("starrocks-kpi-5m")
            .sinkTo(JdbcSinks.cellKpiSink()).name("cell-kpi-5m-jdbc-sink");
        cellAnomalies.process(new SinkLatencyProbe<>("starrocks-cell-anomaly", "Cell Anomaly StarRocks Sink",
                "starrocks", "cell_anomaly_events", "ANOMALY", 100), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain().name("starrocks-cell-anomaly")
            .sinkTo(StarRocksSinks.cellAnomalySink()).name("cell-anomaly-starrocks-sink");
        gridAnomalies.process(new SinkLatencyProbe<>("starrocks-grid-anomaly", "Grid Anomaly StarRocks Sink",
                "starrocks", "grid_anomaly_events", "ANOMALY", 100), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain().name("starrocks-grid-anomaly")
            .sinkTo(StarRocksSinks.gridAnomalySink()).name("grid-anomaly-starrocks-sink");
    }

    private static void attachIceberg(DataStream<CellKpi> cellKpi1m, DataStream<CellKpi> cellKpi5m,
                                      DataStream<AnomalyEvent> cellAnomalies,
                                      DataStream<AnomalyEvent> gridAnomalies,
                                      IcebergConfig icebergConfig) {
        IcebergSinks.appendBusinessResultSinks(cellKpi1m, cellKpi5m, cellAnomalies, gridAnomalies, icebergConfig);
    }

    private static void attachHive(DataStream<CellKpi> cellKpi1m, DataStream<CellKpi> cellKpi5m,
                                   DataStream<AnomalyEvent> cellAnomalies,
                                   DataStream<AnomalyEvent> gridAnomalies) {
        cellKpi1m.process(new SinkLatencyProbe<>("hive-kpi-1m", "Cell KPI 1m Hive Sink",
                "hive", "kpi_1m", "MIN_1", 100), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain().name("hive-kpi-1m")
            .sinkTo(HiveSinks.cellKpiSink("MIN_1")).name("cell-kpi-hive-sink");
        cellKpi5m.process(new SinkLatencyProbe<>("hive-kpi-5m", "Cell KPI 5m Hive Sink",
                "hive", "kpi_5m", "MIN_5", 100), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain().name("hive-kpi-5m")
            .sinkTo(HiveSinks.cellKpiSink("MIN_5")).name("cell-kpi-5m-hive-sink");
        cellAnomalies.process(new SinkLatencyProbe<>("hive-cell-anomaly", "Cell Anomaly Hive Sink",
                "hive", "cell_anomaly_events", "ANOMALY", 100), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain().name("hive-cell-anomaly")
            .sinkTo(HiveSinks.cellAnomalySink()).name("cell-anomaly-hive-sink");
        gridAnomalies.process(new SinkLatencyProbe<>("hive-grid-anomaly", "Grid Anomaly Hive Sink",
                "hive", "grid_anomaly_events", "ANOMALY", 100), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain().name("hive-grid-anomaly")
            .sinkTo(HiveSinks.gridAnomalySink()).name("grid-anomaly-hive-sink");
    }

    private static void attachKafka(DataStream<CellKpi> cellKpi1m, DataStream<CellKpi> cellKpi5m,
                                    DataStream<AnomalyEvent> cellAnomalies,
                                    DataStream<AnomalyEvent> gridAnomalies,
                                    String bootstrap) {
        cellKpi1m.process(new SinkLatencyProbe<>("kafka-kpi-1m", "Cell KPI 1m Kafka Sink",
                "kafka", "kpi_1m", "MIN_1", 100), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain().name("kafka-kpi-1m")
            .sinkTo(KafkaResultSinks.cellKpiSink(bootstrap, "cell-kpi-1m")).name("cell-kpi-kafka-sink");
        cellKpi5m.process(new SinkLatencyProbe<>("kafka-kpi-5m", "Cell KPI 5m Kafka Sink",
                "kafka", "kpi_5m", "MIN_5", 100), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain().name("kafka-kpi-5m")
            .sinkTo(KafkaResultSinks.cellKpiSink(bootstrap, "cell-kpi-5m")).name("cell-kpi-5m-kafka-sink");
        cellAnomalies.process(new SinkLatencyProbe<>("kafka-cell-anomaly", "Cell Anomaly Kafka Sink",
                "kafka", "cell_anomaly_events", "ANOMALY", 100), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain().name("kafka-cell-anomaly")
            .sinkTo(KafkaResultSinks.anomalySink(bootstrap, "cell-anomaly-events")).name("cell-anomaly-kafka-sink");
        gridAnomalies.process(new SinkLatencyProbe<>("kafka-grid-anomaly", "Grid Anomaly Kafka Sink",
                "kafka", "grid_anomaly_events", "ANOMALY", 100), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain().name("kafka-grid-anomaly")
            .sinkTo(KafkaResultSinks.anomalySink(bootstrap, "grid-anomaly-events")).name("grid-anomaly-kafka-sink");
    }
}
```

This code references `KafkaResultSinks`, `HiveSinks.cellAnomalySink`, `HiveSinks.gridAnomalySink`, and `IcebergSinks.appendBusinessResultSinks`; Task 4 creates those methods. Compile will fail until Task 4, so keep Task 3 and Task 4 in the same implementation checkpoint if using inline execution.

- [ ] **Step 4: Remove direct business sink attachment from `FlinkJobMain`**

In `FlinkJobMain`, replace the current repeated Kafka/StarRocks/Hive/Iceberg business sink blocks with:

```java
ResultSinks.attachBusinessResultSinks(
    cellKpi1m,
    cellKpi5m,
    cellAnomalies,
    coverageAnomalies,
    resultSinkConfig,
    bootstrap,
    icebergConfig);
```

Keep the DLQ block separate and wrap it:

```java
if (resultSinkConfig.dlqEnabled()) {
    enrichedRaw.getSideOutput(EnrichmentProcessFunction.CHR_DLQ)
        .sinkTo(chrDlqSink).name("chr-dlq-sink");
}
```

- [ ] **Step 5: Run stage-id tests**

Run:

```powershell
mvn -pl flink-job -Dtest=ResultSinksTest test
```

Expected: PASS after Task 4 helper methods are present.

- [ ] **Step 6: Commit after Task 4 compile passes**

Do not commit Task 3 while compilation is broken. Commit together with Task 4:

```powershell
npx gitnexus detect_changes --repo flink-data-balance --scope unstaged
git add flink-job/src/main/java/com/fdb/job/FlinkJobMain.java flink-job/src/main/java/com/fdb/job/sink flink-job/src/test/java/com/fdb/job/sink
git commit -m "feat(flink): select one business result sink"
```

---

### Task 4: Add Hive, Iceberg, And Kafka Helpers For KPI And Anomaly Results

**Files:**
- Create: `flink-job/src/main/java/com/fdb/job/sink/KafkaResultSinks.java`
- Create: `flink-job/src/main/java/com/fdb/job/sink/AnomalyEventIcebergMapper.java`
- Modify: `flink-job/src/main/java/com/fdb/job/sink/HiveSinks.java`
- Modify: `flink-job/src/main/java/com/fdb/job/sink/IcebergConfig.java`
- Modify: `flink-job/src/main/java/com/fdb/job/sink/IcebergSinks.java`
- Test: `flink-job/src/test/java/com/fdb/job/sink/HiveSinksTest.java`
- Test: `flink-job/src/test/java/com/fdb/job/sink/IcebergSinksTest.java`

- [ ] **Step 1: Write Hive path tests**

Add to `HiveSinksTest`:

```java
@Test
void anomaly_sinks_use_dataset_paths() {
    assertThat(HiveSinks.cellAnomalyOutputPath("hdfs://namenode:8020/warehouse/fdb"))
        .isEqualTo("hdfs://namenode:8020/warehouse/fdb/cell_anomaly_events");
    assertThat(HiveSinks.gridAnomalyOutputPath("hdfs://namenode:8020/warehouse/fdb"))
        .isEqualTo("hdfs://namenode:8020/warehouse/fdb/grid_anomaly_events");
}
```

- [ ] **Step 2: Write Iceberg identifier tests**

Add to `IcebergSinksTest`:

```java
@Test
void iceberg_business_tables_are_named_independently() {
    IcebergConfig config = new IcebergConfig(true, "fdb_iceberg",
        "hdfs://namenode:8020/warehouse/iceberg", "thrift://hive-metastore:9083",
        "iceberg_db", "cell_kpi", "cell_anomaly_events", "grid_anomaly_events");

    assertThat(IcebergSinks.cellKpiIdentifier(config).toString()).isEqualTo("iceberg_db.cell_kpi");
    assertThat(IcebergSinks.cellAnomalyIdentifier(config).toString())
        .isEqualTo("iceberg_db.cell_anomaly_events");
    assertThat(IcebergSinks.gridAnomalyIdentifier(config).toString())
        .isEqualTo("iceberg_db.grid_anomaly_events");
}
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```powershell
mvn -pl flink-job -Dtest=HiveSinksTest,IcebergSinksTest test
```

Expected: FAIL because new helper methods and Iceberg config fields are missing.

- [ ] **Step 4: Add Kafka result sink helper**

Create `flink-job/src/main/java/com/fdb/job/sink/KafkaResultSinks.java`:

```java
package com.fdb.job.sink;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.CellKpi;
import com.fdb.job.source.FlinkAvroSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

final class KafkaResultSinks {
    private KafkaResultSinks() {}

    static KafkaSink<CellKpi> cellKpiSink(String bootstrap, String topic) {
        return KafkaSink.<CellKpi>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(topic)
                .setValueSerializationSchema(new FlinkAvroSerializationSchema<>(CellKpi.class))
                .build())
            .build();
    }

    static KafkaSink<AnomalyEvent> anomalySink(String bootstrap, String topic) {
        return KafkaSink.<AnomalyEvent>builder()
            .setBootstrapServers(bootstrap)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(topic)
                .setValueSerializationSchema(new FlinkAvroSerializationSchema<>(AnomalyEvent.class))
                .build())
            .build();
    }
}
```

- [ ] **Step 5: Extend `HiveSinks`**

Add methods to `HiveSinks`:

```java
static String cellAnomalyOutputPath(String warehousePath) {
    return warehousePath + "/cell_anomaly_events";
}

static String gridAnomalyOutputPath(String warehousePath) {
    return warehousePath + "/grid_anomaly_events";
}

public static FileSink<AnomalyEvent> cellAnomalySink() {
    return FileSink
        .forBulkFormat(new Path(cellAnomalyOutputPath(warehousePath())),
            AvroParquetWriters.forSpecificRecord(AnomalyEvent.class))
        .withBucketAssigner(new DateTimeBucketAssigner<>(KPI_BUCKET_FORMAT))
        .withOutputFileConfig(PARQUET_OUTPUT_FILE_CONFIG)
        .build();
}

public static FileSink<AnomalyEvent> gridAnomalySink() {
    return FileSink
        .forBulkFormat(new Path(gridAnomalyOutputPath(warehousePath())),
            AvroParquetWriters.forSpecificRecord(AnomalyEvent.class))
        .withBucketAssigner(new DateTimeBucketAssigner<>(KPI_BUCKET_FORMAT))
        .withOutputFileConfig(PARQUET_OUTPUT_FILE_CONFIG)
        .build();
}
```

Add import:

```java
import com.fdb.common.avro.AnomalyEvent;
```

- [ ] **Step 6: Extend `IcebergConfig` record**

Change the record signature to:

```java
public record IcebergConfig(
    boolean enabled,
    String catalogName,
    String warehouse,
    String metastoreUri,
    String database,
    String table,
    String cellAnomalyTable,
    String gridAnomalyTable
) {
```

In `resolve`, map:

```java
resolveString(env, properties, "FDB_ICEBERG_KPI_TABLE", "fdb.iceberg.kpi.table",
    resolveString(env, properties, "FDB_ICEBERG_TABLE", "fdb.iceberg.table", "cell_kpi")),
resolveString(env, properties, "FDB_ICEBERG_CELL_ANOMALY_TABLE",
    "fdb.iceberg.cell.anomaly.table", "cell_anomaly_events"),
resolveString(env, properties, "FDB_ICEBERG_GRID_ANOMALY_TABLE",
    "fdb.iceberg.grid.anomaly.table", "grid_anomaly_events")
```

Keep `FDB_ICEBERG_TABLE` fallback for compatibility.

- [ ] **Step 7: Add Iceberg anomaly mapper**

Create `flink-job/src/main/java/com/fdb/job/sink/AnomalyEventIcebergMapper.java`:

```java
package com.fdb.job.sink;

import com.fdb.common.avro.AnomalyEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public final class AnomalyEventIcebergMapper implements MapFunction<AnomalyEvent, RowData> {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter
        .ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter HOUR_FORMATTER = DateTimeFormatter
        .ofPattern("HH").withZone(ZoneOffset.UTC);

    @Override
    public RowData map(AnomalyEvent event) {
        Instant eventTime = Instant.ofEpochMilli(event.getEventTs());
        GenericRowData row = new GenericRowData(13);
        row.setField(0, event.getDetectionTs());
        row.setField(1, event.getEventTs());
        row.setField(2, string(event.getSiteId()));
        row.setField(3, string(event.getCellId()));
        row.setField(4, string(event.getGridId()));
        row.setField(5, event.getLatitude());
        row.setField(6, event.getLongitude());
        row.setField(7, string(event.getAnomalyType()));
        row.setField(8, string(event.getSeverity()));
        row.setField(9, string(event.getRuleVersion()));
        row.setField(10, string(event.getContextJson()));
        row.setField(11, string(DATE_FORMATTER.format(eventTime)));
        row.setField(12, string(HOUR_FORMATTER.format(eventTime)));
        return row;
    }

    private static StringData string(Object value) {
        return StringData.fromString(value == null ? "" : value.toString());
    }
}
```

- [ ] **Step 8: Extend `IcebergSinks` table identifiers**

Add:

```java
static TableIdentifier cellKpiIdentifier(IcebergConfig config) {
    return TableIdentifier.of(config.database(), config.table());
}

static TableIdentifier cellAnomalyIdentifier(IcebergConfig config) {
    return TableIdentifier.of(config.database(), config.cellAnomalyTable());
}

static TableIdentifier gridAnomalyIdentifier(IcebergConfig config) {
    return TableIdentifier.of(config.database(), config.gridAnomalyTable());
}
```

Keep existing `tableIdentifier` delegating to `cellKpiIdentifier` for test compatibility:

```java
static TableIdentifier tableIdentifier(IcebergConfig config) {
    return cellKpiIdentifier(config);
}
```

- [ ] **Step 9: Add Iceberg anomaly schema and sink append method**

In `IcebergSinks`, add anomaly schema:

```java
static Schema anomalySchema() {
    return new Schema(
        Types.NestedField.required(1, "detection_ts", Types.LongType.get()),
        Types.NestedField.required(2, "event_ts", Types.LongType.get()),
        Types.NestedField.required(3, "site_id", Types.StringType.get()),
        Types.NestedField.required(4, "cell_id", Types.StringType.get()),
        Types.NestedField.required(5, "grid_id", Types.StringType.get()),
        Types.NestedField.required(6, "latitude", Types.DoubleType.get()),
        Types.NestedField.required(7, "longitude", Types.DoubleType.get()),
        Types.NestedField.required(8, "anomaly_type", Types.StringType.get()),
        Types.NestedField.required(9, "severity", Types.StringType.get()),
        Types.NestedField.required(10, "rule_version", Types.StringType.get()),
        Types.NestedField.required(11, "context_json", Types.StringType.get()),
        Types.NestedField.required(12, "dt", Types.StringType.get()),
        Types.NestedField.required(13, "hour", Types.StringType.get()));
}

static PartitionSpec anomalyPartitionSpec(Schema schema) {
    return PartitionSpec.builderFor(schema).identity("dt").identity("hour").build();
}
```

Add a helper:

```java
static Table ensureTable(IcebergConfig config, TableIdentifier identifier, Schema schema, PartitionSpec spec) {
    HiveCatalog catalog = hiveCatalog(config);
    Namespace namespace = Namespace.of(config.database());
    try {
        catalog.createNamespace(namespace, Map.of("location", config.warehouse() + "/" + config.database()));
    } catch (AlreadyExistsException ignored) {
    }
    if (catalog.tableExists(identifier)) {
        return catalog.loadTable(identifier);
    }
    return catalog.createTable(identifier, schema, spec, tableProperties());
}
```

Refactor existing `ensureTable(config)` to call this helper for `cellKpiIdentifier(config)`.

Add:

```java
public static void appendBusinessResultSinks(DataStream<CellKpi> kpi1m,
                                             DataStream<CellKpi> kpi5m,
                                             DataStream<AnomalyEvent> cellAnomalies,
                                             DataStream<AnomalyEvent> gridAnomalies,
                                             IcebergConfig config) {
    DataStream<RowData> icebergKpi1m = kpi1m
        .process(new SinkLatencyProbe<>("iceberg-kpi-1m", "Cell KPI 1m Iceberg Sink",
            "iceberg", "kpi_1m", "MIN_1", 100), new GenericTypeInfo<>(CellKpi.class))
        .startNewChain().name("iceberg-kpi-1m")
        .map(new CellKpiIcebergMapper()).returns(new GenericTypeInfo<>(RowData.class))
        .name("cell-kpi-iceberg-map");
    DataStream<RowData> icebergKpi5m = kpi5m
        .process(new SinkLatencyProbe<>("iceberg-kpi-5m", "Cell KPI 5m Iceberg Sink",
            "iceberg", "kpi_5m", "MIN_5", 100), new GenericTypeInfo<>(CellKpi.class))
        .startNewChain().name("iceberg-kpi-5m")
        .map(new CellKpiIcebergMapper()).returns(new GenericTypeInfo<>(RowData.class))
        .name("cell-kpi-5m-iceberg-map");
    appendRowDataSink(icebergKpi1m.union(icebergKpi5m), config, cellKpiIdentifier(config),
        cellKpiSchema(), cellKpiPartitionSpec(cellKpiSchema()), "cell-kpi-iceberg-sink");

    DataStream<RowData> cellAnomalyRows = cellAnomalies
        .process(new SinkLatencyProbe<>("iceberg-cell-anomaly", "Cell Anomaly Iceberg Sink",
            "iceberg", "cell_anomaly_events", "ANOMALY", 100), new GenericTypeInfo<>(AnomalyEvent.class))
        .startNewChain().name("iceberg-cell-anomaly")
        .map(new AnomalyEventIcebergMapper()).returns(new GenericTypeInfo<>(RowData.class))
        .name("cell-anomaly-iceberg-map");
    appendRowDataSink(cellAnomalyRows, config, cellAnomalyIdentifier(config),
        anomalySchema(), anomalyPartitionSpec(anomalySchema()), "cell-anomaly-iceberg-sink");

    DataStream<RowData> gridAnomalyRows = gridAnomalies
        .process(new SinkLatencyProbe<>("iceberg-grid-anomaly", "Grid Anomaly Iceberg Sink",
            "iceberg", "grid_anomaly_events", "ANOMALY", 100), new GenericTypeInfo<>(AnomalyEvent.class))
        .startNewChain().name("iceberg-grid-anomaly")
        .map(new AnomalyEventIcebergMapper()).returns(new GenericTypeInfo<>(RowData.class))
        .name("grid-anomaly-iceberg-map");
    appendRowDataSink(gridAnomalyRows, config, gridAnomalyIdentifier(config),
        anomalySchema(), anomalyPartitionSpec(anomalySchema()), "grid-anomaly-iceberg-sink");
}
```

Add private sink helper:

```java
private static DataStreamSink<Void> appendRowDataSink(DataStream<RowData> stream, IcebergConfig config,
                                                      TableIdentifier identifier, Schema schema,
                                                      PartitionSpec spec, String name) {
    ensureTable(config, identifier, schema, spec);
    CatalogLoader catalogLoader = CatalogLoader.hive(
        config.catalogName(), new Configuration(), catalogProperties(config));
    TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);
    return FlinkSink.forRowData(stream).tableLoader(tableLoader).append().name(name);
}
```

Add imports:

```java
import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.CellKpi;
import com.fdb.job.metrics.SinkLatencyProbe;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
```

- [ ] **Step 10: Run sink tests and package compile**

Run:

```powershell
mvn -pl flink-job -Dtest=ResultSinksTest,HiveSinksTest,IcebergSinksTest,IcebergConfigTest,FlinkJobMainTest test
```

Expected: PASS.

- [ ] **Step 11: Commit together with Task 3**

Run the commit command from Task 3 Step 6.

---

### Task 5: Make Metrics Lightweight, Toggleable, And Run-Aware

**Files:**
- Modify: `common/src/main/java/com/fdb/common/metrics/StageMetricSample.java`
- Create: `common/src/test/java/com/fdb/common/metrics/StageMetricSampleTest.java`
- Modify: `flink-job/src/main/java/com/fdb/job/metrics/MetricSamplePublisher.java`
- Modify: `flink-job/src/main/java/com/fdb/job/metrics/StageMetricsProbe.java`
- Modify: `flink-job/src/main/java/com/fdb/job/metrics/SinkLatencyProbe.java`
- Modify: `flink-job/src/test/java/com/fdb/job/metrics/StageMetricsProbeTest.java`
- Modify: `flink-job/src/test/java/com/fdb/job/metrics/SinkLatencyProbeTest.java`

- [ ] **Step 1: Add common metrics JSON compatibility test**

Create `common/src/test/java/com/fdb/common/metrics/StageMetricSampleTest.java`:

```java
package com.fdb.common.metrics;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StageMetricSampleTest {

    @Test
    void parses_legacy_json_without_run_metadata() {
        StageMetricSample sample = StageMetricSample.fromJson("""
            {"stageId":"kafka-kpi-1m","displayName":"Kafka","status":"healthy",
             "inEps":1.0,"outEps":1.0,"latencyP95Ms":2,"watermarkLagMs":0,
             "errorCount":0,"rowsWritten":3,"rebalanceTotal":0,"source":"",
             "sink":"kafka","window":"1m","sinkType":"kafka","dataset":"kpi_1m",
             "windowKind":"MIN_1","records":3,"bytes":120,"durationMs":10,
             "latencyP50Ms":1,"latencyP99Ms":3,"failureCount":0,"errorMessage":"",
             "checkpointId":-1,"updatedAtEpochMs":1000}
            """);

        assertThat(sample.runId()).isEqualTo("unknown-run");
        assertThat(sample.resultSink()).isEqualTo("");
        assertThat(sample.parallelism()).isEqualTo(-1);
    }

    @Test
    void serializes_run_metadata() {
        StageMetricSample sample = StageMetricSample.sinkLatency(
            "starrocks-kpi-1m", "StarRocks", "healthy", "starrocks",
            "kpi_1m", "MIN_1", 10, 100, 20, 1, 2, 3, 0, "", -1, 1000)
            .withRunMetadata("run-a", "starrocks", 4);

        StageMetricSample parsed = StageMetricSample.fromJson(sample.toJson());

        assertThat(parsed.runId()).isEqualTo("run-a");
        assertThat(parsed.resultSink()).isEqualTo("starrocks");
        assertThat(parsed.parallelism()).isEqualTo(4);
    }
}
```

- [ ] **Step 2: Run common test and verify failure**

Run:

```powershell
mvn -pl common -Dtest=StageMetricSampleTest test
```

Expected: FAIL because metadata fields and `withRunMetadata` do not exist.

- [ ] **Step 3: Extend `StageMetricSample`**

Change the record tail from:

```java
String errorMessage,
long checkpointId,
long updatedAtEpochMs
```

to:

```java
String errorMessage,
long checkpointId,
String runId,
String resultSink,
int parallelism,
long updatedAtEpochMs
```

Add constructor defaults:

```java
runId = blankToDefault(runId, "unknown-run");
resultSink = blankToDefault(resultSink, "");
```

Add `withRunMetadata`:

```java
public StageMetricSample withRunMetadata(String runId, String resultSink, int parallelism) {
    return new StageMetricSample(stageId, displayName, status, inEps, outEps, latencyP95Ms,
        watermarkLagMs, errorCount, rowsWritten, rebalanceTotal, source, sink, window, sinkType,
        dataset, windowKind, records, bytes, durationMs, latencyP50Ms, latencyP99Ms,
        failureCount, errorMessage, checkpointId, runId, resultSink, parallelism,
        updatedAtEpochMs);
}
```

Update `fromJson` to backfill missing fields:

```java
if (node instanceof ObjectNode object) {
    if (!object.hasNonNull("checkpointId")) {
        object.put("checkpointId", -1L);
    }
    if (!object.hasNonNull("runId")) {
        object.put("runId", "unknown-run");
    }
    if (!object.hasNonNull("resultSink")) {
        object.put("resultSink", "");
    }
    if (!object.hasNonNull("parallelism")) {
        object.put("parallelism", -1);
    }
}
```

Update all static factory methods to pass `"unknown-run"`, `""`, `-1`.

- [ ] **Step 4: Update `MetricSamplePublisher` for metrics enabled switch**

Modify constructor:

```java
private final boolean enabled;

public MetricSamplePublisher() {
    enabled = !"false".equalsIgnoreCase(System.getenv().getOrDefault("FDB_METRICS_ENABLED", "true"));
    String bootstrap = System.getenv().getOrDefault("FDB_KAFKA_BOOTSTRAP", "localhost:9092");
    topic = System.getenv().getOrDefault("FDB_METRICS_TOPIC", "fdb-stage-metrics");
    if (!enabled) {
        producer = null;
        return;
    }
    ...
}
```

Modify `publish`:

```java
public void publish(StageMetricSample sample) {
    if (!enabled || producer == null) {
        return;
    }
    producer.send(new ProducerRecord<>(topic, sample.stageId(), sample.toJson()));
}
```

Modify `close`:

```java
if (producer != null) {
    producer.flush();
    producer.close();
}
```

- [ ] **Step 5: Attach run metadata in probes**

In `StageMetricsProbe` and `SinkLatencyProbe`, add transient or final fields:

```java
private final String runId = System.getenv().getOrDefault("FDB_RUN_ID", "unknown-run");
private final String resultSink = System.getenv().getOrDefault("FDB_RESULT_SINK", "");
private final int parallelism = parseInt(System.getenv().get("FDB_FLINK_PARALLELISM"), -1);
```

Add helper:

```java
private static int parseInt(String value, int defaultValue) {
    if (value == null || value.isBlank()) {
        return defaultValue;
    }
    try {
        return Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
        return defaultValue;
    }
}
```

Call:

```java
sample.withRunMetadata(runId, resultSink, parallelism)
```

before publishing.

- [ ] **Step 6: Run metrics tests**

Run:

```powershell
mvn -pl common,flink-job -Dtest=StageMetricSampleTest,StageMetricsProbeTest,SinkLatencyProbeTest test
```

Expected: PASS.

- [ ] **Step 7: Commit**

Run:

```powershell
npx gitnexus detect_changes --repo flink-data-balance --scope unstaged
git add common/src/main/java common/src/test/java flink-job/src/main/java/com/fdb/job/metrics flink-job/src/test/java/com/fdb/job/metrics
git commit -m "feat(metrics): tag runtime samples with benchmark metadata"
```

---

### Task 6: Persist Metrics History And Generate Benchmark Reports In Observability API

**Files:**
- Create: `observability-api/src/main/java/com/fdb/observability/model/RuntimeConfig.java`
- Create: `observability-api/src/main/java/com/fdb/observability/model/ReportStatus.java`
- Create: `observability-api/src/main/java/com/fdb/observability/service/MetricsHistoryService.java`
- Create: `observability-api/src/main/java/com/fdb/observability/service/BenchmarkReportService.java`
- Modify: `observability-api/src/main/java/com/fdb/observability/service/StageMetricKafkaConsumer.java`
- Modify: `observability-api/src/main/java/com/fdb/observability/service/ObservabilitySnapshotService.java`
- Modify: `observability-api/src/main/java/com/fdb/observability/ObservabilityApiMain.java`
- Test: `observability-api/src/test/java/com/fdb/observability/service/MetricsHistoryServiceTest.java`
- Test: `observability-api/src/test/java/com/fdb/observability/service/BenchmarkReportServiceTest.java`
- Test: `observability-api/src/test/java/com/fdb/observability/ObservabilityApiMainTest.java`

- [ ] **Step 1: Write history service test**

Create `observability-api/src/test/java/com/fdb/observability/service/MetricsHistoryServiceTest.java`:

```java
package com.fdb.observability.service;

import com.fdb.common.metrics.StageMetricSample;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class MetricsHistoryServiceTest {
  @TempDir Path tempDir;

  @Test
  void appends_metric_samples_to_run_jsonl() throws Exception {
    MetricsHistoryService service = new MetricsHistoryService(tempDir, true);
    StageMetricSample sample = StageMetricSample.stage(
        "chr-source", "CHR Source", "healthy", 1.0, 1.0, 0, 0, 0, 1000)
        .withRunMetadata("run-a", "starrocks", 4);

    service.append(sample);

    Path file = tempDir.resolve("run-a").resolve("metrics.jsonl");
    assertThat(file).exists();
    assertThat(Files.readString(file)).contains("\"stageId\":\"chr-source\"");
  }

  @Test
  void disabled_history_does_not_write_files() throws Exception {
    MetricsHistoryService service = new MetricsHistoryService(tempDir, false);
    service.append(StageMetricSample.stage("chr-source", "CHR Source", "healthy",
        1.0, 1.0, 0, 0, 0, 1000).withRunMetadata("run-a", "starrocks", 4));

    assertThat(tempDir.resolve("run-a")).doesNotExist();
  }
}
```

- [ ] **Step 2: Write benchmark report test**

Create `observability-api/src/test/java/com/fdb/observability/service/BenchmarkReportServiceTest.java`:

```java
package com.fdb.observability.service;

import com.fdb.common.metrics.StageMetricSample;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class BenchmarkReportServiceTest {
  @TempDir Path tempDir;

  @Test
  void generates_markdown_report_from_metrics_jsonl() throws Exception {
    MetricsHistoryService history = new MetricsHistoryService(tempDir, true);
    history.append(StageMetricSample.sinkLatency("starrocks-kpi-1m", "StarRocks KPI", "healthy",
        "starrocks", "kpi_1m", "MIN_1", 100, 1024, 1000, 5, 10, 20, 0, "", -1, 1000)
        .withRunMetadata("run-a", "starrocks", 4));

    BenchmarkReportService report = new BenchmarkReportService(tempDir);
    Path reportPath = report.generate("run-a");

    assertThat(reportPath).exists();
    String markdown = Files.readString(reportPath);
    assertThat(markdown).contains("# Benchmark Report: run-a");
    assertThat(markdown).contains("starrocks-kpi-1m");
    assertThat(markdown).contains("records");
  }
}
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```powershell
mvn -pl observability-api -Dtest=MetricsHistoryServiceTest,BenchmarkReportServiceTest test
```

Expected: FAIL because services do not exist.

- [ ] **Step 4: Create `MetricsHistoryService`**

Create `observability-api/src/main/java/com/fdb/observability/service/MetricsHistoryService.java`:

```java
package com.fdb.observability.service;

import com.fdb.common.metrics.StageMetricSample;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class MetricsHistoryService {
  private final Path runsRoot;
  private final boolean enabled;

  public MetricsHistoryService(Path runsRoot, boolean enabled) {
    this.runsRoot = runsRoot;
    this.enabled = enabled;
  }

  public void append(StageMetricSample sample) {
    if (!enabled) {
      return;
    }
    String runId = sanitizeRunId(sample.runId());
    Path runDir = runsRoot.resolve(runId);
    Path metricsFile = runDir.resolve("metrics.jsonl");
    try {
      Files.createDirectories(runDir);
      Files.writeString(metricsFile, sample.toJson() + System.lineSeparator(), StandardCharsets.UTF_8,
          StandardOpenOption.CREATE, StandardOpenOption.APPEND);
      Path runFile = runDir.resolve("run.json");
      if (!Files.exists(runFile)) {
        Files.writeString(runFile, "{\"runId\":\"" + runId + "\",\"resultSink\":\""
            + sample.resultSink() + "\",\"parallelism\":" + sample.parallelism() + "}",
            StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static String sanitizeRunId(String runId) {
    String value = runId == null || runId.isBlank() ? "unknown-run" : runId.trim();
    return value.replaceAll("[^A-Za-z0-9._-]", "_");
  }
}
```

- [ ] **Step 5: Create `BenchmarkReportService`**

Create `observability-api/src/main/java/com/fdb/observability/service/BenchmarkReportService.java`:

```java
package com.fdb.observability.service;

import com.fdb.common.metrics.StageMetricSample;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class BenchmarkReportService {
  private final Path runsRoot;

  public BenchmarkReportService(Path runsRoot) {
    this.runsRoot = runsRoot;
  }

  public Path generate(String runId) {
    String sanitized = MetricsHistoryService.sanitizeRunId(runId);
    Path runDir = runsRoot.resolve(sanitized);
    Path metricsFile = runDir.resolve("metrics.jsonl");
    Path reportFile = runDir.resolve("report.md");
    List<StageMetricSample> samples = readSamples(metricsFile);
    String markdown = render(sanitized, samples);
    try {
      Files.createDirectories(runDir);
      Files.writeString(reportFile, markdown, StandardCharsets.UTF_8,
          StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
      return reportFile;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static List<StageMetricSample> readSamples(Path metricsFile) {
    if (!Files.exists(metricsFile)) {
      return List.of();
    }
    try {
      List<StageMetricSample> samples = new ArrayList<>();
      for (String line : Files.readAllLines(metricsFile, StandardCharsets.UTF_8)) {
        if (!line.isBlank()) {
          samples.add(StageMetricSample.fromJson(line));
        }
      }
      return samples;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String render(String runId, List<StageMetricSample> samples) {
    StringBuilder out = new StringBuilder();
    out.append("# Benchmark Report: ").append(runId).append("\n\n");
    out.append("## Summary\n\n");
    out.append("- Metric samples: ").append(samples.size()).append("\n");
    samples.stream().findFirst().ifPresent(sample -> {
      out.append("- Result sink: ").append(sample.resultSink()).append("\n");
      out.append("- Parallelism: ").append(sample.parallelism()).append("\n");
    });
    out.append("\n## Sink Metrics\n\n");
    out.append("| Stage | Dataset | Window | Records | Bytes | P95 ms | Failures |\n");
    out.append("|---|---|---:|---:|---:|---:|---:|\n");
    latestByStage(samples).values().stream()
        .filter(sample -> !sample.sinkType().isBlank())
        .sorted(Comparator.comparing(StageMetricSample::stageId))
        .forEach(sample -> out.append("| ")
            .append(sample.stageId()).append(" | ")
            .append(sample.dataset()).append(" | ")
            .append(sample.windowKind()).append(" | ")
            .append(sample.records()).append(" | ")
            .append(sample.bytes()).append(" | ")
            .append(sample.latencyP95Ms()).append(" | ")
            .append(sample.failureCount()).append(" |\n"));
    out.append("\n## Bottleneck Candidates\n\n");
    latestByStage(samples).values().stream()
        .filter(sample -> sample.failureCount() > 0 || sample.latencyP95Ms() > 1_000)
        .forEach(sample -> out.append("- ").append(sample.stageId())
            .append(": p95=").append(sample.latencyP95Ms())
            .append("ms failures=").append(sample.failureCount()).append("\n"));
    if (!out.toString().contains("p95=")) {
      out.append("- No automatic bottleneck candidate crossed the default thresholds.\n");
    }
    return out.toString();
  }

  private static Map<String, StageMetricSample> latestByStage(List<StageMetricSample> samples) {
    Map<String, StageMetricSample> latest = new LinkedHashMap<>();
    samples.stream().sorted(Comparator.comparingLong(StageMetricSample::updatedAtEpochMs))
        .forEach(sample -> latest.put(sample.stageId(), sample));
    return latest;
  }
}
```

- [ ] **Step 6: Wire history service into Kafka consumer**

Change `StageMetricKafkaConsumer` constructor to accept history:

```java
private final MetricsHistoryService historyService;

public StageMetricKafkaConsumer(String bootstrap, String topic, ObservabilitySnapshotService service,
                                MetricsHistoryService historyService) {
    this.service = service;
    this.historyService = historyService;
    ...
}
```

Change poll loop:

```java
StageMetricSample sample = StageMetricSample.fromJson(record.value());
service.applyMetricSample(sample);
historyService.append(sample);
```

Keep an overload for existing tests:

```java
public StageMetricKafkaConsumer(String bootstrap, String topic, ObservabilitySnapshotService service) {
    this(bootstrap, topic, service, new MetricsHistoryService(Path.of("docker/data/observability-runs"), false));
}
```

- [ ] **Step 7: Add runtime config model**

Create `observability-api/src/main/java/com/fdb/observability/model/RuntimeConfig.java`:

```java
package com.fdb.observability.model;

public record RuntimeConfig(
    boolean dynamicBalancingEnabled,
    String resultSink,
    boolean dlqEnabled,
    boolean metricsEnabled,
    boolean metricsHistoryEnabled,
    String runId,
    String runLabel,
    int parallelism,
    long checkpointIntervalMs,
    String jobStatus,
    String reportStatus
) {}
```

Create `observability-api/src/main/java/com/fdb/observability/model/ReportStatus.java`:

```java
package com.fdb.observability.model;

public record ReportStatus(String runId, String status, String path) {}
```

- [ ] **Step 8: Add runtime config to snapshot service**

Add to `ObservabilitySnapshotService`:

```java
public RuntimeConfig runtimeConfig() {
    return new RuntimeConfig(
        dynamicBalancingEnabled,
        env("FDB_RESULT_SINK", "starrocks"),
        boolEnv("FDB_DLQ_ENABLED", true),
        boolEnv("FDB_METRICS_ENABLED", true),
        boolEnv("FDB_METRICS_HISTORY_ENABLED", true),
        env("FDB_RUN_ID", "unknown-run"),
        env("FDB_RUN_LABEL", ""),
        intEnv("FDB_FLINK_PARALLELISM", 4),
        longEnv("FDB_FLINK_CHECKPOINT_INTERVAL_MS", 30_000L),
        "unknown",
        "collecting");
}
```

Add private helpers for env parsing in the same class.

- [ ] **Step 9: Wire API endpoints**

In `ObservabilityApiMain.main`, create:

```java
Path runsRoot = Path.of(System.getenv().getOrDefault("FDB_RUN_HISTORY_DIR", "/observability-runs"));
MetricsHistoryService historyService = new MetricsHistoryService(runsRoot,
    !"false".equalsIgnoreCase(System.getenv().getOrDefault("FDB_METRICS_HISTORY_ENABLED", "true")));
BenchmarkReportService reportService = new BenchmarkReportService(runsRoot);
StageMetricKafkaConsumer metricConsumer = new StageMetricKafkaConsumer(bootstrap, metricsTopic, service, historyService);
```

Extend `createServer` overloads to accept `BenchmarkReportService`. Add contexts:

```java
server.createContext("/api/flow/runtime", exchange -> writeJson(exchange, service.runtimeConfig()));
server.createContext("/api/runs/report", exchange -> {
    String runId = queryParameters(exchange).getOrDefault("runId", service.runtimeConfig().runId());
    Path reportPath = reportService.generate(runId);
    writeJson(exchange, new ReportStatus(runId, "ready", reportPath.toString()));
});
```

- [ ] **Step 10: Run observability API tests**

Run:

```powershell
mvn -pl observability-api -Dtest=MetricsHistoryServiceTest,BenchmarkReportServiceTest,ObservabilityApiMainTest,ObservabilitySnapshotServiceTest test
```

Expected: PASS.

- [ ] **Step 11: Commit**

Run:

```powershell
npx gitnexus detect_changes --repo flink-data-balance --scope unstaged
git add observability-api/src/main/java observability-api/src/test/java
git commit -m "feat(api): persist metrics history and reports"
```

---

### Task 7: Add Deploy Report Command And Environment Documentation

**Files:**
- Modify: `scripts/deploy.sh`
- Modify: `README.md`
- Modify: `.env.example.local` if present
- Modify: `.env.example.external-yarn` if present

- [ ] **Step 1: Inspect existing deploy command dispatch**

Run:

```powershell
rg -n "case|check|submit|smoke|stop|usage|COMMAND" scripts/deploy.sh
```

Expected: identify the command dispatch block for adding `report`.

- [ ] **Step 2: Add shell syntax test target**

No new test file is needed. The failing verification before edit is:

```powershell
bash scripts/deploy.sh local report
```

Expected: exits non-zero or prints unknown command.

- [ ] **Step 3: Add `report` command**

In `scripts/deploy.sh`, add `report` to usage text for local and external-yarn.

Add function:

```bash
run_report() {
  load_env_file
  local run_id="${FDB_RUN_ID:-}"
  if [ -z "$run_id" ] && [ -f logs/${TARGET}-current.env ]; then
    run_id="$(grep '^FDB_RUN_ID=' "logs/${TARGET}-current.env" | tail -n 1 | cut -d= -f2- || true)"
  fi
  if [ -z "$run_id" ]; then
    run_id="$(date -u +%Y%m%d-%H%M%S)"
  fi
  local api_url="${FDB_OBSERVABILITY_API_URL:-http://localhost:18080}"
  curl -fsS "${api_url}/api/runs/report?runId=${run_id}"
  echo
}
```

In command dispatch:

```bash
report)
  run_report
  ;;
```

When `stop` completes, add:

```bash
if [ "${FDB_REPORT_ON_STOP:-false}" = "true" ]; then
  run_report || true
fi
```

- [ ] **Step 4: Add run env persistence**

Where submit records current env to `logs/local-current.env` or `logs/external-yarn-current.env`, append:

```bash
FDB_RUN_ID=${FDB_RUN_ID:-$(date -u +%Y%m%d-%H%M%S)}
FDB_RESULT_SINK=${FDB_RESULT_SINK:-starrocks}
```

Ensure the same values are exported into Flink job submission env.

- [ ] **Step 5: Update README**

Add a short section:

```markdown
### Sink Benchmarking

Set one business result sink per run:

```bash
FDB_RESULT_SINK=starrocks FDB_RUN_LABEL=starrocks-p4 bash scripts/deploy.sh local submit
FDB_RESULT_SINK=iceberg FDB_FLINK_CHECKPOINT_INTERVAL_MS=30000 bash scripts/deploy.sh local submit
FDB_RESULT_SINK=hive FDB_FLINK_CHECKPOINT_INTERVAL_MS=30000 bash scripts/deploy.sh local submit
FDB_RESULT_SINK=kafka bash scripts/deploy.sh local submit
FDB_RESULT_SINK=none bash scripts/deploy.sh local submit
```

Generate a report:

```bash
bash scripts/deploy.sh local report
```

Reports are stored under `docker/data/observability-runs/<runId>/report.md`.
```

- [ ] **Step 6: Update env examples**

Add these lines to env examples that exist:

```dotenv
FDB_RESULT_SINK=starrocks
FDB_DLQ_ENABLED=true
FDB_METRICS_ENABLED=true
FDB_METRICS_EMIT_INTERVAL_MS=5000
FDB_METRICS_HISTORY_ENABLED=true
FDB_RUN_ID=
FDB_RUN_LABEL=
FDB_REPORT_ON_STOP=false
FDB_FILE_SINK_ROLLOVER_INTERVAL_MS=600000
FDB_FILE_SINK_INACTIVITY_INTERVAL_MS=300000
FDB_FILE_SINK_MAX_PART_SIZE_BYTES=134217728
```

- [ ] **Step 7: Verify script syntax**

Run:

```powershell
bash -n scripts/deploy.sh
```

Expected: no output and exit code 0.

- [ ] **Step 8: Commit**

Run:

```powershell
npx gitnexus detect_changes --repo flink-data-balance --scope unstaged
git add scripts/deploy.sh README.md .env.example.local .env.example.external-yarn
git commit -m "feat(deploy): add benchmark report command"
```

Before `git add`, run `Test-Path .env.example.local` and `Test-Path .env.example.external-yarn`; add only files that exist.

---

### Task 8: Update Frontend Runtime Overview For Actual Sink Topology

**Files:**
- Modify: `frontend/src/types/observability.ts`
- Modify: `frontend/src/api/client.ts`
- Modify: `frontend/src/pages/FlowOverview.tsx`
- Modify: `frontend/src/components/StreamingFlowGraph.tsx`
- Modify: `frontend/src/components/flowEdges.ts`
- Modify: `frontend/src/components/flowEdges.test.ts`
- Modify: `frontend/src/App.test.tsx`

- [ ] **Step 1: Write flow edge tests**

Update `frontend/src/components/flowEdges.test.ts` with:

```ts
import { describe, expect, it } from 'vitest';
import { resolveFlowEdges } from './flowEdges';

describe('resolveFlowEdges', () => {
  it('only connects starrocks sink nodes when starrocks is active', () => {
    const edges = resolveFlowEdges([
      'chr-source', 'pm-source', 'cfg-source', 'kafka', 'enrichment',
      'starrocks-kpi-1m', 'starrocks-kpi-5m', 'starrocks-cell-anomaly', 'starrocks-grid-anomaly'
    ]);

    expect(edges).toContainEqual(['enrichment', 'starrocks-kpi-1m']);
    expect(edges).not.toContainEqual(['enrichment', 'iceberg-kpi-1m']);
    expect(edges).not.toContainEqual(['enrichment', 'hive-kpi-1m']);
  });

  it('does not create sink edges in none mode', () => {
    const edges = resolveFlowEdges(['chr-source', 'pm-source', 'cfg-source', 'kafka', 'enrichment']);

    expect(edges).toEqual([
      ['chr-source', 'kafka'],
      ['pm-source', 'kafka'],
      ['cfg-source', 'kafka'],
      ['kafka', 'enrichment']
    ]);
  });
});
```

- [ ] **Step 2: Run frontend test and verify failure**

Run:

```powershell
npm --prefix frontend test -- flowEdges
```

Expected: FAIL because current `resolveFlowEdges` emits all sink edges.

- [ ] **Step 3: Update runtime config type**

In `frontend/src/types/observability.ts`, replace `RuntimeConfig` with:

```ts
export interface RuntimeConfig {
  dynamicBalancingEnabled: boolean;
  resultSink: 'starrocks' | 'iceberg' | 'hive' | 'kafka' | 'none' | string;
  dlqEnabled: boolean;
  metricsEnabled: boolean;
  metricsHistoryEnabled: boolean;
  runId: string;
  runLabel: string;
  parallelism: number;
  checkpointIntervalMs: number;
  jobStatus: string;
  reportStatus: 'collecting' | 'ready' | 'failed' | string;
}
```

- [ ] **Step 4: Add API client for runtime**

Keep `fetchRuntimeConfig`, but change path:

```ts
export function fetchRuntimeConfig(): Promise<RuntimeConfig> {
  return getJson<RuntimeConfig>('/api/flow/runtime');
}
```

- [ ] **Step 5: Fix flow edge filtering**

Replace `sinkEdges` handling in `frontend/src/components/flowEdges.ts` with:

```ts
const possibleSinkEdges: FlowEdge[] = [
  ['enrichment', 'kafka-kpi-1m'],
  ['enrichment', 'kafka-kpi-5m'],
  ['enrichment', 'starrocks-kpi-1m'],
  ['enrichment', 'starrocks-kpi-5m'],
  ['enrichment', 'hive-kpi-1m'],
  ['enrichment', 'hive-kpi-5m'],
  ['enrichment', 'iceberg-kpi-1m'],
  ['enrichment', 'iceberg-kpi-5m'],
  ['enrichment', 'kafka-cell-anomaly'],
  ['enrichment', 'kafka-grid-anomaly'],
  ['enrichment', 'starrocks-cell-anomaly'],
  ['enrichment', 'starrocks-grid-anomaly'],
  ['enrichment', 'hive-cell-anomaly'],
  ['enrichment', 'hive-grid-anomaly'],
  ['enrichment', 'iceberg-cell-anomaly'],
  ['enrichment', 'iceberg-grid-anomaly']
];

return [
  ...sourceEdges,
  ...(dynamicBalancingEnabled ? dynamicRoutingEdges : directRoutingEdges),
  ...possibleSinkEdges.filter(([source, target]) => ids.has(source) && ids.has(target))
];
```

- [ ] **Step 6: Update node positions**

In `StreamingFlowGraph.tsx`, add positions:

```ts
'hive-cell-anomaly': { x: 1922, y: 984, group: 'Sink' },
'hive-grid-anomaly': { x: 1922, y: 1198, group: 'Sink' },
'iceberg-cell-anomaly': { x: 1922, y: 984, group: 'Sink' },
'iceberg-grid-anomaly': { x: 1922, y: 1198, group: 'Sink' }
```

Keep existing fallback for any stage not explicitly positioned.

- [ ] **Step 7: Update FlowOverview run bar**

In `FlowOverview.tsx`, import `fetchRuntimeConfig` and add state:

```tsx
const [runtime, setRuntime] = useState<RuntimeConfig>();
```

Update refresh promise:

```tsx
Promise.all([fetchSourceSummaries(), fetchStageStatuses(), fetchSinkSummaries(), fetchRuntimeConfig()])
  .then(([nextSources, nextStages, nextSinks, nextRuntime]) => {
    ...
    setRuntime(nextRuntime);
  })
```

Add top row before source cards:

```tsx
{runtime ? (
  <Row gutter={[12, 12]}>
    {[
      ['Run ID', runtime.runId],
      ['Result Sink', runtime.resultSink],
      ['Metrics', runtime.metricsEnabled ? 'enabled' : 'disabled'],
      ['DLQ', runtime.dlqEnabled ? 'enabled' : 'disabled'],
      ['Parallelism', String(runtime.parallelism)],
      ['Checkpoint', `${runtime.checkpointIntervalMs} ms`],
      ['Job', runtime.jobStatus],
      ['Report', runtime.reportStatus]
    ].map(([label, value]) => (
      <Col xs={12} md={6} xl={3} key={label}>
        <Card size="small">
          <Typography.Text type="secondary">{label}</Typography.Text>
          <div style={{ fontWeight: 700, marginTop: 4 }}>{value}</div>
        </Card>
      </Col>
    ))}
  </Row>
) : null}
```

Add a compact bottleneck summary after graph:

```tsx
<Card size="small" title="瓶颈候选">
  <Space wrap>
    <Tag>Backpressure</Tag>
    <Tag>Checkpoint duration</Tag>
    <Tag>Sink P95</Tag>
    <Tag>Input lag</Tag>
    <Tag>Small files</Tag>
    <Tag>Failures / restarts</Tag>
  </Space>
</Card>
```

Add imports:

```tsx
import { Card, Tag } from 'antd';
import type { RuntimeConfig } from '../types/observability';
```

- [ ] **Step 8: Run frontend tests**

Run:

```powershell
npm --prefix frontend test
```

Expected: PASS.

- [ ] **Step 9: Commit**

Run:

```powershell
npx gitnexus detect_changes --repo flink-data-balance --scope unstaged
git add frontend/src
git commit -m "feat(frontend): show active benchmark topology"
```

---

### Task 9: End-To-End Verification And Docs Refresh

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/specs/2026-04-29-flink-data-balance-design.md` only if implementation changed a decision.

**Verification results from 2026-07-14 local run:**

- `mvn test`: PASS, reactor `BUILD SUCCESS`, 208 Java tests run with 0 failures/errors.
- `npm --prefix frontend test`: PASS, 3 files / 11 tests.
- `bash -n scripts/deploy.sh`, `scripts/test-deploy-dispatch.sh`, `scripts/test-e2e-summary-lib.sh`, `scripts/test-init-kafka-topics.sh`, `scripts/test-retention-maintenance.sh`: PASS.
- `docker compose -f docker/docker-compose.yml --profile e2e config`: PASS.
- `mvn -pl flink-job -am clean package -DskipTests`: PASS, produced `flink-job/target/flink-job-0.1.0-SNAPSHOT.jar`.
- Local StarRocks benchmark verification with shared infra running: `deploy.sh local submit` created run `run-20260714-194605-24404-31462`, Flink job `b01acd359f8ef69286f30de6a41e4a30` reached RUNNING with 60/60 tasks running; Flink plan contained StarRocks business sink nodes and no Hive/Iceberg/Kafka business sink nodes.
- `deploy.sh local report`: PASS, API returned `status=ready` and wrote `report.md`; validation job was then stopped and reached CANCELED.
- `npx gitnexus detect_changes --repo flink-data-balance --scope unstaged`: PASS, `No changes detected.`

- [x] **Step 1: Run Maven tests**

Run:

```powershell
mvn test
```

Expected: PASS.

- [x] **Step 2: Run frontend tests**

Run:

```powershell
npm --prefix frontend test
```

Expected: PASS.

- [x] **Step 3: Run deploy script syntax check**

Run:

```powershell
bash -n scripts/deploy.sh
```

Expected: no output, exit code 0.

- [x] **Step 4: Run docker compose config if compose changed**

Run only if `docker/docker-compose.yml` changed:

```powershell
docker compose -f docker/docker-compose.yml --profile e2e config
```

Expected: compose renders successfully.

- [x] **Step 5: Build latest Flink jar**

Run:

```powershell
mvn -pl flink-job -am clean package -DskipTests
```

Expected: `flink-job/target/flink-job-0.1.0-SNAPSHOT.jar` is produced.

- [x] **Step 6: Local smoke for one sink when shared infra is running**

When shared infra containers are running, run:

```powershell
$env:FDB_RESULT_SINK="starrocks"; bash scripts/deploy.sh local submit
Start-Sleep -Seconds 20
Invoke-RestMethod http://localhost:8081/jobs/overview | ConvertTo-Json -Depth 10
```

Expected: job is RUNNING and only selected business sink nodes appear in Flink UI plan. Stop the job after verification:

```powershell
bash scripts/deploy.sh local stop
```

- [x] **Step 7: Generate report**

Run:

```powershell
bash scripts/deploy.sh local report
```

Expected: API returns JSON with `status=ready` and a `report.md` path, or returns a clear error if observability-api is not running.

- [x] **Step 8: Final GitNexus detect_changes**

Run:

```powershell
npx gitnexus detect_changes --repo flink-data-balance --scope unstaged
```

Expected: risk matches touched areas. Review any HIGH or CRITICAL result before committing.

- [x] **Step 9: Commit final docs or verification fixes**

Run if there are final docs or small verification fixes:

```powershell
git add README.md docs/superpowers/specs/2026-04-29-flink-data-balance-design.md
git commit -m "docs: document sink benchmark workflow"
```

Omit the commit if no files changed.

---

## Self-Review

- Spec coverage: package split, single `FDB_RESULT_SINK`, DLQ switch, metrics history, benchmark report, frontend overview, Hive/Iceberg anomaly tables, checkpoint default 30s and max 180s are covered.
- Placeholder scan: no task uses unresolved placeholder language or an unspecified "add tests" instruction.
- Type consistency: `ResultSinkType`, `ResultSinkConfig`, `ResultSinks`, `MetricsHistoryService`, `BenchmarkReportService`, and `RuntimeConfig` names are used consistently across tasks.
- Known implementation checkpoint: Task 3 references sink helpers created in Task 4, so execute Task 3 and Task 4 as one compile checkpoint if using inline execution.
