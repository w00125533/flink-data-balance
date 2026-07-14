package com.fdb.observability.service;

import com.fdb.common.metrics.StageMetricSample;
import com.fdb.observability.model.MigrationEvent;
import com.fdb.observability.model.SinkLatencySummary;
import com.fdb.observability.model.SinkSummary;
import com.fdb.observability.model.SourceSummary;
import com.fdb.observability.model.StageStatus;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public final class ObservabilitySnapshotService {
  private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
  private final Map<String, StageMetricSample> samples = new ConcurrentHashMap<>();
  private final boolean dynamicBalancingEnabled;

  public ObservabilitySnapshotService() {
    this(resolveDynamicBalancingEnabled(System.getenv(), System.getProperties()));
  }

  ObservabilitySnapshotService(boolean dynamicBalancingEnabled) {
    this.dynamicBalancingEnabled = dynamicBalancingEnabled;
    seedKnownStages();
  }

  public void applyMetricSample(StageMetricSample sample) {
    samples.merge(sampleKey(sample), sample,
        (current, incoming) -> isUnknownSeed(current) || incoming.updatedAtEpochMs() >= current.updatedAtEpochMs()
            ? incoming : current);
  }

  public List<StageStatus> stageStatuses() {
    Map<String, StageMetricSample> latestByStage = new LinkedHashMap<>();
    samples.values().stream()
        .sorted(Comparator.comparingLong(StageMetricSample::updatedAtEpochMs))
        .forEach(sample -> latestByStage.put(sample.stageId(), sample));
    return latestByStage.values().stream()
        .sorted(Comparator.comparingInt(sample -> stageOrder().getOrDefault(sample.stageId(), 100)))
        .map(sample -> new StageStatus(
            sample.stageId(),
            sample.displayName(),
            sample.status(),
            sample.inEps(),
            sample.outEps(),
            sample.latencyP50Ms(),
            sample.latencyP95Ms(),
            sample.watermarkLagMs(),
            (int) Math.min(Integer.MAX_VALUE, sample.errorCount()),
            summary(sample),
            formatUpdatedAt(sample.updatedAtEpochMs())))
        .toList();
  }

  public List<SourceSummary> sourceSummaries() {
    return samples.values().stream()
        .filter(sample -> !sample.source().isBlank())
        .sorted(Comparator.comparing(StageMetricSample::source))
        .map(sample -> new SourceSummary(sample.source(), sample.status(), sample.outEps(), 0,
            sample.watermarkLagMs(), summary(sample), formatUpdatedAt(sample.updatedAtEpochMs())))
        .toList();
  }

  public List<MigrationEvent> migrationEvents() {
    StageMetricSample coordinator = samples.get("load-coordinator");
    if (coordinator == null || coordinator.rebalanceTotal() == 0L) {
      return List.of();
    }
    return List.of(new MigrationEvent(
        "rebalance-total",
        (int) coordinator.rebalanceTotal(),
        (int) coordinator.rebalanceTotal(),
        "Load coordinator has emitted rebalance decisions",
        "applied",
        formatUpdatedAt(coordinator.updatedAtEpochMs()),
        formatUpdatedAt(coordinator.updatedAtEpochMs()),
        List.of()));
  }

  public List<SinkSummary> sinkSummaries() {
    return samples.values().stream()
        .filter(sample -> !sample.sink().isBlank())
        .sorted(Comparator.comparing((StageMetricSample sample) -> sample.stageId())
            .thenComparing(StageMetricSample::window))
        .map(sample -> new SinkSummary(sample.stageId(), sample.window(), sample.status(),
            sample.rowsWritten(), sample.latencyP95Ms(), summary(sample), formatUpdatedAt(sample.updatedAtEpochMs())))
        .toList();
  }

  public List<SinkLatencySummary> sinkLatencySummaries() {
    return samples.values().stream()
        .filter(sample -> !sample.sink().isBlank())
        .sorted(Comparator.comparing((StageMetricSample sample) -> sample.stageId())
            .thenComparing(StageMetricSample::window))
        .map(sample -> new SinkLatencySummary(
            sample.stageId(),
            sample.sinkType(),
            sample.dataset(),
            sample.windowKind(),
            sample.records(),
            sample.bytes(),
            sample.durationMs(),
            sample.latencyP50Ms(),
            sample.latencyP95Ms(),
            sample.latencyP99Ms(),
            sample.failureCount(),
            sample.errorMessage(),
            sample.checkpointId(),
            formatUpdatedAt(sample.updatedAtEpochMs())))
        .toList();
  }

  public long rebalanceTotal() {
    StageMetricSample coordinator = samples.get("load-coordinator");
    return coordinator == null ? 0L : coordinator.rebalanceTotal();
  }

  public boolean dynamicBalancingEnabled() {
    return dynamicBalancingEnabled;
  }

  private void seedKnownStages() {
    long now = System.currentTimeMillis();
    List<StageMetricSample> defaults = new ArrayList<>();
    defaults.add(StageMetricSample.stage("chr-source", "CHR Source", "unknown", 0, 0, 0, 0, 0, now));
    defaults.add(StageMetricSample.stage("pm-source", "PM Source", "unknown", 0, 0, 0, 0, 0, now));
    defaults.add(StageMetricSample.stage("cfg-source", "CFG Source", "unknown", 0, 0, 0, 0, 0, now));
    defaults.add(StageMetricSample.stage("kafka", "Kafka Topics", "unknown", 0, 0, 0, 0, 0, now));
    if (dynamicBalancingEnabled) {
      defaults.add(StageMetricSample.stage("assigner", "VBucket Assigner", "unknown", 0, 0, 0, 0, 0, now));
    }
    defaults.add(StageMetricSample.stage("enrichment", "Enrichment Process", "unknown", 0, 0, 0, 0, 0, now));
    if (dynamicBalancingEnabled) {
      defaults.add(StageMetricSample.stage("load-coordinator", "Load Coordinator", "unknown", 0, 0, 0, 0, 0, now));
    }
    defaults.add(sinkDefault("kafka-kpi-1m", "Cell KPI 1m Kafka Sink", "kafka", "kpi_1m", "MIN_1", now));
    defaults.add(sinkDefault("starrocks-kpi-1m", "Cell KPI 1m StarRocks Sink", "starrocks", "kpi_1m", "MIN_1", now));
    defaults.add(sinkDefault("hive-kpi-1m", "Cell KPI 1m Hive Sink", "hive", "kpi_1m", "MIN_1", now));
    defaults.add(sinkDefault("iceberg-kpi-1m", "Cell KPI 1m Iceberg Sink", "iceberg", "kpi_1m", "MIN_1", now));
    defaults.add(sinkDefault("kafka-kpi-5m", "Cell KPI 5m Kafka Sink", "kafka", "kpi_5m", "MIN_5", now));
    defaults.add(sinkDefault("starrocks-kpi-5m", "Cell KPI 5m StarRocks Sink", "starrocks", "kpi_5m", "MIN_5", now));
    defaults.add(sinkDefault("hive-kpi-5m", "Cell KPI 5m Hive Sink", "hive", "kpi_5m", "MIN_5", now));
    defaults.add(sinkDefault("iceberg-kpi-5m", "Cell KPI 5m Iceberg Sink", "iceberg", "kpi_5m", "MIN_5", now));
    defaults.add(sinkDefault("kafka-cell-anomaly", "Cell Anomaly Kafka Sink", "kafka", "cell_anomaly_events", "ANOMALY", now));
    defaults.add(sinkDefault("kafka-grid-anomaly", "Grid Anomaly Kafka Sink", "kafka", "grid_anomaly_events", "ANOMALY", now));
    defaults.add(sinkDefault("starrocks-cell-anomaly", "Cell Anomaly StarRocks Sink", "starrocks", "cell_anomaly_events", "ANOMALY", now));
    defaults.add(sinkDefault("starrocks-grid-anomaly", "Grid Anomaly StarRocks Sink", "starrocks", "grid_anomaly_events", "ANOMALY", now));
    defaults.forEach(sample -> samples.put(sampleKey(sample), sample));
  }

  private Map<String, Integer> stageOrder() {
    Map<String, Integer> order = new LinkedHashMap<>();
    order.put("chr-source", 0);
    order.put("pm-source", 1);
    order.put("cfg-source", 2);
    order.put("kafka", 3);
    int next = 4;
    if (dynamicBalancingEnabled) {
      order.put("assigner", next++);
    }
    order.put("enrichment", next++);
    if (dynamicBalancingEnabled) {
      order.put("load-coordinator", next++);
    }
    order.put("kafka-kpi-1m", next++);
    order.put("starrocks-kpi-1m", next++);
    order.put("hive-kpi-1m", next++);
    order.put("iceberg-kpi-1m", next++);
    order.put("kafka-kpi-5m", next++);
    order.put("starrocks-kpi-5m", next++);
    order.put("hive-kpi-5m", next++);
    order.put("iceberg-kpi-5m", next++);
    order.put("kafka-cell-anomaly", next++);
    order.put("kafka-grid-anomaly", next++);
    order.put("starrocks-cell-anomaly", next++);
    order.put("starrocks-grid-anomaly", next);
    return order;
  }

  private static StageMetricSample sinkDefault(String stageId, String displayName, String sinkType,
                                               String dataset, String windowKind, long now) {
    return StageMetricSample.sinkLatency(stageId, displayName, "unknown", sinkType, dataset, windowKind,
        0L, 0L, 0L, 0L, 0L, 0L, 0L, "", -1L, now);
  }

  static boolean resolveDynamicBalancingEnabled(Map<String, String> env, Properties properties) {
    String configured = env.get("FDB_DYNAMIC_BALANCING_ENABLED");
    if (configured == null || configured.isBlank()) {
      configured = properties.getProperty("fdb.dynamic.balancing.enabled");
    }
    return configured != null && "true".equalsIgnoreCase(configured.trim());
  }

  private static String summary(StageMetricSample sample) {
    if ("unknown".equals(sample.status())) {
      return "Waiting for runtime metrics";
    }
    return sample.displayName() + " runtime metrics are being collected";
  }

  private static String formatUpdatedAt(long epochMs) {
    return ISO.format(Instant.ofEpochMilli(epochMs).atOffset(ZoneOffset.UTC));
  }

  private static String sampleKey(StageMetricSample sample) {
    if (!sample.sink().isBlank()) {
      return sample.stageId() + ":" + sample.sink() + ":" + sample.window();
    }
    return sample.stageId();
  }

  private static boolean isUnknownSeed(StageMetricSample sample) {
    return "unknown".equals(sample.status());
  }
}
