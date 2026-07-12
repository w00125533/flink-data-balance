package com.fdb.observability.service;

import com.fdb.common.metrics.StageMetricSample;
import com.fdb.observability.model.MigrationEvent;
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
import java.util.concurrent.ConcurrentHashMap;

public final class ObservabilitySnapshotService {
  private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
  private final Map<String, StageMetricSample> samples = new ConcurrentHashMap<>();

  public ObservabilitySnapshotService() {
    seedKnownStages();
  }

  public void applyMetricSample(StageMetricSample sample) {
    samples.put(sampleKey(sample), sample);
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
            Math.max(0L, sample.latencyP95Ms() / 3L),
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
        .sorted(Comparator.comparing((StageMetricSample sample) -> sample.sink())
            .thenComparing(StageMetricSample::window))
        .map(sample -> new SinkSummary(sample.sink(), sample.window(), sample.status(),
            sample.rowsWritten(), sample.latencyP95Ms(), summary(sample), formatUpdatedAt(sample.updatedAtEpochMs())))
        .toList();
  }

  public long rebalanceTotal() {
    StageMetricSample coordinator = samples.get("load-coordinator");
    return coordinator == null ? 0L : coordinator.rebalanceTotal();
  }

  private void seedKnownStages() {
    long now = System.currentTimeMillis();
    List<StageMetricSample> defaults = new ArrayList<>();
    defaults.add(StageMetricSample.stage("chr-source", "CHR Source", "unknown", 0, 0, 0, 0, 0, now));
    defaults.add(StageMetricSample.stage("mr-source", "MR Source", "unknown", 0, 0, 0, 0, 0, now));
    defaults.add(StageMetricSample.stage("cm-source", "CM Source", "unknown", 0, 0, 0, 0, 0, now));
    defaults.add(StageMetricSample.stage("kafka", "Kafka Topics", "unknown", 0, 0, 0, 0, 0, now));
    defaults.add(StageMetricSample.stage("assigner", "VBucket Assigner", "unknown", 0, 0, 0, 0, 0, now));
    defaults.add(StageMetricSample.stage("enrichment", "Enrichment Process", "unknown", 0, 0, 0, 0, 0, now));
    defaults.add(StageMetricSample.stage("load-coordinator", "Load Coordinator", "unknown", 0, 0, 0, 0, 0, now));
    defaults.add(StageMetricSample.sink("starrocks-sink", "StarRocks Sink", "unknown", "starrocks", "anomaly", 0, 0, now));
    defaults.add(StageMetricSample.sink("hive-sink", "Hive Sink", "unknown", "hive", "1m", 0, 0, now));
    defaults.add(StageMetricSample.sink("hive-sink", "Hive Sink", "unknown", "hive", "5m", 0, 0, now));
    defaults.add(StageMetricSample.sink("iceberg-sink", "Iceberg Sink", "unknown", "iceberg", "1m", 0, 0, now));
    defaults.add(StageMetricSample.sink("iceberg-sink", "Iceberg Sink", "unknown", "iceberg", "5m", 0, 0, now));
    defaults.forEach(sample -> samples.put(sampleKey(sample), sample));
  }

  private static Map<String, Integer> stageOrder() {
    Map<String, Integer> order = new LinkedHashMap<>();
    order.put("chr-source", 0);
    order.put("mr-source", 1);
    order.put("cm-source", 2);
    order.put("kafka", 3);
    order.put("assigner", 4);
    order.put("enrichment", 5);
    order.put("load-coordinator", 6);
    order.put("starrocks-sink", 7);
    order.put("hive-sink", 8);
    order.put("iceberg-sink", 9);
    return order;
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
}
