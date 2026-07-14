package com.fdb.observability.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.common.metrics.StageMetricSample;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class ObservabilitySnapshotServiceTest {

  private final ObservabilitySnapshotService service = new ObservabilitySnapshotService(false);

  @Test
  void returnsCoreStages() {
    assertThat(service.stageStatuses())
        .extracting("stageId")
        .contains("chr-source", "pm-source", "cfg-source", "kafka", "enrichment", "iceberg-kpi-1m")
        .doesNotContain("assigner", "load-coordinator", "hive-sink", "iceberg-sink");
  }

  @Test
  void returnsDynamicBalancingStagesWhenEnabled() {
    ObservabilitySnapshotService dynamicService = new ObservabilitySnapshotService(true);

    assertThat(dynamicService.stageStatuses())
        .extracting("stageId")
        .contains("assigner", "load-coordinator");
  }

  @Test
  void returnsThreeSourceSummaries() {
    assertThat(service.sourceSummaries())
        .extracting("source")
        .containsExactlyInAnyOrder("chr", "pm", "cfg");
  }

  @Test
  void includesMigrationEventAfterRuntimeRebalanceMetric() {
    assertThat(service.migrationEvents()).isEmpty();

    service.applyMetricSample(StageMetricSample.stage("load-coordinator", "Load Coordinator", "healthy",
        0.0, 2.0, 0, 0, 0, 1_717_400_000_000L).withRebalanceTotal(2));

    assertThat(service.migrationEvents()).hasSize(1);
    assertThat(service.migrationEvents().get(0).routingVersionAfter()).isEqualTo(2);
  }

  @Test
  void includesPerBranchSinkSummaries() {
    assertThat(service.sinkSummaries())
        .extracting("sink")
        .contains("kafka-kpi-1m", "starrocks-kpi-1m", "hive-kpi-1m", "iceberg-kpi-1m",
            "kafka-kpi-5m", "starrocks-kpi-5m", "hive-kpi-5m", "iceberg-kpi-5m",
            "kafka-cell-anomaly", "kafka-grid-anomaly",
            "starrocks-cell-anomaly", "starrocks-grid-anomaly");
    assertThat(service.stageStatuses())
        .extracting("stageId")
        .contains("kafka-cell-anomaly", "kafka-grid-anomaly",
            "starrocks-cell-anomaly", "starrocks-grid-anomaly");
  }

  @Test
  void returnsUniqueSinkWindowKeysWithDistinctAnomalySinkNames() {
    var summaries = service.sinkSummaries();

    assertThat(summaries)
        .extracting(summary -> summary.sink() + "/" + summary.window())
        .doesNotHaveDuplicates();
    assertThat(summaries)
        .filteredOn(summary -> summary.window().equals("ANOMALY"))
        .extracting("sink")
        .contains("kafka-cell-anomaly", "kafka-grid-anomaly",
            "starrocks-cell-anomaly", "starrocks-grid-anomaly");
  }

  @Test
  void keepsSeparateSinkSummariesForEachWindow() {
    service.applyMetricSample(StageMetricSample.sinkLatency(
        "hive-kpi-1m", "Cell KPI 1m Hive Sink", "healthy", "hive", "kpi_1m", "MIN_1",
        10, 1_000, 50, 40, 100, 120, 0, "", -1, 1_717_400_000_000L));
    service.applyMetricSample(StageMetricSample.sinkLatency(
        "hive-kpi-5m", "Cell KPI 5m Hive Sink", "healthy", "hive", "kpi_5m", "MIN_5",
        2, 200, 50, 60, 120, 140, 0, "", -1, 1_717_400_001_000L));

    assertThat(service.sinkSummaries())
        .filteredOn(summary -> summary.sink().startsWith("hive-kpi-"))
        .extracting("window")
        .contains("1m", "5m");
  }

  @Test
  void mapsStageLatencyP50FromMetricSample() {
    service.applyMetricSample(StageMetricSample.sinkLatency(
        "starrocks-kpi-1m", "Cell KPI 1m StarRocks Sink", "healthy", "starrocks", "kpi_1m", "MIN_1",
        20, 2_000, 100, 35, 80, 120, 0, "", -1, 1_717_400_002_000L));

    assertThat(service.stageStatuses())
        .filteredOn(stage -> stage.stageId().equals("starrocks-kpi-1m"))
        .singleElement()
        .extracting("latencyP50Ms")
        .isEqualTo(35L);
  }

  @Test
  void keepsLatestSinkLatencySampleWhenOlderSampleArrivesLater() {
    service.applyMetricSample(StageMetricSample.sinkLatency(
        "iceberg-kpi-1m", "Cell KPI 1m Iceberg Sink", "healthy", "iceberg", "kpi_1m", "MIN_1",
        200, 20_000, 40, 20, 40, 60, 0, "", 8, 1_717_400_002_000L));
    service.applyMetricSample(StageMetricSample.sinkLatency(
        "iceberg-kpi-1m", "Cell KPI 1m Iceberg Sink", "degraded", "iceberg", "kpi_1m", "MIN_1",
        1, 100, 999, 900, 999, 1_200, 3, "old failure", 7, 1_717_400_001_000L));

    assertThat(service.sinkLatencySummaries())
        .filteredOn(summary -> summary.sinkName().equals("iceberg-kpi-1m"))
        .singleElement()
        .satisfies(summary -> {
          assertThat(summary.records()).isEqualTo(200L);
          assertThat(summary.p95Ms()).isEqualTo(40L);
          assertThat(summary.checkpointId()).isEqualTo(8L);
          assertThat(summary.updatedAt()).isEqualTo("2024-06-03T07:33:22Z");
        });
  }

  @Test
  void resolveDynamicBalancingDefaultsToDisabled() {
    assertThat(ObservabilitySnapshotService.resolveDynamicBalancingEnabled(Map.<String, String>of(), new Properties()))
        .isFalse();
  }
}
