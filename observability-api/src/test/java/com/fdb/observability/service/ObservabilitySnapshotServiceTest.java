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
            "starrocks-cell-anomaly", "starrocks-grid-anomaly",
            "hive-cell-anomaly", "hive-grid-anomaly",
            "iceberg-cell-anomaly", "iceberg-grid-anomaly");
    assertThat(service.stageStatuses())
        .extracting("stageId")
        .contains("kafka-cell-anomaly", "kafka-grid-anomaly",
            "starrocks-cell-anomaly", "starrocks-grid-anomaly",
            "hive-cell-anomaly", "hive-grid-anomaly",
            "iceberg-cell-anomaly", "iceberg-grid-anomaly");
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
            "starrocks-cell-anomaly", "starrocks-grid-anomaly",
            "hive-cell-anomaly", "hive-grid-anomaly",
            "iceberg-cell-anomaly", "iceberg-grid-anomaly");
  }

  @Test
  void includesAllBusinessResultStagesForEachSinkType() {
    assertThat(service.stageStatuses())
        .extracting("stageId")
        .contains(
            "kafka-kpi-1m", "kafka-kpi-5m", "kafka-cell-anomaly", "kafka-grid-anomaly",
            "starrocks-kpi-1m", "starrocks-kpi-5m", "starrocks-cell-anomaly", "starrocks-grid-anomaly",
            "hive-kpi-1m", "hive-kpi-5m", "hive-cell-anomaly", "hive-grid-anomaly",
            "iceberg-kpi-1m", "iceberg-kpi-5m", "iceberg-cell-anomaly", "iceberg-grid-anomaly");
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

  @Test
  void runtimeConfigUsesExpectedDefaults() {
    var config = service.runtimeConfig();

    assertThat(config.dynamicBalancingEnabled()).isFalse();
    assertThat(config.resultSink()).isEqualTo("starrocks");
    assertThat(config.dlqEnabled()).isTrue();
    assertThat(config.metricsEnabled()).isTrue();
    assertThat(config.metricsHistoryEnabled()).isTrue();
    assertThat(config.runId()).isEqualTo("unknown-run");
    assertThat(config.runLabel()).isEmpty();
    assertThat(config.parallelism()).isEqualTo(4);
    assertThat(config.checkpointIntervalMs()).isEqualTo(30_000L);
    assertThat(config.jobStatus()).isEqualTo("unknown");
    assertThat(config.reportStatus()).isEqualTo("collecting");
  }

  @Test
  void runtimeConfigInfersRunningJobStatusAfterRuntimeMetricArrives() {
    service.applyMetricSample(StageMetricSample.stage("enrichment", "Enrichment Process", "healthy",
        1.0, 1.0, 0, 0, 0, 1_717_400_000_000L));

    assertThat(service.runtimeConfig().jobStatus()).isEqualTo("running");
  }

  @Test
  void runtimeConfigReflectsReportStatusState() {
    service.markReportReady();

    assertThat(service.runtimeConfig().reportStatus()).isEqualTo("ready");

    service.markReportFailed();

    assertThat(service.runtimeConfig().reportStatus()).isEqualTo("failed");
  }

  @Test
  void runtimeConfigUsesFlinkPropertyNamesForParallelismAndCheckpoint() {
    Properties properties = new Properties();
    properties.setProperty("fdb.flink.parallelism", "8");
    properties.setProperty("fdb.flink.checkpoint.interval.ms", "45000");

    var config = service.runtimeConfig(Map.of(), properties);

    assertThat(config.parallelism()).isEqualTo(8);
    assertThat(config.checkpointIntervalMs()).isEqualTo(45_000L);
  }

  @Test
  void runtimeConfigEnvOverridesFlinkProperties() {
    Properties properties = new Properties();
    properties.setProperty("fdb.flink.parallelism", "8");
    properties.setProperty("fdb.flink.checkpoint.interval.ms", "45000");

    var config = service.runtimeConfig(Map.of(
        "FDB_FLINK_PARALLELISM", "12",
        "FDB_FLINK_CHECKPOINT_INTERVAL_MS", "60000"), properties);

    assertThat(config.parallelism()).isEqualTo(12);
    assertThat(config.checkpointIntervalMs()).isEqualTo(60_000L);
  }

  @Test
  void runtimeConfigCapsHiveAndIcebergCheckpointInterval() {
    Properties hiveProperties = new Properties();
    hiveProperties.setProperty("fdb.result.sink", "hive");
    hiveProperties.setProperty("fdb.flink.checkpoint.interval.ms", "300000");
    Properties icebergProperties = new Properties();
    icebergProperties.setProperty("fdb.result.sink", "iceberg");
    icebergProperties.setProperty("fdb.flink.checkpoint.interval.ms", "240000");
    Properties starrocksProperties = new Properties();
    starrocksProperties.setProperty("fdb.result.sink", "starrocks");
    starrocksProperties.setProperty("fdb.flink.checkpoint.interval.ms", "300000");

    assertThat(service.runtimeConfig(Map.of(), hiveProperties).checkpointIntervalMs()).isEqualTo(180_000L);
    assertThat(service.runtimeConfig(Map.of(), icebergProperties).checkpointIntervalMs()).isEqualTo(180_000L);
    assertThat(service.runtimeConfig(Map.of(), starrocksProperties).checkpointIntervalMs()).isEqualTo(300_000L);
  }
}
