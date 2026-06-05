package com.fdb.observability.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.common.metrics.StageMetricSample;
import org.junit.jupiter.api.Test;

class ObservabilitySnapshotServiceTest {

  private final ObservabilitySnapshotService service = new ObservabilitySnapshotService();

  @Test
  void returnsCoreStages() {
    assertThat(service.stageStatuses())
        .extracting("stageId")
        .contains("chr-source", "mr-source", "cm-source", "enrichment", "load-coordinator", "iceberg-sink");
  }

  @Test
  void returnsThreeSourceSummaries() {
    assertThat(service.sourceSummaries())
        .extracting("source")
        .containsExactlyInAnyOrder("chr", "mr", "cm");
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
  void includesHiveAndIcebergSinkSummaries() {
    assertThat(service.sinkSummaries())
        .extracting("sink")
        .contains("mysql", "hive", "iceberg");
  }

  @Test
  void keepsSeparateSinkSummariesForEachWindow() {
    service.applyMetricSample(StageMetricSample.sink(
        "hive-sink", "Hive Sink", "healthy", "hive", "1m", 10, 100, 1_717_400_000_000L));
    service.applyMetricSample(StageMetricSample.sink(
        "hive-sink", "Hive Sink", "healthy", "hive", "5m", 2, 120, 1_717_400_001_000L));

    assertThat(service.sinkSummaries())
        .filteredOn(summary -> summary.sink().equals("hive"))
        .extracting("window")
        .contains("1m", "5m");
  }
}
