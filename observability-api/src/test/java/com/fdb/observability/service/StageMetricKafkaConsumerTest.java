package com.fdb.observability.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.common.metrics.StageMetricSample;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.junit.jupiter.api.Test;

class StageMetricKafkaConsumerTest {

  @Test
  void historyAppendFailureDoesNotPreventSnapshotApplyOrLaterRecords() {
    ObservabilitySnapshotService service = new ObservabilitySnapshotService(false);
    MetricHistoryAppender failingHistory = sample -> {
      throw new UncheckedIOException(new IOException("disk full"));
    };
    StageMetricSample first = StageMetricSample.stage("chr-source", "CHR Source", "healthy",
        10.0, 11.0, 12, 13, 0, 1_717_400_000_000L);
    StageMetricSample second = StageMetricSample.stage("pm-source", "PM Source", "healthy",
        20.0, 21.0, 22, 23, 0, 1_717_400_001_000L);

    StageMetricKafkaConsumer.handleRecordValue(first.toJson(), service, failingHistory);
    StageMetricKafkaConsumer.handleRecordValue(second.toJson(), service, failingHistory);

    assertThat(service.stageStatuses())
        .filteredOn(stage -> stage.stageId().equals("chr-source"))
        .singleElement()
        .extracting("inEps")
        .isEqualTo(10.0);
    assertThat(service.stageStatuses())
        .filteredOn(stage -> stage.stageId().equals("pm-source"))
        .singleElement()
        .extracting("inEps")
        .isEqualTo(20.0);
  }
}
