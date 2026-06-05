package com.fdb.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.observability.service.ObservabilitySnapshotService;
import org.junit.jupiter.api.Test;

class ObservabilityApiMainTest {

  @Test
  void serializesStageStatusJson() throws Exception {
    String json = ObservabilityApiMain.toJson(new ObservabilitySnapshotService().stageStatuses());
    assertThat(json).contains("\"stageId\":\"chr-source\"");
    assertThat(json).contains("\"stageId\":\"iceberg-sink\"");
  }
}
