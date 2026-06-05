package com.fdb.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.observability.service.ObservabilitySnapshotService;
import org.junit.jupiter.api.Test;

class PrometheusMetricsTest {

  @Test
  void rendersSourceAndSinkMetrics() {
    String metrics = ObservabilityApiMain.toPrometheusMetrics(new ObservabilitySnapshotService());
    assertThat(metrics).contains("fdb_source_eps{source=\"chr\"}");
    assertThat(metrics).contains("fdb_sink_write_latency_ms{sink=\"iceberg\",window=\"1m\"}");
    assertThat(metrics).contains("fdb_rebalance_total");
  }
}
