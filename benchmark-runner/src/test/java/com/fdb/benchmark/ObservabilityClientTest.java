package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ObservabilityClientTest {
  @Test
  void reads_stage_and_sink_latency_summaries() throws Exception {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/api/flow/stages", "[{\"stageId\":\"kpi-1m\",\"latencyP95Ms\":70000,\"watermarkLagMs\":12000}]",
        "/api/results/sink-latency",
            "[{\"sinkName\":\"starrocks-kpi-1m\",\"records\":100,\"latencyP95Ms\":90000,\"failureCount\":0}]"));

    FdbMetricsSnapshot snapshot = new ObservabilityClient(URI.create("http://api:18080"), http).snapshot();

    assertThat(snapshot.kpi1mP95Ms()).isEqualTo(70_000);
    assertThat(snapshot.sinkP95Ms()).isEqualTo(90_000);
    assertThat(snapshot.sinkFailures()).isZero();
    assertThat(snapshot.watermarkLagMs()).isEqualTo(12_000);
  }
}
