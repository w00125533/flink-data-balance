package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpTimeoutException;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ObservabilityClientTest {
  @Test
  void reads_stage_and_sink_latency_summaries() throws Exception {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/api/flow/stages",
            """
                [
                  {"stageId":"kpi-1m","latencyP50Ms":30000,"latencyP95Ms":70000,"latencyP99Ms":90000,"watermarkLagMs":12000},
                  {"stageId":"kpi-5m","latencyP50Ms":45000,"latencyP95Ms":80000,"latencyP99Ms":99000,"watermarkLagMs":15000}
                ]
                """,
        "/api/results/sink-latency",
            """
                [{
                  "sinkName":"starrocks-kpi-1m",
                  "records":100,
                  "bytes":4096,
                  "latencyP50Ms":50000,
                  "latencyP95Ms":90000,
                  "latencyP99Ms":120000,
                  "failureCount":0
                }]
                """));

    FdbMetricsSnapshot snapshot = new ObservabilityClient(URI.create("http://api:18080"), http).snapshot();

    assertThat(snapshot.kpi1mP95Ms()).isEqualTo(70_000);
    assertThat(snapshot.sinkP95Ms()).isEqualTo(90_000);
    assertThat(snapshot.sinkFailures()).isZero();
    assertThat(snapshot.watermarkLagMs()).isEqualTo(15_000);
    assertThat(snapshot.stageLatencies()).hasSize(2);
    assertThat(snapshot.stageLatencies().get(0).stageId()).isEqualTo("kpi-1m");
    assertThat(snapshot.stageLatencies().get(0).latencyP50Ms()).isEqualTo(30_000);
    assertThat(snapshot.stageLatencies().get(0).latencyP99Ms()).isEqualTo(90_000);
    assertThat(snapshot.sinkLatencies()).singleElement().satisfies(sink -> {
      assertThat(sink.sinkName()).isEqualTo("starrocks-kpi-1m");
      assertThat(sink.records()).isEqualTo(100);
      assertThat(sink.bytes()).isEqualTo(4096);
      assertThat(sink.latencyP50Ms()).isEqualTo(50_000);
      assertThat(sink.latencyP95Ms()).isEqualTo(90_000);
      assertThat(sink.latencyP99Ms()).isEqualTo(120_000);
      assertThat(sink.failures()).isZero();
    });
  }

  @Test
  void returns_empty_metrics_when_observability_requests_timeout() throws Exception {
    HttpGateway http = uri -> {
      throw new HttpTimeoutException("request timed out");
    };

    FdbMetricsSnapshot snapshot = new ObservabilityClient(URI.create("http://api:18080"), http).snapshot();

    assertThat(snapshot.sourceDelayP95Ms()).isZero();
    assertThat(snapshot.kpi1mP95Ms()).isZero();
    assertThat(snapshot.kpi5mP95Ms()).isZero();
    assertThat(snapshot.sinkP95Ms()).isZero();
    assertThat(snapshot.sinkFailures()).isZero();
    assertThat(snapshot.watermarkLagMs()).isZero();
    assertThat(snapshot.stageLatencies()).isEmpty();
    assertThat(snapshot.sinkLatencies()).isEmpty();
  }
}
