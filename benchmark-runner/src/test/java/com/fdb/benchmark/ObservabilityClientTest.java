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
                  "sinkType":"starrocks",
                  "dataset":"kpi_1m",
                  "windowKind":"MIN_1",
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
      assertThat(sink.sinkType()).isEqualTo("starrocks");
      assertThat(sink.dataset()).isEqualTo("kpi_1m");
      assertThat(sink.windowKind()).isEqualTo("MIN_1");
      assertThat(sink.records()).isEqualTo(100);
      assertThat(sink.bytes()).isEqualTo(4096);
      assertThat(sink.latencyP50Ms()).isEqualTo(50_000);
      assertThat(sink.latencyP95Ms()).isEqualTo(90_000);
      assertThat(sink.latencyP99Ms()).isEqualTo(120_000);
      assertThat(sink.failures()).isZero();
    });
  }

  @Test
  void separates_sink_front_latency_from_connector_internal_latency() {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/api/flow/status", "[]",
        "/api/results/sink-latency",
            """
                [
                  {
                    "sinkName":"starrocks-kpi-1m",
                    "sinkType":"starrocks",
                    "dataset":"kpi_1m",
                    "windowKind":"MIN_1",
                    "records":100,
                    "latencyP95Ms":90,
                    "failureCount":0
                  },
                  {
                    "sinkName":"starrocks-kpi-1m.connector-write",
                    "sinkType":"starrocks",
                    "dataset":"kpi_1m",
                    "windowKind":"MIN_1",
                    "records":100,
                    "latencyP95Ms":12,
                    "failureCount":0
                  },
                  {
                    "sinkName":"starrocks-kpi-1m.connector-commit",
                    "sinkType":"starrocks",
                    "dataset":"kpi_1m",
                    "windowKind":"MIN_1",
                    "records":3,
                    "latencyP95Ms":250,
                    "failureCount":0
                  }
                ]
                """));

    FdbMetricsSnapshot snapshot = new ObservabilityClient(URI.create("http://api:18080"), http).snapshot();

    assertThat(snapshot.sinkP95Ms()).isEqualTo(90L);
    assertThat(snapshot.connectorWriteP95Ms()).isEqualTo(12L);
    assertThat(snapshot.connectorCommitP95Ms()).isEqualTo(250L);
    assertThat(snapshot.sinkLatencies())
        .extracting(SinkLatencySnapshot::scope)
        .containsExactly("sink-front", "connector-write", "connector-commit");
  }

  @Test
  void returns_empty_metrics_when_observability_requests_timeout() throws Exception {
    HttpGateway http = uri -> {
      throw new HttpTimeoutException("request timed out");
    };

    FdbMetricsSnapshot snapshot = new ObservabilityClient(URI.create("http://api:18080"), http).snapshot();

    assertThat(snapshot.sourceDelayP95Ms()).isEqualTo(-1);
    assertThat(snapshot.kpi1mP95Ms()).isEqualTo(-1);
    assertThat(snapshot.kpi5mP95Ms()).isEqualTo(-1);
    assertThat(snapshot.sinkP95Ms()).isEqualTo(-1);
    assertThat(snapshot.connectorWriteP95Ms()).isEqualTo(-1);
    assertThat(snapshot.connectorCommitP95Ms()).isEqualTo(-1);
    assertThat(snapshot.sinkFailures()).isZero();
    assertThat(snapshot.watermarkLagMs()).isZero();
    assertThat(snapshot.stageLatencies()).isEmpty();
    assertThat(snapshot.sinkLatencies()).isEmpty();
  }

  @Test
  void excludes_window_materialization_samples_from_business_sink_p95() {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/api/flow/status", "[]",
        "/api/results/sink-latency",
            """
                [{
                  "sinkName":"window-chr-1m",
                  "sinkType":"window-materialization",
                  "dataset":"chr-1m",
                  "windowKind":"MIN_1@60000",
                  "records":1000,
                  "latencyP95Ms":0,
                  "failureCount":0
                }]
                """));

    FdbMetricsSnapshot snapshot = new ObservabilityClient(URI.create("http://api:18080"), http).snapshot();

    assertThat(snapshot.sinkLatencies()).singleElement().satisfies(sink -> {
      assertThat(sink.sinkType()).isEqualTo("window-materialization");
      assertThat(sink.dataset()).isEqualTo("chr-1m");
    });
    assertThat(snapshot.sinkP95Ms()).isEqualTo(-1);
    assertThat(snapshot.sinkFailures()).isZero();
  }

  @Test
  void missing_latency_p99_is_reported_as_not_available() {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/api/flow/status",
            """
                {"stages":[
                  {"stageId":"kpi-1m","latencyP50Ms":30000,"latencyP95Ms":70000,"watermarkLagMs":12000}
                ]}
                """,
        "/api/results/sink-latency", "[]"));

    FdbMetricsSnapshot snapshot = new ObservabilityClient(URI.create("http://api:18080"), http).snapshot();

    assertThat(snapshot.stageLatencies()).singleElement().satisfies(stage -> {
      assertThat(stage.latencyP50Ms()).isEqualTo(30_000);
      assertThat(stage.latencyP95Ms()).isEqualTo(70_000);
      assertThat(stage.latencyP99Ms()).isEqualTo(-1);
    });
  }

  @Test
  void skips_default_latency_placeholders() {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/api/flow/status",
            """
                [
                  {"stageId":"kpi-1m","status":"unknown","latencyP50Ms":-1,"latencyP95Ms":0,"latencyP99Ms":-1,"watermarkLagMs":0},
                  {"stageId":"enrichment","status":"healthy","latencyP50Ms":4,"latencyP95Ms":8,"latencyP99Ms":12,"watermarkLagMs":5}
                ]
                """,
        "/api/results/sink-latency",
            """
                [
                  {"sinkName":"starrocks-kpi-1m","records":0,"bytes":0,"p50Ms":0,"p95Ms":0,"p99Ms":0,"failureCount":0},
                  {"sinkName":"starrocks-cell-anomaly","records":7,"bytes":700,"p50Ms":2,"p95Ms":3,"p99Ms":4,"failureCount":0}
                ]
                """));

    FdbMetricsSnapshot snapshot = new ObservabilityClient(URI.create("http://api:18080"), http).snapshot();

    assertThat(snapshot.stageLatencies())
        .extracting(StageLatencySnapshot::stageId)
        .containsExactly("enrichment");
    assertThat(snapshot.sinkLatencies())
        .extracting(SinkLatencySnapshot::sinkName)
        .containsExactly("starrocks-cell-anomaly");
    assertThat(snapshot.kpi1mP95Ms()).isEqualTo(-1);
  }

  @Test
  void keeps_healthy_zero_latency_stage_samples() {
    ObservabilityClient client = new ObservabilityClient(URI.create("http://localhost:18080"), uri -> switch (uri.getPath()) {
      case "/api/flow/status" -> """
          {"stages":[
            {"stageId":"cfg-source","status":"healthy","latencyP50Ms":0,"latencyP95Ms":0,"latencyP99Ms":0,"watermarkLagMs":0}
          ]}
          """;
      case "/api/results/sink-latency" -> "[]";
      default -> "[]";
    });

    FdbMetricsSnapshot snapshot = client.snapshot();

    assertThat(snapshot.stageLatencies()).singleElement().satisfies(stage -> {
      assertThat(stage.stageId()).isEqualTo("cfg-source");
      assertThat(stage.latencyP95Ms()).isEqualTo(0);
    });
  }

  @Test
  void skips_zero_only_stage_latency_placeholders_when_status_is_unknown() {
    FakeHttpGateway http = new FakeHttpGateway(Map.of(
        "/api/flow/status",
            """
                [
                  {"stageId":"kpi-5m","status":"unknown","latencyP50Ms":0,"latencyP95Ms":0,"latencyP99Ms":0,"watermarkLagMs":0},
                  {"stageId":"kpi-1m","status":"healthy","latencyP50Ms":4,"latencyP95Ms":8,"latencyP99Ms":12,"watermarkLagMs":5}
                ]
                """,
        "/api/results/sink-latency", "[]"));

    FdbMetricsSnapshot snapshot = new ObservabilityClient(URI.create("http://api:18080"), http).snapshot();

    assertThat(snapshot.stageLatencies())
        .extracting(StageLatencySnapshot::stageId)
        .containsExactly("kpi-1m");
    assertThat(snapshot.kpi5mP95Ms()).isEqualTo(-1);
  }
}
