package com.fdb.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.common.metrics.StageMetricSample;
import com.fdb.observability.model.ReportStatus;
import com.fdb.observability.service.BenchmarkReportService;
import com.fdb.observability.service.ExecutionRunHistoryService;
import com.fdb.observability.service.ObservabilitySnapshotService;
import com.fdb.observability.service.StarRocksQueryService;
import com.sun.net.httpserver.HttpServer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ObservabilityApiMainTest {
  @TempDir
  Path tempDir;

  @Test
  void serializesStageStatusJson() throws Exception {
    String json = ObservabilityApiMain.toJson(new ObservabilitySnapshotService().stageStatuses());
    assertThat(json).contains("\"stageId\":\"chr-source\"");
    assertThat(json).contains("\"stageId\":\"iceberg-kpi-1m\"");
  }

  @Test
  void serializesStageLatencyP99Json() throws Exception {
    ObservabilitySnapshotService service = new ObservabilitySnapshotService();
    service.applyMetricSample(StageMetricSample.stageLatency(
        "kpi-1m", "KPI 1m Full Join", "healthy",
        100.0, 90.0, 30, 70, 120, 12_000, 0, 1_717_400_000_000L));

    String json = ObservabilityApiMain.toJson(service.stageStatuses());

    assertThat(json).contains("\"latencyP99Ms\":120");
  }

  @Test
  void flowRuntimeReturnsRuntimeConfigJson() throws Exception {
    HttpServer server = ObservabilityApiMain.createServer(0, new ObservabilitySnapshotService(),
        new ExecutionRunHistoryService(tempDir), new StarRocksQueryService(), new BenchmarkReportService(tempDir));
    server.start();
    try {
      String body = get(server, "/api/flow/runtime");

      assertThat(body).contains("\"resultSink\":\"starrocks\"");
      assertThat(body).contains("\"metricsHistoryEnabled\":true");
      assertThat(body).contains("\"runId\":\"unknown-run\"");
      assertThat(body).contains("\"parallelism\":4");
    } finally {
      server.stop(0);
    }
  }

  @Test
  void runsReportEndpointGeneratesReportStatusJson() throws Exception {
    ObservabilitySnapshotService service = new ObservabilitySnapshotService();
    HttpServer server = ObservabilityApiMain.createServer(0, service,
        new ExecutionRunHistoryService(tempDir), new StarRocksQueryService(), new BenchmarkReportService(tempDir));
    server.start();
    try {
      String body = get(server, "/api/runs/report?runId=run-a");
      ReportStatus status = ObservabilityApiMain.fromJson(body, ReportStatus.class);

      assertThat(status.runId()).isEqualTo("run-a");
      assertThat(status.status()).isEqualTo("ready");
      assertThat(status.path()).endsWith("report.md");
      assertThat(Files.exists(Path.of(status.path()))).isTrue();
      assertThat(service.runtimeConfig().reportStatus()).isEqualTo("ready");
    } finally {
      server.stop(0);
    }
  }

  private static String get(HttpServer server, String path) throws Exception {
    int port = server.getAddress().getPort();
    HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).GET().build();
    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString()).body();
  }
}
