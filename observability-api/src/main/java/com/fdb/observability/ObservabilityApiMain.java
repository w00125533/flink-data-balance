package com.fdb.observability;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fdb.observability.service.ExecutionRunHistoryService;
import com.fdb.observability.service.ObservabilitySnapshotService;
import com.fdb.observability.service.StageMetricKafkaConsumer;
import com.fdb.observability.service.StarRocksQueryService;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ObservabilityApiMain {
  private static final Logger log = LoggerFactory.getLogger(ObservabilityApiMain.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  private ObservabilityApiMain() {
  }

  public static void main(String[] args) throws IOException {
    int port = Integer.parseInt(System.getenv().getOrDefault("FDB_OBSERVABILITY_PORT", "18080"));
    ObservabilitySnapshotService service = new ObservabilitySnapshotService();
    String bootstrap = System.getenv().getOrDefault("FDB_KAFKA_BOOTSTRAP", "kafka:9092");
    String metricsTopic = System.getenv().getOrDefault("FDB_METRICS_TOPIC", "fdb-stage-metrics");
    StageMetricKafkaConsumer metricConsumer = new StageMetricKafkaConsumer(bootstrap, metricsTopic, service);
    metricConsumer.start();
    Runtime.getRuntime().addShutdownHook(new Thread(metricConsumer::close));
    Path runsRoot = Path.of(System.getenv().getOrDefault("FDB_RUN_HISTORY_DIR", "/observability-runs"));
    HttpServer server = createServer(port, service, new ExecutionRunHistoryService(runsRoot),
        new StarRocksQueryService());
    server.start();
  }

  static HttpServer createServer(int port, ObservabilitySnapshotService service) throws IOException {
    return createServer(port, service, new ExecutionRunHistoryService(Path.of("docker/data/observability-runs")),
        new StarRocksQueryService());
  }

  static HttpServer createServer(
      int port,
      ObservabilitySnapshotService service,
      StarRocksQueryService queryService) throws IOException {
    return createServer(port, service, new ExecutionRunHistoryService(Path.of("docker/data/observability-runs")),
        queryService);
  }

  static HttpServer createServer(
      int port,
      ObservabilitySnapshotService service,
      ExecutionRunHistoryService runHistoryService) throws IOException {
    return createServer(port, service, runHistoryService, new StarRocksQueryService());
  }

  static HttpServer createServer(
      int port,
      ObservabilitySnapshotService service,
      ExecutionRunHistoryService runHistoryService,
      StarRocksQueryService queryService) throws IOException {
    HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
    server.createContext("/api/flow/status", exchange -> writeJson(exchange, service.stageStatuses()));
    server.createContext("/api/flow/sources", exchange -> writeJson(exchange, service.sourceSummaries()));
    server.createContext("/api/flow/migrations", exchange -> writeJson(exchange, service.migrationEvents()));
    server.createContext("/api/flow/sinks", exchange -> writeJson(exchange, service.sinkSummaries()));
    server.createContext("/api/metrics/summary", exchange -> writeJson(exchange, Map.of(
        "stages", service.stageStatuses(),
        "sources", service.sourceSummaries(),
        "migrations", service.migrationEvents(),
        "sinks", service.sinkSummaries())));
    server.createContext("/api/results/kpi/1m",
        exchange -> writeQueryJson(exchange, () -> queryService.queryKpi1m(queryParameters(exchange))));
    server.createContext("/api/results/kpi/5m",
        exchange -> writeQueryJson(exchange, () -> queryService.queryKpi5m(queryParameters(exchange))));
    server.createContext("/api/results/anomalies/cell",
        exchange -> writeQueryJson(exchange, () -> queryService.queryCellAnomalies(queryParameters(exchange))));
    server.createContext("/api/results/anomalies/grid",
        exchange -> writeQueryJson(exchange, () -> queryService.queryGridAnomalies(queryParameters(exchange))));
    server.createContext("/api/results/sink-latency", exchange -> writeJson(exchange, service.sinkLatencySummaries()));
    server.createContext("/api/runtime/config", exchange -> writeJson(exchange, Map.of(
        "dynamicBalancingEnabled", service.dynamicBalancingEnabled(),
        "resultQueryLayer", "starrocks",
        "kpiStorage", "starrocks",
        "anomalyStorage", "starrocks")));
    server.createContext("/api/events/stream", exchange -> writeSse(exchange, service));
    server.createContext("/api/runs", exchange -> writeRuns(exchange, runHistoryService));
    server.createContext("/metrics", exchange -> writeText(exchange, toPrometheusMetrics(service)));
    server.setExecutor(Executors.newCachedThreadPool());
    return server;
  }

  static String toJson(Object value) throws IOException {
    return JSON.writeValueAsString(value);
  }

  static String toPrometheusMetrics(ObservabilitySnapshotService service) {
    StringBuilder out = new StringBuilder();
    service.sourceSummaries().forEach(source -> {
      String sourceName = escapeLabelValue(source.source());
      out.append("fdb_source_eps{source=\"").append(sourceName).append("\"} ").append(source.eps()).append('\n');
      out.append("fdb_source_lag_ms{source=\"").append(sourceName).append("\"} ").append(source.eventDelayMs()).append('\n');
    });
    service.stageStatuses().forEach(stage -> {
      String stageId = escapeLabelValue(stage.stageId());
      out.append("fdb_stage_in_eps{stage=\"").append(stageId).append("\"} ").append(stage.inEps()).append('\n');
      out.append("fdb_stage_out_eps{stage=\"").append(stageId).append("\"} ").append(stage.outEps()).append('\n');
      out.append("fdb_stage_latency_ms{stage=\"").append(stageId).append("\",quantile=\"p95\"} ")
          .append(stage.latencyP95Ms()).append('\n');
      out.append("fdb_stage_watermark_lag_ms{stage=\"").append(stageId).append("\"} ")
          .append(stage.watermarkLagMs()).append('\n');
    });
    service.sinkSummaries().forEach(sink -> {
      String sinkName = escapeLabelValue(sink.sink());
      String window = escapeLabelValue(sink.window());
      out.append("fdb_sink_write_rows_total{sink=\"").append(sinkName).append("\",window=\"").append(window)
          .append("\"} ").append(sink.rowsWritten()).append('\n');
      out.append("fdb_sink_write_latency_ms{sink=\"").append(sinkName).append("\",window=\"").append(window)
          .append("\"} ").append(sink.writeLatencyP95Ms()).append('\n');
    });
    service.stageStatuses().forEach(stage -> out.append("fdb_stage_error_total{stage=\"")
        .append(escapeLabelValue(stage.stageId())).append("\"} ").append(stage.dlqCount()).append('\n'));
    out.append("fdb_rebalance_total ").append(service.rebalanceTotal()).append('\n');
    return out.toString();
  }

  private static String escapeLabelValue(String value) {
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\"", "\\\"");
  }

  private static void writeJson(HttpExchange exchange, Object body) throws IOException {
    byte[] bytes = toJson(body).getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
    exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
    exchange.sendResponseHeaders(200, bytes.length);
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(bytes);
    }
  }

  private static void writeQueryJson(HttpExchange exchange, QueryHandler query) throws IOException {
    try {
      writeJson(exchange, query.execute());
    } catch (IllegalArgumentException e) {
      log.warn("Rejected result query request: {}", e.getMessage());
      writeError(exchange, 400, "bad request");
    } catch (SQLException e) {
      log.error("Result query failed", e);
      writeError(exchange, 500, "query failed");
    } catch (RuntimeException e) {
      log.error("Result query failed", e);
      writeError(exchange, 500, "query failed");
    }
  }

  private static Map<String, String> queryParameters(HttpExchange exchange) {
    String rawQuery = exchange.getRequestURI().getRawQuery();
    if (rawQuery == null || rawQuery.isBlank()) {
      return Map.of();
    }
    Map<String, String> parameters = new LinkedHashMap<>();
    for (String part : rawQuery.split("&")) {
      if (part.isBlank()) {
        continue;
      }
      String[] keyValue = part.split("=", 2);
      String key = urlDecode(keyValue[0]);
      String value = keyValue.length == 2 ? urlDecode(keyValue[1]) : "";
      parameters.putIfAbsent(key, value);
    }
    return parameters;
  }

  private static String urlDecode(String value) {
    return URLDecoder.decode(value, StandardCharsets.UTF_8);
  }

  private static void writeRuns(HttpExchange exchange, ExecutionRunHistoryService runHistoryService)
      throws IOException {
    String path = exchange.getRequestURI().getPath();
    if ("/api/runs".equals(path) || "/api/runs/".equals(path)) {
      writeJson(exchange, runHistoryService.listRuns());
      return;
    }
    String prefix = "/api/runs/";
    if (path.startsWith(prefix)) {
      String runId = path.substring(prefix.length());
      var detail = runHistoryService.runDetail(runId);
      if (detail.isPresent()) {
        writeJson(exchange, detail.get());
      } else {
        writeNotFound(exchange);
      }
      return;
    }
    writeNotFound(exchange);
  }

  private static void writeSse(HttpExchange exchange, ObservabilitySnapshotService service) throws IOException {
    exchange.getResponseHeaders().add("Content-Type", "text/event-stream; charset=utf-8");
    exchange.getResponseHeaders().add("Cache-Control", "no-cache");
    exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
    exchange.sendResponseHeaders(200, 0);
    try (OutputStream output = exchange.getResponseBody()) {
      while (true) {
        String payload = "event: stage_status\n"
            + "data: " + toJson(service.stageStatuses()) + "\n\n"
            + "event: migration_event\n"
            + "data: " + toJson(service.migrationEvents()) + "\n\n"
            + ": heartbeat\n\n";
        output.write(payload.getBytes(StandardCharsets.UTF_8));
        output.flush();
        sleepBetweenSseSnapshots();
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private static void sleepBetweenSseSnapshots() throws InterruptedException {
    Thread.sleep(5_000L);
  }

  private static void writeText(HttpExchange exchange, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
    exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
    exchange.sendResponseHeaders(200, bytes.length);
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(bytes);
    }
  }

  private static void writeNotFound(HttpExchange exchange) throws IOException {
    byte[] bytes = "{\"error\":\"not found\"}".getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
    exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
    exchange.sendResponseHeaders(404, bytes.length);
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(bytes);
    }
  }

  private static void writeError(HttpExchange exchange, int statusCode, String error) throws IOException {
    byte[] bytes = toJson(Map.of("error", error)).getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
    exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
    exchange.sendResponseHeaders(statusCode, bytes.length);
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(bytes);
    }
  }

  private interface QueryHandler {
    Object execute() throws SQLException;
  }
}
