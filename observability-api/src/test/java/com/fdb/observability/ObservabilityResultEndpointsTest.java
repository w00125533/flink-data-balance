package com.fdb.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fdb.common.metrics.StageMetricSample;
import com.fdb.observability.model.AnomalyResultRow;
import com.fdb.observability.model.KpiResultRow;
import com.fdb.observability.service.ObservabilitySnapshotService;
import com.fdb.observability.service.StarRocksQueryService;
import com.sun.net.httpserver.HttpServer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ObservabilityResultEndpointsTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  private HttpServer server;

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void returnsKpiOneMinuteResults() throws Exception {
    FakeStarRocksQueryService queries = new FakeStarRocksQueryService();
    server = startServer(new ObservabilitySnapshotService(), queries);

    JsonNode body = get("/api/results/kpi/1m?cellId=CELL-001");

    assertThat(queries.lastKpi1mParams).containsEntry("cellId", "CELL-001");
    assertThat(body).hasSize(1);
    assertThat(body.get(0).get("windowStartTs").asLong()).isEqualTo(1000L);
    assertThat(body.get(0).get("windowKind").asText()).isEqualTo("MIN_1");
    assertThat(body.get(0).get("joinQuality").asText()).isEqualTo("JOINED");
    assertThat(body.get(0).get("cellId").asText()).isEqualTo("CELL-001");
    assertThat(body.get(0).get("avgPrbUsageDl").asDouble()).isEqualTo(0.7d);
  }

  @Test
  void returnsKpiFiveMinuteResults() throws Exception {
    FakeStarRocksQueryService queries = new FakeStarRocksQueryService();
    server = startServer(new ObservabilitySnapshotService(), queries);

    JsonNode body = get("/api/results/kpi/5m?siteId=SITE-001");

    assertThat(queries.lastKpi5mParams).containsEntry("siteId", "SITE-001");
    assertThat(body.get(0).get("windowKind").asText()).isEqualTo("MIN_5");
    assertThat(body.get(0).get("siteId").asText()).isEqualTo("SITE-001");
  }

  @Test
  void returnsCellAnomalyResults() throws Exception {
    FakeStarRocksQueryService queries = new FakeStarRocksQueryService();
    server = startServer(new ObservabilitySnapshotService(), queries);

    JsonNode body = get("/api/results/anomalies/cell?severity=HIGH");

    assertThat(queries.lastCellAnomalyParams).containsEntry("severity", "HIGH");
    assertThat(body.get(0).get("detectionTs").asLong()).isEqualTo(1000L);
    assertThat(body.get(0).get("eventTs").asLong()).isEqualTo(900L);
    assertThat(body.get(0).get("entityType").asText()).isEqualTo("CELL");
    assertThat(body.get(0).get("entityId").asText()).isEqualTo("CELL-001");
    assertThat(body.get(0).get("windowStartTs").asLong()).isEqualTo(100L);
    assertThat(body.get(0).get("windowEndTs").asLong()).isEqualTo(900L);
    assertThat(body.get(0).get("anomalyType").asText()).isEqualTo("CELL_RADIO_BAD");
    assertThat(body.get(0).get("severity").asText()).isEqualTo("HIGH");
    assertThat(body.get(0).get("contextJson").asText()).isEqualTo("{}");
  }

  @Test
  void returnsUserAnomalyResults() throws Exception {
    FakeStarRocksQueryService queries = new FakeStarRocksQueryService();
    server = startServer(new ObservabilitySnapshotService(), queries);

    JsonNode body = get("/api/results/anomalies/user?imsi=460001234567890&entityId=460001234567890");

    assertThat(queries.lastUserAnomalyParams).containsEntry("imsi", "460001234567890");
    assertThat(queries.lastUserAnomalyParams).containsEntry("entityId", "460001234567890");
    assertThat(body.get(0).get("entityType").asText()).isEqualTo("USER");
    assertThat(body.get(0).get("entityId").asText()).isEqualTo("460001234567890");
    assertThat(body.get(0).get("imsi").asText()).isEqualTo("460001234567890");
    assertThat(body.get(0).get("windowStartTs").asLong()).isEqualTo(100L);
    assertThat(body.get(0).get("windowEndTs").asLong()).isEqualTo(900L);
    assertThat(body.get(0).get("anomalyType").asText()).isEqualTo("USER_QOE_BAD");
  }

  @Test
  void returnsGridAnomalyResults() throws Exception {
    FakeStarRocksQueryService queries = new FakeStarRocksQueryService();
    server = startServer(new ObservabilitySnapshotService(), queries);

    JsonNode body = get("/api/results/anomalies/grid?gridId=wx4g0e");

    assertThat(queries.lastGridAnomalyParams).containsEntry("gridId", "wx4g0e");
    assertThat(body.get(0).get("entityType").asText()).isEqualTo("GRID");
    assertThat(body.get(0).get("entityId").asText()).isEqualTo("wx4g0e");
    assertThat(body.get(0).get("gridId").asText()).isEqualTo("wx4g0e");
  }

  @Test
  void returnsSinkLatencySummariesFromSnapshotService() throws Exception {
    ObservabilitySnapshotService service = new ObservabilitySnapshotService();
    service.applyMetricSample(StageMetricSample.sinkLatency(
        "iceberg-kpi-1m", "Iceberg KPI 1m Sink", "healthy", "iceberg", "kpi_1m", "MIN_1",
        120, 12_000, 45, 30, 45, 80, 0, "", 42, 1_717_400_000_000L));
    server = startServer(service, new FakeStarRocksQueryService());

    JsonNode body = get("/api/results/sink-latency");

    JsonNode iceberg = firstWith(body, "sinkName", "iceberg-kpi-1m");
    assertThat(iceberg.get("sinkType").asText()).isEqualTo("iceberg");
    assertThat(iceberg.get("dataset").asText()).isEqualTo("kpi_1m");
    assertThat(iceberg.get("windowKind").asText()).isEqualTo("MIN_1");
    assertThat(iceberg.get("records").asLong()).isEqualTo(120L);
    assertThat(iceberg.get("bytes").asLong()).isEqualTo(12_000L);
    assertThat(iceberg.get("durationMs").asLong()).isEqualTo(45L);
    assertThat(iceberg.get("p50Ms").asLong()).isEqualTo(30L);
    assertThat(iceberg.get("p95Ms").asLong()).isEqualTo(45L);
    assertThat(iceberg.get("p99Ms").asLong()).isEqualTo(80L);
    assertThat(iceberg.get("checkpointId").asLong()).isEqualTo(42L);
    assertThat(iceberg.get("updatedAt").asText()).isEqualTo("2024-06-03T07:33:20Z");
  }

  @Test
  void returnsRuntimeConfig() throws Exception {
    server = startServer(new ObservabilitySnapshotService(), new FakeStarRocksQueryService());

    JsonNode body = get("/api/runtime/config");

    assertThat(body.get("dynamicBalancingEnabled").asBoolean()).isFalse();
    assertThat(body.get("resultQueryLayer").asText()).isEqualTo("starrocks");
    assertThat(body.get("kpiStorage").asText()).isEqualTo("starrocks");
    assertThat(body.get("anomalyStorage").asText()).isEqualTo("starrocks");
  }

  @Test
  void returnsServerErrorJsonWhenResultQueryThrowsSqlException() throws Exception {
    FakeStarRocksQueryService queries = new FakeStarRocksQueryService();
    queries.sqlFailure = new SQLException("database unavailable");
    server = startServer(new ObservabilitySnapshotService(), queries);

    TestResponse response = request("/api/results/kpi/1m");

    assertThat(response.statusCode()).isEqualTo(500);
    assertThat(response.body().get("error").asText()).isEqualTo("query failed");
  }

  @Test
  void returnsServerErrorJsonWhenResultMappingThrowsRuntimeException() throws Exception {
    FakeStarRocksQueryService queries = new FakeStarRocksQueryService();
    queries.runtimeFailure = new IllegalStateException("mapper failed");
    server = startServer(new ObservabilitySnapshotService(), queries);

    TestResponse response = request("/api/results/kpi/1m");

    assertThat(response.statusCode()).isEqualTo(500);
    assertThat(response.body().get("error").asText()).isEqualTo("query failed");
  }

  @Test
  void returnsBadRequestJsonWhenResultQueryParsingThrowsIllegalArgumentException() throws Exception {
    FakeStarRocksQueryService queries = new FakeStarRocksQueryService();
    queries.badRequestFailure = new IllegalArgumentException("URLDecoder: Illegal hex characters");
    server = startServer(new ObservabilitySnapshotService(), queries);

    TestResponse response = request("/api/results/kpi/1m?cellId=CELL-001");

    assertThat(response.statusCode()).isEqualTo(400);
    assertThat(response.body().get("error").asText()).isEqualTo("bad request");
  }

  private static HttpServer startServer(ObservabilitySnapshotService service, StarRocksQueryService queries)
      throws Exception {
    HttpServer httpServer = ObservabilityApiMain.createServer(0, service, queries);
    httpServer.start();
    return httpServer;
  }

  private JsonNode get(String path) throws Exception {
    TestResponse response = request(path);
    assertThat(response.statusCode()).isEqualTo(200);
    return response.body();
  }

  private TestResponse request(String path) throws Exception {
    int port = server.getAddress().getPort();
    HttpResponse<String> response = HttpClient.newHttpClient().send(
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).GET().build(),
        HttpResponse.BodyHandlers.ofString());
    return new TestResponse(response.statusCode(), JSON.readTree(response.body()));
  }

  private static JsonNode firstWith(JsonNode array, String field, String value) {
    for (JsonNode item : array) {
      if (value.equals(item.get(field).asText())) {
        return item;
      }
    }
    throw new AssertionError("No item with " + field + "=" + value + " in " + array);
  }

  private static final class FakeStarRocksQueryService extends StarRocksQueryService {
    private Map<String, String> lastKpi1mParams = Map.of();
    private Map<String, String> lastKpi5mParams = Map.of();
    private Map<String, String> lastCellAnomalyParams = Map.of();
    private Map<String, String> lastUserAnomalyParams = Map.of();
    private Map<String, String> lastGridAnomalyParams = Map.of();
    private IllegalArgumentException badRequestFailure;
    private SQLException sqlFailure;
    private RuntimeException runtimeFailure;

    FakeStarRocksQueryService() {
      super(() -> {
        throw new AssertionError("fake service should not open a JDBC connection");
      });
    }

    @Override
    public List<KpiResultRow> queryKpi1m(Map<String, String> queryParameters) throws SQLException {
      throwIfConfigured();
      lastKpi1mParams = queryParameters;
      return List.of(kpi("MIN_1"));
    }

    @Override
    public List<KpiResultRow> queryKpi5m(Map<String, String> queryParameters) throws SQLException {
      throwIfConfigured();
      lastKpi5mParams = queryParameters;
      return List.of(kpi("MIN_5"));
    }

    @Override
    public List<AnomalyResultRow> queryCellAnomalies(Map<String, String> queryParameters) throws SQLException {
      throwIfConfigured();
      lastCellAnomalyParams = queryParameters;
      return List.of(anomaly("CELL", "CELL-001", null, "CELL-001", "wx4g0e", "CELL_RADIO_BAD"));
    }

    @Override
    public List<AnomalyResultRow> queryUserAnomalies(Map<String, String> queryParameters) throws SQLException {
      throwIfConfigured();
      lastUserAnomalyParams = queryParameters;
      return List.of(anomaly("USER", "460001234567890", "460001234567890", "CELL-001", "wx4g0e",
          "USER_QOE_BAD"));
    }

    @Override
    public List<AnomalyResultRow> queryGridAnomalies(Map<String, String> queryParameters) throws SQLException {
      throwIfConfigured();
      lastGridAnomalyParams = queryParameters;
      return List.of(anomaly("GRID", "wx4g0e", null, null, "wx4g0e", "COVERAGE_HOLE"));
    }

    private void throwIfConfigured() throws SQLException {
      if (badRequestFailure != null) {
        throw badRequestFailure;
      }
      if (sqlFailure != null) {
        throw sqlFailure;
      }
      if (runtimeFailure != null) {
        throw runtimeFailure;
      }
    }

    private static KpiResultRow kpi(String windowKind) {
      return new KpiResultRow(1000L, 61_000L, windowKind, "JOINED", "SITE-001", "CELL-001", "wx4g0e",
          10L, 42L, -92.0d, 12.0d, 0.7d, 120.0d, 0.01d, 0.95d, 0.98d);
    }

    private static AnomalyResultRow anomaly(
        String entityType,
        String entityId,
        String imsi,
        String cellId,
        String gridId,
        String anomalyType) {
      return new AnomalyResultRow(1000L, 900L, entityType, entityId, 100L, 900L, imsi, "SITE-001", cellId,
          gridId, anomalyType, "HIGH", "{}", 39.9d, 116.4d, "v1");
    }
  }

  private record TestResponse(int statusCode, JsonNode body) {
  }
}
