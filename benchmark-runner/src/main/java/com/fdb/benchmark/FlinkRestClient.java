package com.fdb.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class FlinkRestClient {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String VERTEX_RATE_METRICS = String.join(",",
      "numRecordsInPerSecond",
      "numRecordsOutPerSecond",
      "numBytesInPerSecond",
      "numBytesOutPerSecond",
      "busyTimeMsPerSecond",
      "idleTimeMsPerSecond",
      "backPressuredTimeMsPerSecond",
      "pendingRecords",
      "currentInputWatermark",
      "currentOutputWatermark");

  private final URI baseUri;
  private final HttpGateway http;

  public FlinkRestClient(URI baseUri, HttpGateway http) {
    this.baseUri = baseUri;
    this.http = http;
  }

  public FlinkSnapshot snapshot() throws IOException, InterruptedException {
    JsonNode overview = readOrEmpty("/jobs/overview");
    JsonNode job = selectJob(overview.path("jobs"));
    String jobId = text(job, "jid", "");
    String status = text(job, "state", "UNKNOWN");
    long checkpointDurationMs = 0;
    int consecutiveCheckpointFailures = 0;
    double recordsInPerSec = 0;
    double recordsOutPerSec = 0;
    double recordsInTotal = 0;
    double recordsOutTotal = 0;
    long sourceBacklogRecords = 0;
    double backpressureRatio = 0;
    int taskManagers = 0;
    int slots = 0;
    List<FlinkOperatorSnapshot> operators = new ArrayList<>();
    List<FlinkOperatorEdge> operatorEdges = List.of();

    JsonNode taskManagersNode = readOrEmpty("/taskmanagers").path("taskmanagers");
    if (taskManagersNode.isArray()) {
      taskManagers = taskManagersNode.size();
      for (JsonNode taskManager : taskManagersNode) {
        slots += firstInt(taskManager, "slotsNumber", "slots");
      }
    }

    if (!jobId.isBlank()) {
      JsonNode checkpoints = readOrEmpty("/jobs/" + jobId + "/checkpoints");
      checkpointDurationMs = checkpointDurationMs(checkpoints.path("latest").path("completed"));
      consecutiveCheckpointFailures = checkpoints.path("counts").path("failed").asInt(0);

      JsonNode jobDetails = readOrEmpty("/jobs/" + jobId);
      operatorEdges = operatorEdges(jobDetails.path("plan").path("nodes"));
      Map<String, List<String>> inputSourcesByTarget = inputSourcesByTarget(operatorEdges);
      JsonNode vertices = jobDetails.path("vertices");
      if (!vertices.isArray()) {
        vertices = readOrEmpty("/jobs/" + jobId + "/vertices").path("vertices");
      }
      if (vertices.isArray()) {
        for (JsonNode vertex : vertices) {
          String vertexId = text(vertex, "id", "");
          JsonNode metrics = vertex.path("metrics");
          JsonNode rateMetrics = vertexId.isBlank()
              ? MAPPER.createObjectNode()
              : metricMap(readOrEmpty("/jobs/" + jobId + "/vertices/" + vertexId + "/metrics?get="
                  + VERTEX_RATE_METRICS));
          List<FlinkMarkerLatencySnapshot> markerLatencies = vertexId.isBlank()
              ? List.of()
              : flinkMarkerLatencies(jobId, vertexId, inputSourcesByTarget.getOrDefault(vertexId, List.of()));
          FlinkOperatorSnapshot operator = operatorSnapshot(vertex, metrics, rateMetrics,
              flinkMarkerP95Ms(markerLatencies), markerLatencies);
          operators.add(operator);
          recordsInPerSec += operator.recordsInPerSec();
          recordsOutPerSec += operator.recordsOutPerSec();
          recordsInTotal += operator.recordsInTotal();
          recordsOutTotal += operator.recordsOutTotal();
          if (isSourceOperator(operator)) {
            sourceBacklogRecords = Math.max(sourceBacklogRecords, operator.pendingRecords());
          }
          backpressureRatio = Math.max(backpressureRatio, operator.backpressureRatio());
        }
      }
    }

    return new FlinkSnapshot(status, backpressureRatio, checkpointDurationMs, consecutiveCheckpointFailures,
        recordsInPerSec, recordsOutPerSec, recordsInTotal, recordsOutTotal, taskManagers, slots, operators,
        operatorEdges, sourceBacklogRecords);
  }

  private JsonNode selectJob(JsonNode jobs) {
    if (!jobs.isArray() || jobs.isEmpty()) {
      return MAPPER.createObjectNode();
    }
    for (JsonNode job : jobs) {
      String state = text(job, "state", "");
      if (!"FINISHED".equalsIgnoreCase(state) && !"FAILED".equalsIgnoreCase(state)
          && !"CANCELED".equalsIgnoreCase(state)) {
        return job;
      }
    }
    return jobs.get(0);
  }

  private JsonNode read(String path) throws IOException, InterruptedException {
    return MAPPER.readTree(http.get(baseUri.resolve(path)));
  }

  private JsonNode readOrEmpty(String path) {
    try {
      return read(path);
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return MAPPER.createObjectNode();
    }
  }

  private static String text(JsonNode node, String field, String defaultValue) {
    JsonNode value = node.path(field);
    return value.isMissingNode() || value.isNull() ? defaultValue : value.asText(defaultValue);
  }

  private static double metricNumber(JsonNode metrics, String field) {
    JsonNode value = metrics.path(field);
    if (value.isNumber()) {
      return value.asDouble();
    }
    if (value.isTextual()) {
      try {
        return Double.parseDouble(value.asText());
      } catch (NumberFormatException ignored) {
        return 0;
      }
    }
    return 0;
  }

  private static long metricLong(JsonNode metrics, String field) {
    return metricLong(metrics, field, 0);
  }

  private static long metricLong(JsonNode metrics, String field, long defaultValue) {
    JsonNode value = metrics.path(field);
    if (value.isIntegralNumber()) {
      return value.asLong();
    }
    if (value.isNumber()) {
      return (long) value.asDouble();
    }
    if (value.isTextual()) {
      try {
        return Long.parseLong(value.asText());
      } catch (NumberFormatException ignored) {
        try {
          return (long) Double.parseDouble(value.asText());
        } catch (NumberFormatException ignoredAgain) {
          return defaultValue;
        }
      }
    }
    return defaultValue;
  }

  private static JsonNode metricMap(JsonNode metricsResponse) {
    if (!metricsResponse.isArray()) {
      return MAPPER.createObjectNode();
    }
    var metrics = MAPPER.createObjectNode();
    for (JsonNode metric : metricsResponse) {
      String id = text(metric, "id", "");
      if (!id.isBlank()) {
        metrics.put(id, text(metric, "value", "0"));
      }
    }
    return metrics;
  }

  private static double metricNumberOrFallback(JsonNode metrics, JsonNode fallback, String field) {
    return hasMetric(metrics, field) ? metricNumber(metrics, field) : metricNumber(fallback, field);
  }

  private static boolean hasMetric(JsonNode metrics, String field) {
    JsonNode value = metrics.path(field);
    return !value.isMissingNode() && !value.isNull();
  }

  private static FlinkOperatorSnapshot operatorSnapshot(JsonNode vertex, JsonNode cumulativeMetrics,
      JsonNode rateMetrics, long flinkMarkerP95Ms, List<FlinkMarkerLatencySnapshot> markerLatencies) {
    double busyMs = metricNumberOrFallback(rateMetrics, cumulativeMetrics, "busyTimeMsPerSecond");
    double idleMs = metricNumberOrFallback(rateMetrics, cumulativeMetrics, "idleTimeMsPerSecond");
    double backpressureMs = metricNumberOrFallback(rateMetrics, cumulativeMetrics, "backPressuredTimeMsPerSecond");
    return new FlinkOperatorSnapshot(
        text(vertex, "id", ""),
        text(vertex, "name", ""),
        vertex.path("parallelism").asInt(0),
        metricNumber(rateMetrics, "numRecordsInPerSecond"),
        metricNumber(rateMetrics, "numRecordsOutPerSecond"),
        metricNumber(cumulativeMetrics, "read-records"),
        metricNumber(cumulativeMetrics, "write-records"),
        metricNumber(rateMetrics, "numBytesInPerSecond"),
        metricNumber(rateMetrics, "numBytesOutPerSecond"),
        ratioFromMillisPerSecond(busyMs),
        ratioFromMillisPerSecond(idleMs),
        ratioFromMillisPerSecond(backpressureMs),
        metricLong(rateMetrics, "pendingRecords"),
        metricLong(rateMetrics, "currentInputWatermark", -1),
        metricLong(rateMetrics, "currentOutputWatermark", -1),
        flinkMarkerP95Ms,
        markerLatencies);
  }

  private static long flinkMarkerP95Ms(List<FlinkMarkerLatencySnapshot> markers) {
    long max = -1L;
    for (FlinkMarkerLatencySnapshot marker : markers) {
      max = Math.max(max, marker.p95Ms());
    }
    return max;
  }

  private List<FlinkMarkerLatencySnapshot> flinkMarkerLatencies(String jobId, String targetId,
      List<String> inputSourceIds) {
    String metricPath = "/jobs/" + jobId + "/vertices/" + targetId + "/metrics";
    JsonNode markerMetrics = readOrEmpty(metricPath);
    List<String> markerIds = flinkMarkerP95MetricIds(markerMetrics);
    if (markerIds.isEmpty()) {
      return List.of();
    }
    List<FlinkMarkerLatencySnapshot> markerValues = parseFlinkMarkerLatencies(targetId, inputSourceIds,
        readOrEmpty(metricPath + "?get=" + String.join(",", markerIds)));
    if (!markerValues.isEmpty()) {
      return markerValues;
    }
    return parseFlinkMarkerLatencies(targetId, inputSourceIds, markerMetrics);
  }

  private static List<String> flinkMarkerP95MetricIds(JsonNode metricsResponse) {
    if (!metricsResponse.isArray()) {
      return List.of();
    }
    List<String> ids = new ArrayList<>();
    for (JsonNode metric : metricsResponse) {
      String id = text(metric, "id", "");
      if (isFlinkMarkerP95Metric(id)) {
        ids.add(id);
      }
    }
    return List.copyOf(ids);
  }

  private static List<FlinkMarkerLatencySnapshot> parseFlinkMarkerLatencies(String targetId,
      List<String> inputSourceIds,
      JsonNode metricsResponse) {
    if (!metricsResponse.isArray()) {
      return List.of();
    }
    Map<String, FlinkMarkerLatencySnapshot> byEdge = new LinkedHashMap<>();
    for (JsonNode metric : metricsResponse) {
      String id = text(metric, "id", "");
      if (isFlinkMarkerP95Metric(id)) {
        long p95Ms = metricLong(metric, "value", -1L);
        if (p95Ms < 0) {
          continue;
        }
        String sourceId = markerSourceId(id, inputSourceIds);
        String key = sourceId + "\u0000" + targetId;
        FlinkMarkerLatencySnapshot existing = byEdge.get(key);
        if (existing == null || p95Ms > existing.p95Ms()) {
          byEdge.put(key, new FlinkMarkerLatencySnapshot(sourceId, targetId, p95Ms, id));
        }
      }
    }
    return List.copyOf(byEdge.values());
  }

  private static String markerSourceId(String metricId, List<String> inputSourceIds) {
    String lowerMetric = lower(metricId);
    for (String sourceId : inputSourceIds) {
      if (!sourceId.isBlank() && lowerMetric.contains(lower(sourceId))) {
        return sourceId;
      }
    }
    if (inputSourceIds.size() == 1) {
      return inputSourceIds.get(0);
    }
    return "unknown";
  }

  private static boolean isFlinkMarkerP95Metric(String metricId) {
    String value = metricId == null ? "" : metricId.toLowerCase(java.util.Locale.ROOT);
    return value.contains("latency")
        && !value.contains("watermark")
        && (value.contains("p95")
            || value.contains("95th")
            || value.contains("95percentile")
            || value.contains("percentile95")
            || value.endsWith(".95")
            || value.endsWith("_95"));
  }

  private static long checkpointDurationMs(JsonNode completed) {
    if (completed.has("end_to_end_duration")) {
      return completed.path("end_to_end_duration").asLong(0);
    }
    return completed.path("duration").asLong(0);
  }

  private static List<FlinkOperatorEdge> operatorEdges(JsonNode nodes) {
    if (!nodes.isArray()) {
      return List.of();
    }
    List<FlinkOperatorEdge> edges = new ArrayList<>();
    for (JsonNode node : nodes) {
      String targetId = text(node, "id", "");
      JsonNode inputs = node.path("inputs");
      if (targetId.isBlank() || !inputs.isArray()) {
        continue;
      }
      for (JsonNode input : inputs) {
        String sourceId = text(input, "id", "");
        if (!sourceId.isBlank()) {
          edges.add(new FlinkOperatorEdge(sourceId, targetId));
        }
      }
    }
    return List.copyOf(edges);
  }

  private static Map<String, List<String>> inputSourcesByTarget(List<FlinkOperatorEdge> edges) {
    Map<String, List<String>> values = new LinkedHashMap<>();
    for (FlinkOperatorEdge edge : edges) {
      values.computeIfAbsent(edge.targetId(), ignored -> new ArrayList<>()).add(edge.sourceId());
    }
    return values;
  }

  private static boolean isSourceOperator(FlinkOperatorSnapshot operator) {
    return operator.name().toLowerCase().contains("source");
  }

  private static String lower(String value) {
    return value == null ? "" : value.toLowerCase(java.util.Locale.ROOT);
  }

  private static double ratioFromMillisPerSecond(double value) {
    return value <= 0 ? 0 : value / 1000.0;
  }

  private static int firstInt(JsonNode node, String first, String second) {
    if (node.has(first)) {
      return node.path(first).asInt(0);
    }
    return node.path(second).asInt(0);
  }
}
