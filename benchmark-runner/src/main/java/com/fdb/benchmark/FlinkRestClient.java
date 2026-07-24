package com.fdb.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class FlinkRestClient {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int METRIC_QUERY_BATCH_SIZE = 32;
  private static final String TASKMANAGER_CPU_LOAD_METRIC = "Status.JVM.CPU.Load";
  private static final String[] VERTEX_METRIC_NAMES = {
      "numRecordsInPerSecond",
      "numRecordsOutPerSecond",
      "numBytesInPerSecond",
      "numBytesOutPerSecond",
      "busyTimeMsPerSecond",
      "idleTimeMsPerSecond",
      "backPressuredTimeMsPerSecond",
      "pendingRecords",
      "currentInputWatermark",
      "currentOutputWatermark",
      "numRecordsIn",
      "numRecordsOut",
      "numBytesIn",
      "numBytesOut"};
  private static final String VERTEX_METRICS = String.join(",", VERTEX_METRIC_NAMES);
  private static final Set<String> AVERAGE_VERTEX_METRICS = Set.of(
      "busyTimeMsPerSecond",
      "idleTimeMsPerSecond",
      "backPressuredTimeMsPerSecond");
  private static final Set<String> WATERMARK_VERTEX_METRICS = Set.of(
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
    double taskManagerCpuLoad = -1.0d;
    List<FlinkOperatorSnapshot> operators = new ArrayList<>();
    List<FlinkOperatorEdge> operatorEdges = List.of();

    JsonNode taskManagersNode = readOrEmpty("/taskmanagers").path("taskmanagers");
    if (taskManagersNode.isArray()) {
      taskManagers = taskManagersNode.size();
      for (JsonNode taskManager : taskManagersNode) {
        slots += firstInt(taskManager, "slotsNumber", "slots");
        taskManagerCpuLoad = Math.max(taskManagerCpuLoad, taskManagerCpuLoad(taskManager));
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
          JsonNode vertexMetrics = vertexId.isBlank()
              ? MAPPER.createObjectNode()
              : vertexMetrics(jobId, vertexId, vertex.path("parallelism").asInt(0));
          List<FlinkMarkerLatencySnapshot> markerLatencies = vertexId.isBlank()
              ? List.of()
              : flinkMarkerLatencies(jobId, vertexId, inputSourcesByTarget.getOrDefault(vertexId, List.of()));
          FlinkOperatorSnapshot operator = operatorSnapshot(vertex, vertexMetrics,
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
        recordsInPerSec, recordsOutPerSec, recordsInTotal, recordsOutTotal, taskManagers, slots,
        taskManagerCpuLoad, operators,
        operatorEdges, sourceBacklogRecords);
  }

  private double taskManagerCpuLoad(JsonNode taskManager) {
    String taskManagerId = text(taskManager, "id", text(taskManager, "path", ""));
    if (taskManagerId.isBlank()) {
      return -1.0d;
    }
    JsonNode metrics = metricMap(readOrEmpty(
        "/taskmanagers/" + taskManagerId + "/metrics?get=" + TASKMANAGER_CPU_LOAD_METRIC));
    double value = metricNumber(metrics, TASKMANAGER_CPU_LOAD_METRIC, -1.0d);
    return Double.isFinite(value) && value >= 0.0d ? value : -1.0d;
  }

  private JsonNode selectJob(JsonNode jobs) {
    if (!jobs.isArray() || jobs.isEmpty()) {
      return MAPPER.createObjectNode();
    }
    JsonNode newestActive = null;
    long newestActiveStartTime = Long.MIN_VALUE;
    for (JsonNode job : jobs) {
      String state = text(job, "state", "");
      if (!"FINISHED".equalsIgnoreCase(state) && !"FAILED".equalsIgnoreCase(state)
          && !"CANCELED".equalsIgnoreCase(state)) {
        long startTime = longValue(job, "start-time", Long.MIN_VALUE);
        if (newestActive == null || startTime > newestActiveStartTime) {
          newestActive = job;
          newestActiveStartTime = startTime;
        }
      }
    }
    if (newestActive != null) {
      return newestActive;
    }
    JsonNode newest = jobs.get(0);
    long newestStartTime = longValue(newest, "start-time", Long.MIN_VALUE);
    for (JsonNode job : jobs) {
      long startTime = longValue(job, "start-time", Long.MIN_VALUE);
      if (startTime > newestStartTime) {
        newest = job;
        newestStartTime = startTime;
      }
    }
    return newest;
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
    return metricNumber(metrics, field, 0.0d);
  }

  private static double metricNumber(JsonNode metrics, String field, double defaultValue) {
    JsonNode value = metrics.path(field);
    if (value.isNumber()) {
      return value.asDouble();
    }
    if (value.isTextual()) {
      try {
        return Double.parseDouble(value.asText());
      } catch (NumberFormatException ignored) {
        return defaultValue;
      }
    }
    return defaultValue;
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

  private static long longValue(JsonNode node, String field, long defaultValue) {
    JsonNode value = node.path(field);
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
        return defaultValue;
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
      if (!id.isBlank() && hasMetric(metric, "value")) {
        metrics.put(id, text(metric, "value", "0"));
      }
    }
    return metrics;
  }

  private JsonNode vertexMetrics(String jobId, String vertexId, int parallelism) {
    String metricPath = "/jobs/" + jobId + "/vertices/" + vertexId + "/metrics";
    Map<String, List<String>> prefixedMetricIdsByName = prefixedMetricIdsByName(parallelism);
    JsonNode prefixedMetrics = aggregateMetricMap(readMetricValues(metricPath,
        selectedMetricIds(prefixedMetricIdsByName)), prefixedMetricIdsByName);
    if (hasAnyMetric(prefixedMetrics, VERTEX_METRIC_NAMES)) {
      return prefixedMetrics;
    }

    Map<String, List<String>> metricIdsByName = vertexMetricIdsByName(readOrEmpty(metricPath));
    if (!metricIdsByName.isEmpty()) {
      JsonNode aggregated = aggregateMetricMap(
          readMetricValues(metricPath, selectedMetricIds(metricIdsByName)),
          metricIdsByName);
      if (hasAnyMetric(aggregated, VERTEX_METRIC_NAMES)) {
        return aggregated;
      }
    }
    return metricMap(readOrEmpty(metricPath + "?get=" + VERTEX_METRICS));
  }

  private static Map<String, List<String>> prefixedMetricIdsByName(int parallelism) {
    if (parallelism <= 0) {
      return Map.of();
    }
    Map<String, List<String>> values = new LinkedHashMap<>();
    for (String metricName : VERTEX_METRIC_NAMES) {
      List<String> ids = new ArrayList<>();
      for (int subtask = 0; subtask < parallelism; subtask++) {
        ids.add(subtask + "." + metricName);
      }
      values.put(metricName, List.copyOf(ids));
    }
    return values;
  }

  private JsonNode readMetricValues(String metricPath, List<String> metricIds) {
    var values = MAPPER.createArrayNode();
    for (int start = 0; start < metricIds.size(); start += METRIC_QUERY_BATCH_SIZE) {
      int end = Math.min(metricIds.size(), start + METRIC_QUERY_BATCH_SIZE);
      JsonNode batch = readOrEmpty(metricPath + "?get=" + String.join(",", metricIds.subList(start, end)));
      if (batch.isArray()) {
        for (JsonNode metric : batch) {
          values.add(metric);
        }
      }
    }
    return values;
  }

  private static Map<String, List<String>> vertexMetricIdsByName(JsonNode metricsResponse) {
    if (!metricsResponse.isArray()) {
      return Map.of();
    }
    Map<String, List<String>> exactIds = emptyMetricIdMap();
    Map<String, List<String>> subtaskIds = emptyMetricIdMap();
    Map<String, List<String>> operatorScopedIds = emptyMetricIdMap();
    for (JsonNode metric : metricsResponse) {
      String id = text(metric, "id", "");
      if (id.isBlank()) {
        continue;
      }
      for (String metricName : VERTEX_METRIC_NAMES) {
        if (id.equals(metricName)) {
          exactIds.get(metricName).add(id);
        } else if (isSubtaskMetricId(id, metricName)) {
          subtaskIds.get(metricName).add(id);
        } else if (isOperatorScopedMetricId(id, metricName)) {
          operatorScopedIds.get(metricName).add(id);
        }
      }
    }

    Map<String, List<String>> selected = new LinkedHashMap<>();
    for (String metricName : VERTEX_METRIC_NAMES) {
      List<String> ids = firstNonEmpty(exactIds.get(metricName), subtaskIds.get(metricName),
          operatorScopedIds.get(metricName));
      if (!ids.isEmpty()) {
        selected.put(metricName, List.copyOf(ids));
      }
    }
    return selected;
  }

  private static Map<String, List<String>> emptyMetricIdMap() {
    Map<String, List<String>> values = new LinkedHashMap<>();
    for (String metricName : VERTEX_METRIC_NAMES) {
      values.put(metricName, new ArrayList<>());
    }
    return values;
  }

  @SafeVarargs
  private static <T> List<T> firstNonEmpty(List<T>... values) {
    for (List<T> value : values) {
      if (value != null && !value.isEmpty()) {
        return value;
      }
    }
    return List.of();
  }

  private static boolean isSubtaskMetricId(String id, String metricName) {
    int dot = id.indexOf('.');
    return dot > 0 && allDigits(id.substring(0, dot)) && id.substring(dot + 1).equals(metricName);
  }

  private static boolean isOperatorScopedMetricId(String id, String metricName) {
    int dot = id.indexOf('.');
    return dot > 0 && allDigits(id.substring(0, dot)) && id.endsWith("." + metricName);
  }

  private static boolean allDigits(String value) {
    if (value.isBlank()) {
      return false;
    }
    for (int i = 0; i < value.length(); i++) {
      if (!Character.isDigit(value.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  private static List<String> selectedMetricIds(Map<String, List<String>> metricIdsByName) {
    Set<String> ids = new LinkedHashSet<>();
    for (List<String> metricIds : metricIdsByName.values()) {
      ids.addAll(metricIds);
    }
    return List.copyOf(ids);
  }

  private static JsonNode aggregateMetricMap(JsonNode metricsResponse, Map<String, List<String>> metricIdsByName) {
    if (!metricsResponse.isArray()) {
      return MAPPER.createObjectNode();
    }
    Map<String, List<Double>> valuesByName = new LinkedHashMap<>();
    for (String metricName : VERTEX_METRIC_NAMES) {
      valuesByName.put(metricName, new ArrayList<>());
    }
    for (JsonNode metric : metricsResponse) {
      String id = text(metric, "id", "");
      String metricName = metricNameForId(id, metricIdsByName);
      if (metricName.isBlank() || !hasMetric(metric, "value")) {
        continue;
      }
      double value = metricDouble(metric, "value", Double.NaN);
      if (Double.isFinite(value)) {
        valuesByName.get(metricName).add(value);
      }
    }

    var metrics = MAPPER.createObjectNode();
    for (Map.Entry<String, List<Double>> entry : valuesByName.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        metrics.put(entry.getKey(), aggregateMetric(entry.getKey(), entry.getValue()));
      }
    }
    return metrics;
  }

  private static String metricNameForId(String id, Map<String, List<String>> metricIdsByName) {
    for (Map.Entry<String, List<String>> entry : metricIdsByName.entrySet()) {
      if (entry.getValue().contains(id)) {
        return entry.getKey();
      }
    }
    return "";
  }

  private static double metricDouble(JsonNode metrics, String field, double defaultValue) {
    JsonNode value = metrics.path(field);
    if (value.isNumber()) {
      return value.asDouble();
    }
    if (value.isTextual()) {
      try {
        return Double.parseDouble(value.asText());
      } catch (NumberFormatException ignored) {
        return defaultValue;
      }
    }
    return defaultValue;
  }

  private static double aggregateMetric(String metricName, List<Double> values) {
    if (AVERAGE_VERTEX_METRICS.contains(metricName)) {
      return values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0d);
    }
    if (WATERMARK_VERTEX_METRICS.contains(metricName)) {
      double minNonNegative = values.stream()
          .mapToDouble(Double::doubleValue)
          .filter(value -> value >= 0.0d)
          .min()
          .orElse(Double.NaN);
      return Double.isFinite(minNonNegative)
          ? minNonNegative
          : values.stream().mapToDouble(Double::doubleValue).max().orElse(-1.0d);
    }
    return values.stream().mapToDouble(Double::doubleValue).sum();
  }

  private static boolean hasMetric(JsonNode metrics, String field) {
    JsonNode value = metrics.path(field);
    return !value.isMissingNode() && !value.isNull();
  }

  private static boolean hasAnyMetric(JsonNode metrics, String... fields) {
    for (String field : fields) {
      if (hasMetric(metrics, field)) {
        return true;
      }
    }
    return false;
  }

  private static FlinkOperatorSnapshot operatorSnapshot(JsonNode vertex, JsonNode vertexMetrics,
      long flinkMarkerP95Ms, List<FlinkMarkerLatencySnapshot> markerLatencies) {
    double busyMs = metricNumber(vertexMetrics, "busyTimeMsPerSecond");
    double idleMs = metricNumber(vertexMetrics, "idleTimeMsPerSecond");
    double backpressureMs = metricNumber(vertexMetrics, "backPressuredTimeMsPerSecond");
    return new FlinkOperatorSnapshot(
        text(vertex, "id", ""),
        text(vertex, "name", ""),
        vertex.path("parallelism").asInt(0),
        metricNumber(vertexMetrics, "numRecordsInPerSecond"),
        metricNumber(vertexMetrics, "numRecordsOutPerSecond"),
        metricNumber(vertexMetrics, "numRecordsIn"),
        metricNumber(vertexMetrics, "numRecordsOut"),
        metricNumber(vertexMetrics, "numBytesInPerSecond"),
        metricNumber(vertexMetrics, "numBytesOutPerSecond"),
        ratioFromMillisPerSecond(busyMs),
        ratioFromMillisPerSecond(idleMs),
        ratioFromMillisPerSecond(backpressureMs),
        metricLong(vertexMetrics, "pendingRecords"),
        metricLong(vertexMetrics, "currentInputWatermark", -1),
        metricLong(vertexMetrics, "currentOutputWatermark", -1),
        flinkMarkerP95Ms,
        markerLatencies,
        hasAnyMetric(vertexMetrics, VERTEX_METRIC_NAMES));
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
        && !value.contains("mailbox")
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
