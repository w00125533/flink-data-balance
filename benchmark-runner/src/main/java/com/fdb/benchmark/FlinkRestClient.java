package com.fdb.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

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
      "pendingRecords");

  private final URI baseUri;
  private final HttpGateway http;

  public FlinkRestClient(URI baseUri, HttpGateway http) {
    this.baseUri = baseUri;
    this.http = http;
  }

  public FlinkSnapshot snapshot() throws IOException, InterruptedException {
    JsonNode overview = read("/jobs/overview");
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

    JsonNode taskManagersNode = readOrEmpty("/taskmanagers").path("taskmanagers");
    if (taskManagersNode.isArray()) {
      taskManagers = taskManagersNode.size();
      for (JsonNode taskManager : taskManagersNode) {
        slots += firstInt(taskManager, "slotsNumber", "slots");
      }
    }

    if (!jobId.isBlank()) {
      JsonNode checkpoints = readOrEmpty("/jobs/" + jobId + "/checkpoints");
      checkpointDurationMs = checkpoints.path("latest").path("completed").path("duration").asLong(0);
      consecutiveCheckpointFailures = checkpoints.path("counts").path("failed").asInt(0);

      JsonNode vertices = readOrEmpty("/jobs/" + jobId).path("vertices");
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
          FlinkOperatorSnapshot operator = operatorSnapshot(vertex, metrics, rateMetrics);
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
        sourceBacklogRecords);
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
          return 0;
        }
      }
    }
    return 0;
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
      JsonNode rateMetrics) {
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
        metricLong(rateMetrics, "pendingRecords"));
  }

  private static boolean isSourceOperator(FlinkOperatorSnapshot operator) {
    return operator.name().toLowerCase().contains("source");
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
