package com.fdb.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;

public final class FlinkRestClient {
  private static final ObjectMapper MAPPER = new ObjectMapper();

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

    if (!jobId.isBlank()) {
      JsonNode checkpoints = readOrEmpty("/jobs/" + jobId + "/checkpoints");
      checkpointDurationMs = checkpoints.path("latest").path("completed").path("duration").asLong(0);
      consecutiveCheckpointFailures = checkpoints.path("counts").path("failed").asInt(0);

      JsonNode vertices = readOrEmpty("/jobs/" + jobId + "/vertices").path("vertices");
      if (vertices.isArray()) {
        for (JsonNode vertex : vertices) {
          JsonNode metrics = vertex.path("metrics");
          recordsInPerSec += metricNumber(metrics, "read-records");
          recordsOutPerSec += metricNumber(metrics, "write-records");
        }
      }
    }

    return new FlinkSnapshot(status, 0.0, checkpointDurationMs, consecutiveCheckpointFailures,
        recordsInPerSec, recordsOutPerSec, 0, 0);
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
}
