package com.fdb.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;

public final class ObservabilityClient {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final URI baseUri;
  private final HttpGateway http;

  public ObservabilityClient(URI baseUri, HttpGateway http) {
    this.baseUri = baseUri;
    this.http = http;
  }

  public FdbMetricsSnapshot snapshot() throws IOException, InterruptedException {
    JsonNode stages = readStages();
    long sourceDelayP95Ms = -1;
    long kpi1mP95Ms = -1;
    long kpi5mP95Ms = -1;
    long watermarkLagMs = 0;
    if (stages.isArray()) {
      for (JsonNode stage : stages) {
        String stageId = stage.path("stageId").asText("");
        long latencyP95Ms = stage.path("latencyP95Ms").asLong(-1);
        watermarkLagMs = Math.max(watermarkLagMs, stage.path("watermarkLagMs").asLong(0));
        if (isSourceDelayStage(stageId)) {
          sourceDelayP95Ms = maxAvailableLatency(sourceDelayP95Ms, latencyP95Ms);
        }
        if (stageId.contains("1m") || stageId.contains("kpi-1")) {
          kpi1mP95Ms = maxAvailableLatency(kpi1mP95Ms, latencyP95Ms);
        }
        if (stageId.contains("5m") || stageId.contains("kpi-5")) {
          kpi5mP95Ms = maxAvailableLatency(kpi5mP95Ms, latencyP95Ms);
        }
      }
    }

    JsonNode sinks = readOrEmpty("/api/results/sink-latency");
    long sinkP95Ms = -1;
    long sinkFailures = 0;
    if (sinks.isArray()) {
      for (JsonNode sink : sinks) {
        sinkP95Ms = maxAvailableLatency(sinkP95Ms, firstLong(sink, "latencyP95Ms", "p95Ms", -1));
        sinkFailures += firstLong(sink, "failureCount", "failures", 0);
      }
    }
    return new FdbMetricsSnapshot(sourceDelayP95Ms, kpi1mP95Ms, kpi5mP95Ms, sinkP95Ms, sinkFailures,
        watermarkLagMs);
  }

  private JsonNode readStages() throws IOException, InterruptedException {
    try {
      return read("/api/flow/status");
    } catch (IOException e) {
      return read("/api/flow/stages");
    }
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
      return MAPPER.createArrayNode();
    }
  }

  private static long maxAvailableLatency(long current, long candidate) {
    return candidate < 0 ? current : Math.max(current, candidate);
  }

  private static boolean isSourceDelayStage(String stageId) {
    String normalized = stageId.toLowerCase();
    return normalized.contains("source")
        && !normalized.contains("sink")
        && !normalized.contains("kpi");
  }

  private static long firstLong(JsonNode node, String first, String second, long defaultValue) {
    if (node.has(first)) {
      return node.path(first).asLong(defaultValue);
    }
    return node.path(second).asLong(defaultValue);
  }
}
