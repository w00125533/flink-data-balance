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
    long kpi1mP95Ms = 0;
    long kpi5mP95Ms = 0;
    long watermarkLagMs = 0;
    if (stages.isArray()) {
      for (JsonNode stage : stages) {
        String stageId = stage.path("stageId").asText("");
        long latencyP95Ms = stage.path("latencyP95Ms").asLong(0);
        watermarkLagMs = Math.max(watermarkLagMs, stage.path("watermarkLagMs").asLong(0));
        if (stageId.contains("1m") || stageId.contains("kpi-1")) {
          kpi1mP95Ms = Math.max(kpi1mP95Ms, latencyP95Ms);
        }
        if (stageId.contains("5m") || stageId.contains("kpi-5")) {
          kpi5mP95Ms = Math.max(kpi5mP95Ms, latencyP95Ms);
        }
      }
    }

    JsonNode sinks = readOrEmpty("/api/results/sink-latency");
    long sinkP95Ms = 0;
    long sinkFailures = 0;
    if (sinks.isArray()) {
      for (JsonNode sink : sinks) {
        sinkP95Ms = Math.max(sinkP95Ms, firstLong(sink, "latencyP95Ms", "p95Ms"));
        sinkFailures += firstLong(sink, "failureCount", "failures");
      }
    }
    return new FdbMetricsSnapshot(0, kpi1mP95Ms, kpi5mP95Ms, sinkP95Ms, sinkFailures, watermarkLagMs);
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

  private static long firstLong(JsonNode node, String first, String second) {
    if (node.has(first)) {
      return node.path(first).asLong(0);
    }
    return node.path(second).asLong(0);
  }
}
