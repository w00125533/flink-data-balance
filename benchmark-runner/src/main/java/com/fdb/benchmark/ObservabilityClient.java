package com.fdb.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public final class ObservabilityClient {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final URI baseUri;
  private final HttpGateway http;

  public ObservabilityClient(URI baseUri, HttpGateway http) {
    this.baseUri = baseUri;
    this.http = http;
  }

  public FdbMetricsSnapshot snapshot() {
    JsonNode stages = stageArray(readStages());
    List<StageLatencySnapshot> stageLatencies = new ArrayList<>();
    long sourceDelayP95Ms = 0;
    long kpi1mP95Ms = 0;
    long kpi5mP95Ms = 0;
    long watermarkLagMs = 0;
    if (stages.isArray()) {
      for (JsonNode stage : stages) {
        String stageId = stage.path("stageId").asText("");
        long latencyP50Ms = firstLong(stage, "latencyP50Ms", "p50Ms");
        long latencyP95Ms = firstLong(stage, "latencyP95Ms", "p95Ms");
        long latencyP99Ms = firstLong(stage, "latencyP99Ms", "p99Ms");
        long stageWatermarkLagMs = stage.path("watermarkLagMs").asLong(0);
        stageLatencies.add(new StageLatencySnapshot(stageId, latencyP50Ms, latencyP95Ms, latencyP99Ms,
            stageWatermarkLagMs));
        watermarkLagMs = Math.max(watermarkLagMs, stageWatermarkLagMs);
        if (stageId.contains("source")) {
          sourceDelayP95Ms = Math.max(sourceDelayP95Ms, latencyP95Ms);
        }
        if (stageId.contains("1m") || stageId.contains("kpi-1")) {
          kpi1mP95Ms = Math.max(kpi1mP95Ms, latencyP95Ms);
        }
        if (stageId.contains("5m") || stageId.contains("kpi-5")) {
          kpi5mP95Ms = Math.max(kpi5mP95Ms, latencyP95Ms);
        }
      }
    }

    JsonNode sinks = readOrEmpty("/api/results/sink-latency");
    List<SinkLatencySnapshot> sinkLatencies = new ArrayList<>();
    long sinkP95Ms = 0;
    long sinkFailures = 0;
    if (sinks.isArray()) {
      for (JsonNode sink : sinks) {
        long latencyP50Ms = firstLong(sink, "latencyP50Ms", "p50Ms");
        long latencyP95Ms = firstLong(sink, "latencyP95Ms", "p95Ms");
        long latencyP99Ms = firstLong(sink, "latencyP99Ms", "p99Ms");
        long failures = firstLong(sink, "failureCount", "failures");
        sinkLatencies.add(new SinkLatencySnapshot(
            sink.path("sinkName").asText(sink.path("sink").asText("")),
            firstLong(sink, "records", "recordCount"),
            firstLong(sink, "bytes", "byteCount"),
            latencyP50Ms,
            latencyP95Ms,
            latencyP99Ms,
            failures));
        sinkP95Ms = Math.max(sinkP95Ms, latencyP95Ms);
        sinkFailures += failures;
      }
    }
    return new FdbMetricsSnapshot(sourceDelayP95Ms, kpi1mP95Ms, kpi5mP95Ms, sinkP95Ms, sinkFailures,
        watermarkLagMs, stageLatencies, sinkLatencies);
  }

  private JsonNode readStages() {
    JsonNode status = readOrEmpty("/api/flow/status");
    if (!stageArray(status).isEmpty()) {
      return status;
    }
    return readOrEmpty("/api/flow/stages");
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

  private static JsonNode stageArray(JsonNode node) {
    if (node.isArray()) {
      return node;
    }
    if (node.path("stages").isArray()) {
      return node.path("stages");
    }
    return MAPPER.createArrayNode();
  }

  private static long firstLong(JsonNode node, String... fields) {
    for (String field : fields) {
      if (node.has(field)) {
        return node.path(field).asLong(0);
      }
    }
    return 0;
  }
}
