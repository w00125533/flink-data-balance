package com.fdb.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public record TopologyMetricsSnapshot(
    boolean present,
    long generatedRecords,
    long siteCount,
    long frequencyBandCount,
    long generationDurationMs,
    long publishDurationMs,
    long totalDurationMs,
    long publishedRecords,
    long publishFailures,
    double minLatitude,
    double maxLatitude,
    double minLongitude,
    double maxLongitude) {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static TopologyMetricsSnapshot empty() {
    return new TopologyMetricsSnapshot(false, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
  }

  public static Path metricsFile(Path outputRoot, BenchmarkRunPlan plan) {
    return outputRoot.resolve(plan.benchmarkId()).resolve("runs").resolve(plan.runId()).resolve("topology-metrics.json");
  }

  public static TopologyMetricsSnapshot read(Path metricsFile) throws IOException {
    if (!Files.exists(metricsFile)) {
      return empty();
    }
    RawTopologyMetrics raw = MAPPER.readValue(metricsFile.toFile(), RawTopologyMetrics.class);
    return new TopologyMetricsSnapshot(
        true,
        raw.generatedRecords,
        raw.siteCount,
        raw.frequencyBandCount,
        raw.generationDurationMs,
        raw.publishDurationMs,
        raw.totalDurationMs,
        raw.publishedRecords,
        raw.publishFailures,
        raw.minLatitude,
        raw.maxLatitude,
        raw.minLongitude,
        raw.maxLongitude);
  }

  private static final class RawTopologyMetrics {
    public long generatedRecords;
    public long siteCount;
    public long frequencyBandCount;
    public long generationDurationMs;
    public long publishDurationMs;
    public long totalDurationMs;
    public long publishedRecords;
    public long publishFailures;
    public double minLatitude;
    public double maxLatitude;
    public double minLongitude;
    public double maxLongitude;
  }
}
