package com.fdb.benchmark;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public record SourceMetricsSnapshot(
    boolean present,
    long chrTargetEps,
    long chrPublished,
    double chrObservedEps,
    long pmPublished,
    double pmObservedEps,
    long cfgPublished,
    double cfgObservedEps) {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static SourceMetricsSnapshot empty() {
    return new SourceMetricsSnapshot(false, 0, 0, 0, 0, 0, 0, 0);
  }

  public static Path runDir(Path outputRoot, BenchmarkRunPlan plan) {
    return outputRoot.resolve(plan.benchmarkId()).resolve("runs").resolve(plan.runId());
  }

  public static SourceMetricsSnapshot read(Path outputRoot, BenchmarkRunPlan plan) throws IOException {
    Path runDir = runDir(outputRoot, plan);
    RawSourceMetrics chr = readRaw(runDir.resolve("chr-source-metrics.json"));
    RawSourceMetrics pm = readRaw(runDir.resolve("pm-source-metrics.json"));
    RawSourceMetrics cfg = readRaw(runDir.resolve("cfg-source-metrics.json"));
    boolean present = chr != null || pm != null || cfg != null;
    return new SourceMetricsSnapshot(
        present,
        chr == null ? 0L : chr.targetEps,
        chr == null ? 0L : chr.published,
        chr == null ? 0.0d : chr.observedEps,
        pm == null ? 0L : pm.published,
        pm == null ? 0.0d : pm.observedEps,
        cfg == null ? 0L : cfg.published,
        cfg == null ? 0.0d : cfg.observedEps);
  }

  public double producerDeliveryRatio() {
    return chrTargetEps <= 0 ? 1.0d : chrObservedEps / chrTargetEps;
  }

  public double chrTotalPerCell(int cellLevel) {
    return perCell(chrPublished, cellLevel);
  }

  public double chrPerSecondPerCell(int cellLevel) {
    return perCell(chrObservedEps, cellLevel);
  }

  public double pmTotalPerCell(int cellLevel) {
    return perCell(pmPublished, cellLevel);
  }

  public double pmPerSecondPerCell(int cellLevel) {
    return perCell(pmObservedEps, cellLevel);
  }

  public double cfgTotalPerCell(int cellLevel) {
    return perCell(cfgPublished, cellLevel);
  }

  public double cfgPerSecondPerCell(int cellLevel) {
    return perCell(cfgObservedEps, cellLevel);
  }

  private static double perCell(double value, int cellLevel) {
    return cellLevel <= 0 ? 0.0d : value / cellLevel;
  }

  private static RawSourceMetrics readRaw(Path path) throws IOException {
    if (!Files.exists(path)) {
      return null;
    }
    return MAPPER.readValue(path.toFile(), RawSourceMetrics.class);
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  private static final class RawSourceMetrics {
    public long targetEps;
    public long published;
    public double observedEps;
  }
}
