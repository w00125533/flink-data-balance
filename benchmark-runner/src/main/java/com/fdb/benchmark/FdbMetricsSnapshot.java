package com.fdb.benchmark;

import java.util.List;

public record FdbMetricsSnapshot(
    long sourceDelayP95Ms,
    long kpi1mP95Ms,
    long kpi5mP95Ms,
    long sinkP95Ms,
    long sinkFailures,
    long watermarkLagMs,
    List<StageLatencySnapshot> stageLatencies,
    List<SinkLatencySnapshot> sinkLatencies) {

  public FdbMetricsSnapshot(
      long sourceDelayP95Ms,
      long kpi1mP95Ms,
      long kpi5mP95Ms,
      long sinkP95Ms,
      long sinkFailures,
      long watermarkLagMs) {
    this(sourceDelayP95Ms, kpi1mP95Ms, kpi5mP95Ms, sinkP95Ms, sinkFailures, watermarkLagMs,
        List.of(), List.of());
  }

  public FdbMetricsSnapshot {
    stageLatencies = stageLatencies == null ? List.of() : List.copyOf(stageLatencies);
    sinkLatencies = sinkLatencies == null ? List.of() : List.copyOf(sinkLatencies);
  }

  public FdbMetricsSnapshot withKpi1mP95Ms(long value) {
    return new FdbMetricsSnapshot(sourceDelayP95Ms, value, kpi5mP95Ms, sinkP95Ms, sinkFailures, watermarkLagMs,
        stageLatencies, sinkLatencies);
  }

  public FdbMetricsSnapshot withSinkP95Ms(long value) {
    return new FdbMetricsSnapshot(sourceDelayP95Ms, kpi1mP95Ms, kpi5mP95Ms, value, sinkFailures, watermarkLagMs,
        stageLatencies, sinkLatencies);
  }
}
