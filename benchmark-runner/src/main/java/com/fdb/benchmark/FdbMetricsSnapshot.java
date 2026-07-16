package com.fdb.benchmark;

public record FdbMetricsSnapshot(
    long sourceDelayP95Ms,
    long kpi1mP95Ms,
    long kpi5mP95Ms,
    long sinkP95Ms,
    long sinkFailures,
    long watermarkLagMs) {

  public FdbMetricsSnapshot withKpi1mP95Ms(long value) {
    return new FdbMetricsSnapshot(sourceDelayP95Ms, value, kpi5mP95Ms, sinkP95Ms, sinkFailures, watermarkLagMs);
  }

  public FdbMetricsSnapshot withSinkP95Ms(long value) {
    return new FdbMetricsSnapshot(sourceDelayP95Ms, kpi1mP95Ms, kpi5mP95Ms, value, sinkFailures, watermarkLagMs);
  }
}
