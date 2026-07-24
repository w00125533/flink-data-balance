package com.fdb.benchmark;

import java.util.List;

public record FdbMetricsSnapshot(
    long sourceDelayP95Ms,
    long kpi1mP95Ms,
    long kpi5mP95Ms,
    long sinkP95Ms,
    long connectorWriteP95Ms,
    long connectorCommitP95Ms,
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
    this(sourceDelayP95Ms, kpi1mP95Ms, kpi5mP95Ms, sinkP95Ms, -1L, -1L, sinkFailures, watermarkLagMs,
        List.of(), List.of());
  }

  public FdbMetricsSnapshot(
      long sourceDelayP95Ms,
      long kpi1mP95Ms,
      long kpi5mP95Ms,
      long sinkP95Ms,
      long sinkFailures,
      long watermarkLagMs,
      List<StageLatencySnapshot> stageLatencies,
      List<SinkLatencySnapshot> sinkLatencies) {
    this(sourceDelayP95Ms, kpi1mP95Ms, kpi5mP95Ms, sinkP95Ms, -1L, -1L, sinkFailures, watermarkLagMs,
        stageLatencies, sinkLatencies);
  }

  public FdbMetricsSnapshot {
    stageLatencies = stageLatencies == null ? List.of() : List.copyOf(stageLatencies);
    sinkLatencies = sinkLatencies == null ? List.of() : List.copyOf(sinkLatencies);
  }

  public FdbMetricsSnapshot withKpi1mP95Ms(long value) {
    return new FdbMetricsSnapshot(sourceDelayP95Ms, value, kpi5mP95Ms, sinkP95Ms, connectorWriteP95Ms,
        connectorCommitP95Ms, sinkFailures, watermarkLagMs, stageLatencies, sinkLatencies);
  }

  public FdbMetricsSnapshot withSinkP95Ms(long value) {
    return new FdbMetricsSnapshot(sourceDelayP95Ms, kpi1mP95Ms, kpi5mP95Ms, value, connectorWriteP95Ms,
        connectorCommitP95Ms, sinkFailures, watermarkLagMs, stageLatencies, sinkLatencies);
  }

  public FdbMetricsSnapshot withConnectorWriteP95Ms(long value) {
    return new FdbMetricsSnapshot(sourceDelayP95Ms, kpi1mP95Ms, kpi5mP95Ms, sinkP95Ms, value,
        connectorCommitP95Ms, sinkFailures, watermarkLagMs, stageLatencies, sinkLatencies);
  }

  public FdbMetricsSnapshot withConnectorCommitP95Ms(long value) {
    return new FdbMetricsSnapshot(sourceDelayP95Ms, kpi1mP95Ms, kpi5mP95Ms, sinkP95Ms, connectorWriteP95Ms,
        value, sinkFailures, watermarkLagMs, stageLatencies, sinkLatencies);
  }
}
