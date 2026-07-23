package com.fdb.benchmark;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public record WindowMaterializationSnapshot(
    long expectedClosedMinuteWindows,
    WindowStage chr,
    WindowStage pm,
    WindowStage kpi) {

  private static final long MINUTE_MS = 60_000L;
  private static final String MATERIALIZATION_SINK_TYPE = "window-materialization";

  public static WindowMaterializationSnapshot from(BenchmarkRunPlan plan, SourceMetricsSnapshot source,
      FdbMetricsSnapshot fdb) {
    long expected = expectedClosedMinuteWindows(source);
    return new WindowMaterializationSnapshot(
        expected,
        stage("CHR 1m", plan.cellLevel(), fdb.sinkLatencies(), "chr-1m"),
        stage("PM 1m", plan.cellLevel(), fdb.sinkLatencies(), "pm-1m"),
        stage("KPI 1m", plan.cellLevel(), fdb.sinkLatencies(), "kpi-1m"));
  }

  public static WindowMaterializationSnapshot from(BenchmarkRunPlan plan, SourceMetricsSnapshot source,
      FlinkSnapshot flink) {
    long expected = expectedClosedMinuteWindows(source);
    return new WindowMaterializationSnapshot(
        expected,
        stage("CHR 1m", plan.cellLevel(), findOperator(flink, "chr-1m-fact")),
        stage("PM 1m", plan.cellLevel(), findOperator(flink, "pm-1m-fact")),
        stage("KPI 1m", plan.cellLevel(), findOperator(flink, "kpi-1m-full-join")));
  }

  public boolean applicable() {
    return expectedClosedMinuteWindows > 0;
  }

  public boolean healthy() {
    return !applicable()
        || (chr.healthy(expectedClosedMinuteWindows)
            && pm.healthy(expectedClosedMinuteWindows)
            && kpi.healthy(expectedClosedMinuteWindows));
  }

  public String observedClosedMinutes() {
    return "CHR=" + chr.closedMinuteWindows()
        + ", PM=" + pm.closedMinuteWindows()
        + ", KPI=" + kpi.closedMinuteWindows()
        + " closed minutes";
  }

  public String thresholdText() {
    return applicable() ? ">= " + expectedClosedMinuteWindows + " closed minutes" : "N/A";
  }

  public String bottleneckReason() {
    return "1m window materialization lag: expected >= " + expectedClosedMinuteWindows
        + " closed minutes, CHR=" + chr.closedMinuteWindows()
        + ", PM=" + pm.closedMinuteWindows()
        + ", KPI=" + kpi.closedMinuteWindows();
  }

  private static long expectedClosedMinuteWindows(SourceMetricsSnapshot source) {
    long durationMs = minPositive(source.chrDurationMs(), source.pmDurationMs());
    if (durationMs <= 0) {
      return 0;
    }
    return Math.max(0, durationMs / MINUTE_MS - 1);
  }

  private static long minPositive(long left, long right) {
    if (left <= 0 && right <= 0) {
      return 0;
    }
    if (left <= 0) {
      return right;
    }
    if (right <= 0) {
      return left;
    }
    return Math.min(left, right);
  }

  private static WindowStage stage(String label, int cellLevel, FlinkOperatorSnapshot operator) {
    if (operator == null) {
      return new WindowStage(label, "", false, 0, 0, 0, -1, -1);
    }
    return new WindowStage(
        label,
        operator.name(),
        true,
        operator.recordsInTotal(),
        operator.recordsOutTotal(),
        closedMinuteWindows(operator.recordsOutTotal(), cellLevel),
        operator.currentInputWatermarkMs(),
        operator.currentOutputWatermarkMs());
  }

  private static WindowStage stage(String label, int cellLevel, List<SinkLatencySnapshot> sinkLatencies,
      String dataset) {
    Map<String, Long> recordsByWindow = new LinkedHashMap<>();
    String operatorName = "";
    for (SinkLatencySnapshot latency : sinkLatencies) {
      if (!MATERIALIZATION_SINK_TYPE.equalsIgnoreCase(latency.sinkType())
          || !dataset.equalsIgnoreCase(latency.dataset())) {
        continue;
      }
      if (operatorName.isBlank()) {
        operatorName = latency.sinkName();
      }
      recordsByWindow.merge(latency.windowKind(), latency.records(), Long::sum);
    }
    long recordsOutTotal = recordsByWindow.values().stream().mapToLong(Long::longValue).sum();
    long closedMinuteWindows = recordsByWindow.values().stream()
        .filter(records -> records >= cellLevel)
        .count();
    return new WindowStage(label, operatorName, !recordsByWindow.isEmpty(), 0, recordsOutTotal,
        closedMinuteWindows, -1, -1);
  }

  private static long closedMinuteWindows(double recordsOutTotal, int cellLevel) {
    if (cellLevel <= 0 || !Double.isFinite(recordsOutTotal) || recordsOutTotal <= 0) {
      return 0;
    }
    return (long) Math.floor(recordsOutTotal / cellLevel);
  }

  private static FlinkOperatorSnapshot findOperator(FlinkSnapshot flink, String nameToken) {
    String normalized = nameToken.toLowerCase(Locale.ROOT);
    return flink.operators().stream()
        .filter(operator -> operator.name().toLowerCase(Locale.ROOT).contains(normalized))
        .findFirst()
        .orElse(null);
  }

  public record WindowStage(
      String label,
      String operatorName,
      boolean present,
      double recordsInTotal,
      double recordsOutTotal,
      long closedMinuteWindows,
      long currentInputWatermarkMs,
      long currentOutputWatermarkMs) {

    boolean healthy(long expectedClosedMinuteWindows) {
      return expectedClosedMinuteWindows <= 0 || (present && closedMinuteWindows >= expectedClosedMinuteWindows);
    }
  }
}
