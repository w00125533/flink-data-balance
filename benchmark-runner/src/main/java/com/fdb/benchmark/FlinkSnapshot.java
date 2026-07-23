package com.fdb.benchmark;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record FlinkSnapshot(
    String jobStatus,
    double backpressureRatio,
    long checkpointDurationMs,
    int consecutiveCheckpointFailures,
    double recordsInPerSec,
    double recordsOutPerSec,
    double recordsInTotal,
    double recordsOutTotal,
    int taskManagers,
    int slots,
    List<FlinkOperatorSnapshot> operators,
    List<FlinkOperatorEdge> operatorEdges,
    long sourceBacklogRecords) {

  public FlinkSnapshot(
      String jobStatus,
      double backpressureRatio,
      long checkpointDurationMs,
      int consecutiveCheckpointFailures,
      double recordsInPerSec,
      double recordsOutPerSec,
      int taskManagers,
      int slots) {
    this(jobStatus, backpressureRatio, checkpointDurationMs, consecutiveCheckpointFailures,
        recordsInPerSec, recordsOutPerSec, 0, 0, taskManagers, slots, List.of(), List.of(), 0);
  }

  public FlinkSnapshot(
      String jobStatus,
      double backpressureRatio,
      long checkpointDurationMs,
      int consecutiveCheckpointFailures,
      double recordsInPerSec,
      double recordsOutPerSec,
      double recordsInTotal,
      double recordsOutTotal,
      int taskManagers,
      int slots) {
    this(jobStatus, backpressureRatio, checkpointDurationMs, consecutiveCheckpointFailures,
        recordsInPerSec, recordsOutPerSec, recordsInTotal, recordsOutTotal, taskManagers, slots, List.of(), List.of(), 0);
  }

  public FlinkSnapshot(
      String jobStatus,
      double backpressureRatio,
      long checkpointDurationMs,
      int consecutiveCheckpointFailures,
      double recordsInPerSec,
      double recordsOutPerSec,
      int taskManagers,
      int slots,
      List<FlinkOperatorSnapshot> operators) {
    this(jobStatus, backpressureRatio, checkpointDurationMs, consecutiveCheckpointFailures,
        recordsInPerSec, recordsOutPerSec, 0, 0, taskManagers, slots, operators, List.of(), 0);
  }

  public FlinkSnapshot(
      String jobStatus,
      double backpressureRatio,
      long checkpointDurationMs,
      int consecutiveCheckpointFailures,
      double recordsInPerSec,
      double recordsOutPerSec,
      double recordsInTotal,
      double recordsOutTotal,
      int taskManagers,
      int slots,
      List<FlinkOperatorSnapshot> operators) {
    this(jobStatus, backpressureRatio, checkpointDurationMs, consecutiveCheckpointFailures,
        recordsInPerSec, recordsOutPerSec, recordsInTotal, recordsOutTotal, taskManagers, slots, operators, List.of(), 0);
  }

  public FlinkSnapshot(
      String jobStatus,
      double backpressureRatio,
      long checkpointDurationMs,
      int consecutiveCheckpointFailures,
      double recordsInPerSec,
      double recordsOutPerSec,
      double recordsInTotal,
      double recordsOutTotal,
      int taskManagers,
      int slots,
      List<FlinkOperatorSnapshot> operators,
      List<FlinkOperatorEdge> operatorEdges) {
    this(jobStatus, backpressureRatio, checkpointDurationMs, consecutiveCheckpointFailures,
        recordsInPerSec, recordsOutPerSec, recordsInTotal, recordsOutTotal, taskManagers, slots, operators,
        operatorEdges, 0);
  }

  public FlinkSnapshot {
    operators = operators == null ? List.of() : List.copyOf(operators);
    operatorEdges = operatorEdges == null ? List.of() : List.copyOf(operatorEdges);
  }

  public FlinkSnapshot withJobStatus(String value) {
    return new FlinkSnapshot(value, backpressureRatio, checkpointDurationMs, consecutiveCheckpointFailures,
        recordsInPerSec, recordsOutPerSec, recordsInTotal, recordsOutTotal, taskManagers, slots, operators,
        operatorEdges, sourceBacklogRecords);
  }

  public FlinkSnapshot withBackpressureRatio(double value) {
    return new FlinkSnapshot(jobStatus, value, checkpointDurationMs, consecutiveCheckpointFailures,
        recordsInPerSec, recordsOutPerSec, recordsInTotal, recordsOutTotal, taskManagers, slots, operators,
        operatorEdges, sourceBacklogRecords);
  }

  public FlinkSnapshot withSourceBacklogRecords(long value) {
    return new FlinkSnapshot(jobStatus, backpressureRatio, checkpointDurationMs, consecutiveCheckpointFailures,
        recordsInPerSec, recordsOutPerSec, recordsInTotal, recordsOutTotal, taskManagers, slots, operators,
        operatorEdges, value);
  }

  public static FlinkSnapshot measurementWindow(List<FlinkSnapshot> snapshots) {
    if (snapshots == null || snapshots.isEmpty()) {
      return new FlinkSnapshot("UNKNOWN", 0, 0, 0, 0, 0, 0, 0);
    }
    FlinkSnapshot last = snapshots.get(snapshots.size() - 1);
    if (snapshots.size() == 1) {
      return last;
    }
    return new FlinkSnapshot(
        last.jobStatus(),
        maxDouble(snapshots, FlinkSnapshot::backpressureRatio),
        maxLong(snapshots, FlinkSnapshot::checkpointDurationMs),
        maxInt(snapshots, FlinkSnapshot::consecutiveCheckpointFailures),
        averagePositive(snapshots.stream().mapToDouble(FlinkSnapshot::recordsInPerSec).toArray(),
            last.recordsInPerSec()),
        averagePositive(snapshots.stream().mapToDouble(FlinkSnapshot::recordsOutPerSec).toArray(),
            last.recordsOutPerSec()),
        last.recordsInTotal(),
        last.recordsOutTotal(),
        last.taskManagers(),
        last.slots(),
        measurementWindowOperators(snapshots),
        last.operatorEdges(),
        maxSourceBacklogRecords(snapshots));
  }

  private static List<FlinkOperatorSnapshot> measurementWindowOperators(List<FlinkSnapshot> snapshots) {
    Map<String, List<FlinkOperatorSnapshot>> byId = new LinkedHashMap<>();
    for (FlinkSnapshot snapshot : snapshots) {
      for (FlinkOperatorSnapshot operator : snapshot.operators()) {
        byId.computeIfAbsent(operator.id(), ignored -> new ArrayList<>()).add(operator);
      }
    }
    List<FlinkOperatorSnapshot> operators = new ArrayList<>();
    for (List<FlinkOperatorSnapshot> samples : byId.values()) {
      FlinkOperatorSnapshot last = samples.get(samples.size() - 1);
      operators.add(new FlinkOperatorSnapshot(
          last.id(),
          last.name(),
          last.parallelism(),
          averagePositive(samples.stream().mapToDouble(FlinkOperatorSnapshot::recordsInPerSec).toArray(),
              last.recordsInPerSec()),
          averagePositive(samples.stream().mapToDouble(FlinkOperatorSnapshot::recordsOutPerSec).toArray(),
              last.recordsOutPerSec()),
          last.recordsInTotal(),
          last.recordsOutTotal(),
          averagePositive(samples.stream().mapToDouble(FlinkOperatorSnapshot::bytesInPerSec).toArray(),
              last.bytesInPerSec()),
          averagePositive(samples.stream().mapToDouble(FlinkOperatorSnapshot::bytesOutPerSec).toArray(),
              last.bytesOutPerSec()),
          averagePositive(samples.stream().mapToDouble(FlinkOperatorSnapshot::busyRatio).toArray(),
              last.busyRatio()),
          averagePositive(samples.stream().mapToDouble(FlinkOperatorSnapshot::idleRatio).toArray(),
              last.idleRatio()),
          maxDoubleOperator(samples, FlinkOperatorSnapshot::backpressureRatio),
          samples.stream().mapToLong(FlinkOperatorSnapshot::pendingRecords).max().orElse(last.pendingRecords()),
          last.currentInputWatermarkMs(),
          last.currentOutputWatermarkMs(),
          maxLongOperator(samples, FlinkOperatorSnapshot::flinkMarkerP95Ms),
          maxMarkerLatencies(samples)));
    }
    return List.copyOf(operators);
  }

  private static List<FlinkMarkerLatencySnapshot> maxMarkerLatencies(List<FlinkOperatorSnapshot> samples) {
    Map<String, FlinkMarkerLatencySnapshot> byEdge = new LinkedHashMap<>();
    for (FlinkOperatorSnapshot sample : samples) {
      for (FlinkMarkerLatencySnapshot marker : sample.flinkMarkerLatencies()) {
        String key = marker.sourceOperatorId() + "\u0000" + marker.targetOperatorId();
        FlinkMarkerLatencySnapshot existing = byEdge.get(key);
        if (existing == null || marker.p95Ms() > existing.p95Ms()) {
          byEdge.put(key, marker);
        }
      }
    }
    return List.copyOf(byEdge.values());
  }

  private static double averagePositive(double[] values, double fallback) {
    double total = 0.0d;
    int count = 0;
    for (double value : values) {
      if (Double.isFinite(value) && value > 0.0d) {
        total += value;
        count++;
      }
    }
    return count == 0 ? fallback : total / count;
  }

  private static double maxDouble(List<FlinkSnapshot> snapshots,
      java.util.function.ToDoubleFunction<FlinkSnapshot> value) {
    return snapshots.stream().mapToDouble(value).max().orElse(0.0d);
  }

  private static double maxDoubleOperator(List<FlinkOperatorSnapshot> snapshots,
      java.util.function.ToDoubleFunction<FlinkOperatorSnapshot> value) {
    return snapshots.stream().mapToDouble(value).max().orElse(0.0d);
  }

  private static long maxLongOperator(List<FlinkOperatorSnapshot> snapshots,
      java.util.function.ToLongFunction<FlinkOperatorSnapshot> value) {
    return snapshots.stream().mapToLong(value).max().orElse(-1L);
  }

  private static long maxLong(List<FlinkSnapshot> snapshots, java.util.function.ToLongFunction<FlinkSnapshot> value) {
    return snapshots.stream().mapToLong(value).max().orElse(0L);
  }

  private static int maxInt(List<FlinkSnapshot> snapshots, java.util.function.ToIntFunction<FlinkSnapshot> value) {
    return snapshots.stream().mapToInt(value).max().orElse(0);
  }

  private static long maxSourceBacklogRecords(List<FlinkSnapshot> snapshots) {
    long max = snapshots.stream().mapToLong(FlinkSnapshot::sourceBacklogRecords).max().orElse(0L);
    for (FlinkSnapshot snapshot : snapshots) {
      for (FlinkOperatorSnapshot operator : snapshot.operators()) {
        if (operator.name().toLowerCase(java.util.Locale.ROOT).contains("source")) {
          max = Math.max(max, operator.pendingRecords());
        }
      }
    }
    return max;
  }
}
