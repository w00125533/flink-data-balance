package com.fdb.benchmark;

import java.util.List;

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
        recordsInPerSec, recordsOutPerSec, 0, 0, taskManagers, slots, List.of(), 0);
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
        recordsInPerSec, recordsOutPerSec, recordsInTotal, recordsOutTotal, taskManagers, slots, List.of(), 0);
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
        recordsInPerSec, recordsOutPerSec, 0, 0, taskManagers, slots, operators, 0);
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
        recordsInPerSec, recordsOutPerSec, recordsInTotal, recordsOutTotal, taskManagers, slots, operators, 0);
  }

  public FlinkSnapshot {
    operators = operators == null ? List.of() : List.copyOf(operators);
  }

  public FlinkSnapshot withJobStatus(String value) {
    return new FlinkSnapshot(value, backpressureRatio, checkpointDurationMs, consecutiveCheckpointFailures,
        recordsInPerSec, recordsOutPerSec, recordsInTotal, recordsOutTotal, taskManagers, slots, operators,
        sourceBacklogRecords);
  }

  public FlinkSnapshot withBackpressureRatio(double value) {
    return new FlinkSnapshot(jobStatus, value, checkpointDurationMs, consecutiveCheckpointFailures,
        recordsInPerSec, recordsOutPerSec, recordsInTotal, recordsOutTotal, taskManagers, slots, operators,
        sourceBacklogRecords);
  }

  public FlinkSnapshot withSourceBacklogRecords(long value) {
    return new FlinkSnapshot(jobStatus, backpressureRatio, checkpointDurationMs, consecutiveCheckpointFailures,
        recordsInPerSec, recordsOutPerSec, recordsInTotal, recordsOutTotal, taskManagers, slots, operators, value);
  }
}
