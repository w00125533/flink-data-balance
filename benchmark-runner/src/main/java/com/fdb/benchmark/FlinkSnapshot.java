package com.fdb.benchmark;

public record FlinkSnapshot(
    String jobStatus,
    double backpressureRatio,
    long checkpointDurationMs,
    int consecutiveCheckpointFailures,
    double recordsInPerSec,
    double recordsOutPerSec,
    int taskManagers,
    int slots) {

  public FlinkSnapshot withJobStatus(String value) {
    return new FlinkSnapshot(value, backpressureRatio, checkpointDurationMs, consecutiveCheckpointFailures,
        recordsInPerSec, recordsOutPerSec, taskManagers, slots);
  }

  public FlinkSnapshot withBackpressureRatio(double value) {
    return new FlinkSnapshot(jobStatus, value, checkpointDurationMs, consecutiveCheckpointFailures,
        recordsInPerSec, recordsOutPerSec, taskManagers, slots);
  }
}
