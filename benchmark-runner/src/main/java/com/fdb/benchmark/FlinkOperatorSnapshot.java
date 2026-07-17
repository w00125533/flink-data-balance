package com.fdb.benchmark;

public record FlinkOperatorSnapshot(
    String id,
    String name,
    int parallelism,
    double recordsInPerSec,
    double recordsOutPerSec,
    double recordsInTotal,
    double recordsOutTotal,
    double bytesInPerSec,
    double bytesOutPerSec,
    double busyRatio,
    double idleRatio,
    double backpressureRatio,
    long pendingRecords) {

  public FlinkOperatorSnapshot(
      String id,
      String name,
      int parallelism,
      double recordsInPerSec,
      double recordsOutPerSec,
      double recordsInTotal,
      double recordsOutTotal,
      double bytesInPerSec,
      double bytesOutPerSec,
      double busyRatio,
      double idleRatio,
      double backpressureRatio) {
    this(id, name, parallelism, recordsInPerSec, recordsOutPerSec, recordsInTotal, recordsOutTotal,
        bytesInPerSec, bytesOutPerSec, busyRatio, idleRatio, backpressureRatio, 0);
  }

  public FlinkOperatorSnapshot(
      String id,
      String name,
      int parallelism,
      double recordsInPerSec,
      double recordsOutPerSec,
      double bytesInPerSec,
      double bytesOutPerSec,
      double busyRatio,
      double idleRatio,
      double backpressureRatio) {
    this(id, name, parallelism, recordsInPerSec, recordsOutPerSec, 0, 0,
        bytesInPerSec, bytesOutPerSec, busyRatio, idleRatio, backpressureRatio, 0);
  }
}
