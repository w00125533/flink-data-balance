package com.fdb.benchmark;

import java.util.List;

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
    long pendingRecords,
    long currentInputWatermarkMs,
    long currentOutputWatermarkMs,
    long flinkMarkerP95Ms,
    List<FlinkMarkerLatencySnapshot> flinkMarkerLatencies) {

  private static final long UNKNOWN_WATERMARK_MS = -1L;

  public FlinkOperatorSnapshot {
    flinkMarkerLatencies = flinkMarkerLatencies == null ? List.of() : List.copyOf(flinkMarkerLatencies);
  }

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
      double backpressureRatio,
      long pendingRecords) {
    this(id, name, parallelism, recordsInPerSec, recordsOutPerSec, recordsInTotal, recordsOutTotal,
        bytesInPerSec, bytesOutPerSec, busyRatio, idleRatio, backpressureRatio, pendingRecords,
        UNKNOWN_WATERMARK_MS, UNKNOWN_WATERMARK_MS, -1L, List.of());
  }

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
        bytesInPerSec, bytesOutPerSec, busyRatio, idleRatio, backpressureRatio, 0,
        UNKNOWN_WATERMARK_MS, UNKNOWN_WATERMARK_MS, -1L, List.of());
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
        bytesInPerSec, bytesOutPerSec, busyRatio, idleRatio, backpressureRatio, 0,
        UNKNOWN_WATERMARK_MS, UNKNOWN_WATERMARK_MS, -1L, List.of());
  }

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
      double backpressureRatio,
      long pendingRecords,
      long currentInputWatermarkMs,
      long currentOutputWatermarkMs) {
    this(id, name, parallelism, recordsInPerSec, recordsOutPerSec, recordsInTotal, recordsOutTotal,
        bytesInPerSec, bytesOutPerSec, busyRatio, idleRatio, backpressureRatio, pendingRecords,
        currentInputWatermarkMs, currentOutputWatermarkMs, -1L, List.of());
  }

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
      double backpressureRatio,
      long pendingRecords,
      long currentInputWatermarkMs,
      long currentOutputWatermarkMs,
      long flinkMarkerP95Ms) {
    this(id, name, parallelism, recordsInPerSec, recordsOutPerSec, recordsInTotal, recordsOutTotal,
        bytesInPerSec, bytesOutPerSec, busyRatio, idleRatio, backpressureRatio, pendingRecords,
        currentInputWatermarkMs, currentOutputWatermarkMs, flinkMarkerP95Ms, List.of());
  }
}
