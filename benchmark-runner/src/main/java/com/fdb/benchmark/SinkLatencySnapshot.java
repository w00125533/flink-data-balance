package com.fdb.benchmark;

public record SinkLatencySnapshot(
    String sinkName,
    String sinkType,
    String dataset,
    String windowKind,
    long records,
    long bytes,
    long latencyP50Ms,
    long latencyP95Ms,
    long latencyP99Ms,
    long failures) {
  public SinkLatencySnapshot(
      String sinkName,
      long records,
      long bytes,
      long latencyP50Ms,
      long latencyP95Ms,
      long latencyP99Ms,
      long failures) {
    this(sinkName, "", "", "", records, bytes, latencyP50Ms, latencyP95Ms, latencyP99Ms, failures);
  }
}
