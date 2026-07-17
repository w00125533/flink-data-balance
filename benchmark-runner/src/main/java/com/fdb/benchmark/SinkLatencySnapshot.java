package com.fdb.benchmark;

public record SinkLatencySnapshot(
    String sinkName,
    long records,
    long bytes,
    long latencyP50Ms,
    long latencyP95Ms,
    long latencyP99Ms,
    long failures) {
}
