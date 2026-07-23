package com.fdb.benchmark;

public record FlinkMarkerLatencySnapshot(
    String sourceOperatorId,
    String targetOperatorId,
    long p95Ms,
    String metricId) {
}
