package com.fdb.observability.model;

public record SinkLatencySummary(
    String sinkName,
    String sinkType,
    String dataset,
    String windowKind,
    long records,
    long bytes,
    long durationMs,
    long p50Ms,
    long p95Ms,
    long p99Ms,
    long failureCount,
    String lastError,
    long checkpointId,
    String updatedAt
) {
}
