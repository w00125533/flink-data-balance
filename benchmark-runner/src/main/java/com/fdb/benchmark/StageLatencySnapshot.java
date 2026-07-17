package com.fdb.benchmark;

public record StageLatencySnapshot(
    String stageId,
    long latencyP50Ms,
    long latencyP95Ms,
    long latencyP99Ms,
    long watermarkLagMs) {
}
