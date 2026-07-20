package com.fdb.observability.model;

public record StageStatus(
    String stageId,
    String label,
    String status,
    double inEps,
    double outEps,
    long latencyP50Ms,
    long latencyP95Ms,
    long latencyP99Ms,
    long watermarkLagMs,
    long dlqCount,
    String summary,
    String updatedAt) {
}
