package com.fdb.observability.model;

public record ExecutionRunSummary(
    String runId,
    String status,
    String startedAt,
    String completedAt,
    int metricCount,
    String summary) {
}
