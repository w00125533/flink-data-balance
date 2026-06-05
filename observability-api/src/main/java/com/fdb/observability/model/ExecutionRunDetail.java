package com.fdb.observability.model;

import java.util.List;

public record ExecutionRunDetail(
    String runId,
    String status,
    String startedAt,
    String completedAt,
    List<ExecutionMetric> metrics,
    List<String> rawLines) {
}
