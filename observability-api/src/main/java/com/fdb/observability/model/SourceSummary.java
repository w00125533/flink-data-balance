package com.fdb.observability.model;

public record SourceSummary(
    String source,
    String status,
    double eps,
    long kafkaLag,
    long eventDelayMs,
    String summary,
    String updatedAt) {
}
