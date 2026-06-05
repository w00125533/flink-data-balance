package com.fdb.observability.model;

public record SinkSummary(
    String sink,
    String window,
    String status,
    long rowsWritten,
    long writeLatencyP95Ms,
    String summary,
    String updatedAt) {
}
