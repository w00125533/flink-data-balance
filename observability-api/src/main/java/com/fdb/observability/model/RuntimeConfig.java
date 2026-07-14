package com.fdb.observability.model;

public record RuntimeConfig(
    boolean dynamicBalancingEnabled,
    String resultSink,
    boolean dlqEnabled,
    boolean metricsEnabled,
    boolean metricsHistoryEnabled,
    String runId,
    String runLabel,
    int parallelism,
    long checkpointIntervalMs,
    String jobStatus,
    String reportStatus
) {
}
