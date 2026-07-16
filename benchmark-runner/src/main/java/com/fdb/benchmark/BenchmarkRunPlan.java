package com.fdb.benchmark;

public record BenchmarkRunPlan(
    String benchmarkId,
    BenchmarkSink sink,
    int cellLevel,
    long targetChrEps,
    String runId,
    String runLabel) {
}
