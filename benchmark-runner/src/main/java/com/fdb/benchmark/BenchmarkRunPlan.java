package com.fdb.benchmark;

public record BenchmarkRunPlan(
    String benchmarkId,
    BenchmarkSink sink,
    int cellLevel,
    double targetChrEpsPerCell,
    long targetChrTotalEps,
    double targetPmEpsPerCell,
    long targetPmTotalEps,
    String runId,
    String runLabel) {
}
