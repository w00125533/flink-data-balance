package com.fdb.benchmark;

public record BenchmarkRunResult(
    BenchmarkRunPlan plan,
    BenchmarkStatus status,
    String bottleneckReason,
    FlinkSnapshot flink,
    FdbMetricsSnapshot fdb,
    StorageSnapshot storage) {
}
