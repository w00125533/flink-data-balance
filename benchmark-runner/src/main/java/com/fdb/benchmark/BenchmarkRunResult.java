package com.fdb.benchmark;

public record BenchmarkRunResult(
    BenchmarkRunPlan plan,
    BenchmarkStatus status,
    String bottleneckReason,
    FlinkSnapshot flink,
    FdbMetricsSnapshot fdb,
    StorageSnapshot storage,
    SourceMetricsSnapshot source) {

  public BenchmarkRunResult(
      BenchmarkRunPlan plan,
      BenchmarkStatus status,
      String bottleneckReason,
      FlinkSnapshot flink,
      FdbMetricsSnapshot fdb,
      StorageSnapshot storage) {
    this(plan, status, bottleneckReason, flink, fdb, storage, SourceMetricsSnapshot.empty());
  }
}
