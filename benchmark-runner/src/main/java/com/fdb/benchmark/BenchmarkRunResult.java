package com.fdb.benchmark;

public record BenchmarkRunResult(
    BenchmarkRunPlan plan,
    BenchmarkStatus status,
    String bottleneckReason,
    FlinkSnapshot flink,
    FdbMetricsSnapshot fdb,
    StorageSnapshot storage,
    TopologyMetricsSnapshot topology,
    SourceMetricsSnapshot source) {

  public BenchmarkRunResult(
      BenchmarkRunPlan plan,
      BenchmarkStatus status,
      String bottleneckReason,
      FlinkSnapshot flink,
      FdbMetricsSnapshot fdb,
      StorageSnapshot storage) {
    this(plan, status, bottleneckReason, flink, fdb, storage, TopologyMetricsSnapshot.empty(), SourceMetricsSnapshot.empty());
  }

  public BenchmarkRunResult(
      BenchmarkRunPlan plan,
      BenchmarkStatus status,
      String bottleneckReason,
      FlinkSnapshot flink,
      FdbMetricsSnapshot fdb,
      StorageSnapshot storage,
      TopologyMetricsSnapshot topology) {
    this(plan, status, bottleneckReason, flink, fdb, storage, topology, SourceMetricsSnapshot.empty());
  }

  public BenchmarkRunResult withStorage(StorageSnapshot value) {
    return new BenchmarkRunResult(plan, status, bottleneckReason, flink, fdb, value, topology, source);
  }
}
