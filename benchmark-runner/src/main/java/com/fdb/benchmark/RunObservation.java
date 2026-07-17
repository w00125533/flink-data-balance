package com.fdb.benchmark;

public record RunObservation(
    FlinkSnapshot flink,
    FdbMetricsSnapshot fdb,
    StorageSnapshot storage,
    TopologyMetricsSnapshot topology,
    SourceMetricsSnapshot source) {

  public RunObservation(FlinkSnapshot flink, FdbMetricsSnapshot fdb, StorageSnapshot storage) {
    this(flink, fdb, storage, TopologyMetricsSnapshot.empty(), SourceMetricsSnapshot.empty());
  }

  public RunObservation(
      FlinkSnapshot flink,
      FdbMetricsSnapshot fdb,
      StorageSnapshot storage,
      TopologyMetricsSnapshot topology) {
    this(flink, fdb, storage, topology, SourceMetricsSnapshot.empty());
  }

  public RunObservation withFlink(FlinkSnapshot value) {
    return new RunObservation(value, fdb, storage, topology, source);
  }

  public RunObservation withFdb(FdbMetricsSnapshot value) {
    return new RunObservation(flink, value, storage, topology, source);
  }

  public RunObservation withStorage(StorageSnapshot value) {
    return new RunObservation(flink, fdb, value, topology, source);
  }

  public RunObservation withSource(SourceMetricsSnapshot value) {
    return new RunObservation(flink, fdb, storage, topology, value);
  }
}
