package com.fdb.benchmark;

public record RunObservation(
    FlinkSnapshot flink,
    FdbMetricsSnapshot fdb,
    StorageSnapshot storage,
    SourceMetricsSnapshot source) {

  public RunObservation(FlinkSnapshot flink, FdbMetricsSnapshot fdb, StorageSnapshot storage) {
    this(flink, fdb, storage, SourceMetricsSnapshot.empty());
  }

  public RunObservation withFlink(FlinkSnapshot value) {
    return new RunObservation(value, fdb, storage, source);
  }

  public RunObservation withFdb(FdbMetricsSnapshot value) {
    return new RunObservation(flink, value, storage, source);
  }

  public RunObservation withStorage(StorageSnapshot value) {
    return new RunObservation(flink, fdb, value, source);
  }

  public RunObservation withSource(SourceMetricsSnapshot value) {
    return new RunObservation(flink, fdb, storage, value);
  }
}
