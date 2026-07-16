package com.fdb.benchmark;

public record RunObservation(FlinkSnapshot flink, FdbMetricsSnapshot fdb, StorageSnapshot storage) {
  public RunObservation withFlink(FlinkSnapshot value) {
    return new RunObservation(value, fdb, storage);
  }

  public RunObservation withFdb(FdbMetricsSnapshot value) {
    return new RunObservation(flink, value, storage);
  }

  public RunObservation withStorage(StorageSnapshot value) {
    return new RunObservation(flink, fdb, value);
  }
}
