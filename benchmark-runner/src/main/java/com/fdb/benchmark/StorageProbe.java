package com.fdb.benchmark;

public interface StorageProbe {
  StorageSnapshot snapshot() throws Exception;

  static StorageProbe forSink(BenchmarkSink sink, CommandRunner commandRunner) {
    return switch (sink) {
      case NONE -> new NoopStorageProbe();
      case KAFKA -> new KafkaStorageProbe(commandRunner);
      case STARROCKS -> new StarRocksStorageProbe(commandRunner);
      case HIVE -> new HiveStorageProbe(commandRunner);
      case ICEBERG -> new IcebergStorageProbe(commandRunner);
    };
  }
}
