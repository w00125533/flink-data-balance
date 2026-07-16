package com.fdb.benchmark;

import java.util.List;

public final class IcebergStorageProbe implements StorageProbe {
  private final CommandRunner commandRunner;

  public IcebergStorageProbe(CommandRunner commandRunner) {
    this.commandRunner = commandRunner;
  }

  @Override
  public StorageSnapshot snapshot() throws Exception {
    CommandResult result = commandRunner.run(List.of("bash", "-lc",
        "hdfs dfs -find ${FDB_ICEBERG_WAREHOUSE_PATH:-/warehouse/iceberg} -name '*.inprogress*' | wc -l"));
    if (!result.success()) {
      return KafkaStorageProbe.snapshotFromCommand("iceberg files", result);
    }
    long inProgress = parseLong(result.stdout());
    return new StorageSnapshot(inProgress == 0, "iceberg in-progress files=" + inProgress, 0, 0, inProgress);
  }

  private static long parseLong(String value) {
    try {
      return Long.parseLong(value.trim());
    } catch (RuntimeException e) {
      return 0;
    }
  }
}
