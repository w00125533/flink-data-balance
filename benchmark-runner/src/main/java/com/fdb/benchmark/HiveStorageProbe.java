package com.fdb.benchmark;

import java.util.List;

public final class HiveStorageProbe implements StorageProbe {
  private final CommandRunner commandRunner;

  public HiveStorageProbe(CommandRunner commandRunner) {
    this.commandRunner = commandRunner;
  }

  @Override
  public StorageSnapshot snapshot() throws Exception {
    CommandResult result = commandRunner.run(List.of("bash", "-lc",
        "set -o pipefail; probe_timeout=${FDB_HDFS_PROBE_TIMEOUT_SEC:-10}; "
            + "run_with_timeout() { if command -v timeout >/dev/null 2>&1; then "
            + "timeout \"$probe_timeout\" \"$@\"; else \"$@\"; fi; }; "
            + "if command -v hdfs >/dev/null 2>&1; then "
            + "run_with_timeout hdfs dfs -fs ${FDB_HDFS_URI:-hdfs://namenode:8020} "
            + "-find ${FDB_HIVE_WAREHOUSE_PATH:-/warehouse/fdb} -name '*.inprogress*'; "
            + "else export MSYS_NO_PATHCONV=1; "
            + "run_with_timeout docker exec ${FDB_SHARED_HDFS_CONTAINER:-shared-data-infra-namenode-1} "
            + "${FDB_LOCAL_HDFS_BIN:-/opt/hadoop-3.2.1/bin/hdfs} dfs "
            + "-fs ${FDB_HDFS_URI:-hdfs://namenode:8020} "
            + "-find ${FDB_HIVE_WAREHOUSE_PATH:-/warehouse/fdb} -name '*.inprogress*'; fi | wc -l"));
    if (!result.success()) {
      return KafkaStorageProbe.snapshotFromCommand("hive files", result);
    }
    long inProgress = parseLong(result.stdout());
    return new StorageSnapshot(true, "hive in-progress files=" + inProgress, 0, 0, inProgress);
  }

  private static long parseLong(String value) {
    try {
      return Long.parseLong(value.trim());
    } catch (RuntimeException e) {
      return 0;
    }
  }
}
