package com.fdb.benchmark;

import java.util.List;

public final class StarRocksStorageProbe implements StorageProbe {
  private final CommandRunner commandRunner;

  public StarRocksStorageProbe(CommandRunner commandRunner) {
    this.commandRunner = commandRunner;
  }

  @Override
  public StorageSnapshot snapshot() throws Exception {
    CommandResult result = commandRunner.run(List.of("bash", "-lc",
        "mysql -h ${FDB_STARROCKS_HOST:-localhost} -P ${FDB_STARROCKS_QUERY_PORT:-9030} "
            + "-u ${FDB_STARROCKS_USER:-root} ${FDB_STARROCKS_PASSWORD:+-p$FDB_STARROCKS_PASSWORD} "
            + "-e 'SELECT 1'"));
    return KafkaStorageProbe.snapshotFromCommand("starrocks query", result);
  }
}
