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
        "if command -v mysql >/dev/null 2>&1; then "
            + "mysql -h ${FDB_STARROCKS_HOST:-127.0.0.1} -P ${FDB_STARROCKS_QUERY_PORT:-9030} "
            + "-u ${FDB_STARROCKS_USER:-root} ${FDB_STARROCKS_PASSWORD:+-p$FDB_STARROCKS_PASSWORD} "
            + "--database=${FDB_STARROCKS_DATABASE:-fdb} -e 'SELECT 1'; "
            + "elif timeout ${FDB_STARROCKS_TCP_PROBE_TIMEOUT_SEC:-3} "
            + "bash -c '</dev/tcp/${FDB_STARROCKS_HOST:-127.0.0.1}/${FDB_STARROCKS_QUERY_PORT:-9030}' "
            + ">/dev/null 2>&1; then echo 'starrocks tcp open'; "
            + "else docker exec ${FDB_SHARED_STARROCKS_FE_CONTAINER:-shared-data-infra-starrocks-fe-1} "
            + "mysql -h 127.0.0.1 -P 9030 -u ${FDB_STARROCKS_USER:-root} "
            + "${FDB_STARROCKS_PASSWORD:+-p$FDB_STARROCKS_PASSWORD} "
            + "--database=${FDB_STARROCKS_DATABASE:-fdb} -e 'SELECT 1'; fi"));
    return KafkaStorageProbe.snapshotFromCommand("starrocks query", result);
  }
}
