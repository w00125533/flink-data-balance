package com.fdb.benchmark;

import java.util.List;

public final class StarRocksStorageProbe implements StorageProbe {
  private static final String ROW_COUNT_SQL = """
      SELECT COALESCE(SUM(cnt), 0) FROM (
        SELECT COUNT(*) AS cnt FROM cell_kpi
        UNION ALL SELECT COUNT(*) FROM cell_anomaly_events
        UNION ALL SELECT COUNT(*) FROM user_anomaly_events
        UNION ALL SELECT COUNT(*) FROM grid_anomaly_events
      ) t;
      """.replace("\n", " ");

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
            + "--database=${FDB_STARROCKS_DATABASE:-fdb} -N -B -e '" + ROW_COUNT_SQL + "'; "
            + "elif timeout ${FDB_STARROCKS_TCP_PROBE_TIMEOUT_SEC:-3} "
            + "bash -c '</dev/tcp/${FDB_STARROCKS_HOST:-127.0.0.1}/${FDB_STARROCKS_QUERY_PORT:-9030}' "
            + ">/dev/null 2>&1; then docker exec ${FDB_SHARED_STARROCKS_FE_CONTAINER:-shared-data-infra-starrocks-fe-1} "
            + "mysql -h 127.0.0.1 -P 9030 -u ${FDB_STARROCKS_USER:-root} "
            + "${FDB_STARROCKS_PASSWORD:+-p$FDB_STARROCKS_PASSWORD} "
            + "--database=${FDB_STARROCKS_DATABASE:-fdb} -N -B -e '" + ROW_COUNT_SQL + "'; "
            + "else docker exec ${FDB_SHARED_STARROCKS_FE_CONTAINER:-shared-data-infra-starrocks-fe-1} "
            + "mysql -h 127.0.0.1 -P 9030 -u ${FDB_STARROCKS_USER:-root} "
            + "${FDB_STARROCKS_PASSWORD:+-p$FDB_STARROCKS_PASSWORD} "
            + "--database=${FDB_STARROCKS_DATABASE:-fdb} -N -B -e '" + ROW_COUNT_SQL + "'; fi"));
    if (!result.success()) {
      return KafkaStorageProbe.snapshotFromCommand("starrocks query", result);
    }
    long rows = KafkaStorageProbe.parseFirstLong(result.stdout());
    return new StorageSnapshot(true, "starrocks rows=" + rows, rows, 0, 0);
  }
}
