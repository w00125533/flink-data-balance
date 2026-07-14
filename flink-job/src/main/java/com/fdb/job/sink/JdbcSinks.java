package com.fdb.job.sink;

import com.fdb.common.avro.CellKpi;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.sink.JdbcSink;

public final class JdbcSinks {

    private JdbcSinks() {}

    private static final String DEFAULT_URL = "jdbc:mysql://starrocks-fe:9030/fdb";
    private static final String DEFAULT_USER = "root";
    private static final String DEFAULT_PASSWORD = "";
    private static final int DEFAULT_BATCH_SIZE = 100_000;
    private static final long DEFAULT_BATCH_INTERVAL_MS = 60_000L;
    private static final int DEFAULT_MAX_RETRIES = 1;

    private static String jdbcUrl() {
        return appendJdbcBatchParameters(System.getenv().getOrDefault("FDB_STARROCKS_JDBC_URL",
            System.getenv().getOrDefault("FDB_JDBC_URL", DEFAULT_URL)));
    }

    private static String appendJdbcBatchParameters(String url) {
        if (!url.startsWith("jdbc:mysql:")) {
            return url;
        }
        String result = url;
        if (!result.contains("rewriteBatchedStatements=")) {
            result = appendJdbcParameter(result, "rewriteBatchedStatements=true");
        }
        if (!result.contains("useServerPrepStmts=")) {
            result = appendJdbcParameter(result, "useServerPrepStmts=false");
        }
        return result;
    }

    private static String appendJdbcParameter(String url, String parameter) {
        return url + (url.contains("?") ? "&" : "?") + parameter;
    }

    private static String jdbcUser() {
        return System.getenv().getOrDefault("FDB_STARROCKS_USER",
            System.getenv().getOrDefault("FDB_JDBC_USER", DEFAULT_USER));
    }

    private static String jdbcPassword() {
        return System.getenv().getOrDefault("FDB_STARROCKS_PASSWORD",
            System.getenv().getOrDefault("FDB_JDBC_PASSWORD", DEFAULT_PASSWORD));
    }

    private static JdbcConnectionOptions connOpts() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl(jdbcUrl())
            .withUsername(jdbcUser())
            .withPassword(jdbcPassword())
            .withDriverName("com.mysql.cj.jdbc.Driver")
            .build();
    }

    private static JdbcExecutionOptions execOpts() {
        return JdbcExecutionOptions.builder()
            .withBatchSize(envInt("FDB_STARROCKS_JDBC_BATCH_SIZE", "FDB_JDBC_BATCH_SIZE", DEFAULT_BATCH_SIZE))
            .withBatchIntervalMs(envLong("FDB_STARROCKS_JDBC_BATCH_INTERVAL_MS", "FDB_JDBC_BATCH_INTERVAL_MS",
                DEFAULT_BATCH_INTERVAL_MS))
            .withMaxRetries(envInt("FDB_STARROCKS_JDBC_MAX_RETRIES", "FDB_JDBC_MAX_RETRIES", DEFAULT_MAX_RETRIES))
            .build();
    }

    private static int envInt(String primary, String fallback, int defaultValue) {
        String value = System.getenv().getOrDefault(primary, System.getenv().get(fallback));
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }

    private static long envLong(String primary, String fallback, long defaultValue) {
        String value = System.getenv().getOrDefault(primary, System.getenv().get(fallback));
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Long.parseLong(value);
    }

    public static JdbcSink<CellKpi> cellKpiSink() {
        return JdbcSink.<CellKpi>builder()
            .withQueryStatement(
                "INSERT INTO cell_kpi (window_start_ts, window_end_ts, window_kind, site_id, cell_id, " +
                "grid_id, num_chr_events, num_users, avg_rsrp, avg_sinr, avg_prb_usage_dl, " +
                "throughput_dl_mbps_avg, drop_rate, ho_success_rate, attach_success_rate) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ps, kpi) -> {
                    ps.setLong(1, kpi.getWindowStartTs());
                    ps.setLong(2, kpi.getWindowEndTs());
                    ps.setString(3, kpi.getWindowKind().toString());
                    ps.setString(4, kpi.getSiteId());
                    ps.setString(5, kpi.getCellId());
                    ps.setString(6, kpi.getGridId());
                    ps.setLong(7, kpi.getNumChrEvents());
                    ps.setLong(8, kpi.getNumUsers());
                    ps.setFloat(9, kpi.getAvgRsrp());
                    ps.setFloat(10, kpi.getAvgSinr());
                    ps.setFloat(11, kpi.getAvgPrbUsageDl());
                    ps.setFloat(12, kpi.getThroughputDlMbpsAvg());
                    ps.setFloat(13, kpi.getDropRate());
                    ps.setFloat(14, kpi.getHoSuccessRate());
                    ps.setFloat(15, kpi.getAttachSuccessRate());
                }
            )
            .withExecutionOptions(execOpts())
            .buildAtLeastOnce(connOpts());
    }
}
