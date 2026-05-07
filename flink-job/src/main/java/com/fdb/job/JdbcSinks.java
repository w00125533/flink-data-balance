package com.fdb.job;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.CellKpi;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.sink.JdbcSink;

public final class JdbcSinks {

    private JdbcSinks() {}

    private static final String DEFAULT_URL = "jdbc:mysql://localhost:3306/fdb";
    private static final String DEFAULT_USER = "root";
    private static final String DEFAULT_PASSWORD = "fdb123";

    private static String jdbcUrl() {
        return System.getenv().getOrDefault("FDB_JDBC_URL", DEFAULT_URL);
    }

    private static String jdbcUser() {
        return System.getenv().getOrDefault("FDB_JDBC_USER", DEFAULT_USER);
    }

    private static String jdbcPassword() {
        return System.getenv().getOrDefault("FDB_JDBC_PASSWORD", DEFAULT_PASSWORD);
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
            .withBatchSize(500)
            .withBatchIntervalMs(5000)
            .build();
    }

    public static JdbcSink<AnomalyEvent> anomalySink() {
        return JdbcSink.<AnomalyEvent>builder()
            .withQueryStatement(
                "INSERT INTO anomaly_events (detection_ts, event_ts, imsi, site_id, cell_id, grid_id, " +
                "latitude, longitude, anomaly_type, severity, rule_version, context_json) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE detection_ts=VALUES(detection_ts), severity=VALUES(severity), " +
                "context_json=VALUES(context_json)",
                (ps, ae) -> {
                    ps.setLong(1, ae.getDetectionTs());
                    ps.setLong(2, ae.getEventTs());
                    ps.setString(3, ae.getImsi());
                    ps.setString(4, ae.getSiteId());
                    ps.setString(5, ae.getCellId());
                    ps.setString(6, ae.getGridId());
                    ps.setDouble(7, ae.getLatitude());
                    ps.setDouble(8, ae.getLongitude());
                    ps.setString(9, ae.getAnomalyType().toString());
                    ps.setString(10, ae.getSeverity().toString());
                    ps.setString(11, ae.getRuleVersion());
                    ps.setString(12, ae.getContextJson());
                }
            )
            .withExecutionOptions(execOpts())
            .buildAtLeastOnce(connOpts());
    }

    public static JdbcSink<CellKpi> cellKpiSink() {
        return JdbcSink.<CellKpi>builder()
            .withQueryStatement(
                "INSERT INTO cell_kpi_1m (window_start_ts, window_end_ts, window_kind, site_id, cell_id, " +
                "grid_id, num_chr_events, num_users, avg_rsrp, avg_sinr, avg_prb_usage_dl, " +
                "throughput_dl_mbps_avg, drop_rate, ho_success_rate, attach_success_rate) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE num_chr_events=VALUES(num_chr_events), avg_rsrp=VALUES(avg_rsrp), " +
                "avg_sinr=VALUES(avg_sinr), avg_prb_usage_dl=VALUES(avg_prb_usage_dl), " +
                "drop_rate=VALUES(drop_rate), attach_success_rate=VALUES(attach_success_rate)",
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
