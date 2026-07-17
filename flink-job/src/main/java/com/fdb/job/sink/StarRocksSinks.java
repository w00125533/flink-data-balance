package com.fdb.job.sink;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.CellKpi;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.row.sink.StarRocksSinkOP;
import com.starrocks.connector.flink.row.sink.StarRocksSinkRowBuilder;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class StarRocksSinks {

    private static final String DEFAULT_CONNECTOR_JDBC_URL = "jdbc:mysql://starrocks-fe:9030";
    private static final String DEFAULT_LOAD_URL = "starrocks-fe:8030";
    private static final String DEFAULT_USER = "root";
    private static final String DEFAULT_PASSWORD = "";
    private static final String DEFAULT_DATABASE = "fdb";
    private static final String DEFAULT_SEMANTIC = "exactly-once";
    private static final String DEFAULT_LABEL_PREFIX = "fdb-starrocks";
    private static final String DEFAULT_SINK_VERSION = "AUTO";
    private static final long MEGABYTE = 1024L * 1024L;
    private static final long GIGABYTE = 1024L * MEGABYTE;

    public static final int UPSERT_OP = StarRocksSinkOP.UPSERT.ordinal();

    private StarRocksSinks() {}

    public record StarRocksConnectorConfig(
        String jdbcUrl,
        String loadUrl,
        String user,
        String password,
        String database,
        String semantic,
        String labelPrefix,
        String bufferFlushMaxBytes,
        String bufferFlushMaxRows,
        String bufferFlushIntervalMs) {}

    public static SinkFunction<CellKpi> cellKpiSink(String labelSuffix) {
        return StarRocksSink.sink(
            cellKpiSchema(),
            sinkOptions(resolveConnectorConfig(System.getenv(), System.getProperties()), "cell_kpi", labelSuffix),
            cellKpiRowBuilder());
    }

    public static SinkFunction<AnomalyEvent> cellAnomalySink() {
        return anomalySink("cell_anomaly_events", "cell-anomaly", cellAnomalyRowBuilder());
    }

    public static SinkFunction<AnomalyEvent> userAnomalySink() {
        return anomalySink("user_anomaly_events", "user-anomaly", userAnomalyRowBuilder());
    }

    public static SinkFunction<AnomalyEvent> gridAnomalySink() {
        return anomalySink("grid_anomaly_events", "grid-anomaly", gridAnomalyRowBuilder());
    }

    private static SinkFunction<AnomalyEvent> anomalySink(
        String tableName,
        String labelSuffix,
        StarRocksSinkRowBuilder<AnomalyEvent> rowBuilder) {
        return StarRocksSink.sink(
            anomalySchema(),
            sinkOptions(resolveConnectorConfig(System.getenv(), System.getProperties()), tableName, labelSuffix),
            rowBuilder);
    }

    static StarRocksConnectorConfig resolveConnectorConfig(Map<String, String> env, Properties properties) {
        String runId = resolve(env, properties, "FDB_RUN_ID", "fdb.run.id", "");
        String labelPrefix = resolve(
            env,
            properties,
            "FDB_STARROCKS_SINK_LABEL_PREFIX",
            "fdb.starrocks.sink.label-prefix",
            runId.isBlank() ? DEFAULT_LABEL_PREFIX : "fdb-" + runId);
        String semantic = resolve(
            env,
            properties,
            "FDB_STARROCKS_SINK_SEMANTIC",
            "fdb.starrocks.sink.semantic",
            DEFAULT_SEMANTIC);
        validateSemantic(semantic);
        String bufferFlushMaxBytes = resolveOptional(env, properties, "FDB_STARROCKS_SINK_BUFFER_FLUSH_MAX_BYTES",
            "fdb.starrocks.sink.buffer-flush.max-bytes");
        String bufferFlushMaxRows = resolveOptional(env, properties, "FDB_STARROCKS_SINK_BUFFER_FLUSH_MAX_ROWS",
            "fdb.starrocks.sink.buffer-flush.max-rows");
        String bufferFlushIntervalMs = resolveOptional(env, properties, "FDB_STARROCKS_SINK_BUFFER_FLUSH_INTERVAL_MS",
            "fdb.starrocks.sink.buffer-flush.interval-ms");
        validateOptionalLong(
            "FDB_STARROCKS_SINK_BUFFER_FLUSH_MAX_BYTES",
            bufferFlushMaxBytes,
            64L * MEGABYTE,
            10L * GIGABYTE);
        validateOptionalLong(
            "FDB_STARROCKS_SINK_BUFFER_FLUSH_MAX_ROWS",
            bufferFlushMaxRows,
            64_000L,
            5_000_000L);
        validateOptionalLong(
            "FDB_STARROCKS_SINK_BUFFER_FLUSH_INTERVAL_MS",
            bufferFlushIntervalMs,
            1L,
            3_600_000L);

        return new StarRocksConnectorConfig(
            resolve(env, properties, "FDB_STARROCKS_CONNECTOR_JDBC_URL",
                "fdb.starrocks.connector.jdbc.url", DEFAULT_CONNECTOR_JDBC_URL),
            resolve(env, properties, "FDB_STARROCKS_LOAD_URL", "fdb.starrocks.load.url", DEFAULT_LOAD_URL),
            resolve(env, properties, "FDB_STARROCKS_USER", "fdb.starrocks.user", DEFAULT_USER),
            resolve(env, properties, "FDB_STARROCKS_PASSWORD", "fdb.starrocks.password", DEFAULT_PASSWORD),
            resolve(env, properties, "FDB_STARROCKS_DATABASE", "fdb.starrocks.database", DEFAULT_DATABASE),
            semantic,
            labelPrefix,
            bufferFlushMaxBytes,
            bufferFlushMaxRows,
            bufferFlushIntervalMs);
    }

    static Map<String, String> connectorProperties(
        StarRocksConnectorConfig config,
        String tableName,
        String labelSuffix) {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("jdbc-url", config.jdbcUrl());
        properties.put("load-url", config.loadUrl());
        properties.put("database-name", config.database());
        properties.put("table-name", tableName);
        properties.put("username", config.user());
        properties.put("password", config.password());
        properties.put("sink.version", DEFAULT_SINK_VERSION);
        properties.put("sink.semantic", config.semantic());
        properties.put("sink.label-prefix", config.labelPrefix() + "-" + labelSuffix);
        properties.put("sink.sanitize-error-log", "true");
        putIfConfigured(properties, "sink.buffer-flush.max-bytes", config.bufferFlushMaxBytes());
        putIfConfigured(properties, "sink.buffer-flush.max-rows", config.bufferFlushMaxRows());
        putIfConfigured(properties, "sink.buffer-flush.interval-ms", config.bufferFlushIntervalMs());
        return properties;
    }

    private static StarRocksSinkOptions sinkOptions(
        StarRocksConnectorConfig config,
        String tableName,
        String labelSuffix) {
        StarRocksSinkOptions.Builder builder = StarRocksSinkOptions.builder();
        connectorProperties(config, tableName, labelSuffix)
            .forEach(builder::withProperty);
        return builder.build();
    }

    static TableSchema cellKpiSchema() {
        return TableSchema.builder()
            .field("window_start_ts", DataTypes.BIGINT().notNull())
            .field("window_kind", DataTypes.STRING().notNull())
            .field("cell_id", DataTypes.STRING().notNull())
            .field("window_end_ts", DataTypes.BIGINT().notNull())
            .field("join_quality", DataTypes.STRING().notNull())
            .field("site_id", DataTypes.STRING().notNull())
            .field("grid_id", DataTypes.STRING().notNull())
            .field("num_chr_events", DataTypes.BIGINT().notNull())
            .field("num_users", DataTypes.BIGINT().notNull())
            .field("rsrp_sample_count", DataTypes.BIGINT().notNull())
            .field("sinr_sample_count", DataTypes.BIGINT().notNull())
            .field("attach_attempts", DataTypes.BIGINT().notNull())
            .field("avg_rsrp", DataTypes.FLOAT().notNull())
            .field("avg_sinr", DataTypes.FLOAT().notNull())
            .field("avg_prb_usage_dl", DataTypes.FLOAT().notNull())
            .field("throughput_dl_mbps_avg", DataTypes.FLOAT().notNull())
            .field("drop_rate", DataTypes.FLOAT().notNull())
            .field("ho_success_rate", DataTypes.FLOAT().notNull())
            .field("attach_success_rate", DataTypes.FLOAT().notNull())
            .primaryKey("window_start_ts", "window_kind", "cell_id")
            .build();
    }

    static TableSchema anomalySchema() {
        return TableSchema.builder()
            .field("anomaly_id", DataTypes.STRING().notNull())
            .field("detection_ts", DataTypes.BIGINT().notNull())
            .field("event_ts", DataTypes.BIGINT().notNull())
            .field("entity_type", DataTypes.STRING().notNull())
            .field("entity_id", DataTypes.STRING().notNull())
            .field("window_start_ts", DataTypes.BIGINT().notNull())
            .field("window_end_ts", DataTypes.BIGINT().notNull())
            .field("imsi", DataTypes.STRING())
            .field("site_id", DataTypes.STRING())
            .field("cell_id", DataTypes.STRING())
            .field("grid_id", DataTypes.STRING())
            .field("latitude", DataTypes.DOUBLE())
            .field("longitude", DataTypes.DOUBLE())
            .field("anomaly_type", DataTypes.STRING().notNull())
            .field("severity", DataTypes.STRING().notNull())
            .field("rule_version", DataTypes.STRING().notNull())
            .field("context_json", DataTypes.STRING())
            .primaryKey("anomaly_id")
            .build();
    }

    static StarRocksSinkRowBuilder<CellKpi> cellKpiRowBuilder() {
        return (row, kpi) -> {
            row[0] = kpi.getWindowStartTs();
            row[1] = text(kpi.getWindowKind());
            row[2] = text(kpi.getCellId());
            row[3] = kpi.getWindowEndTs();
            row[4] = text(kpi.getJoinQuality());
            row[5] = text(kpi.getSiteId());
            row[6] = text(kpi.getGridId());
            row[7] = kpi.getNumChrEvents();
            row[8] = kpi.getNumUsers();
            row[9] = kpi.getRsrpSampleCount();
            row[10] = kpi.getSinrSampleCount();
            row[11] = kpi.getAttachAttempts();
            row[12] = kpi.getAvgRsrp();
            row[13] = kpi.getAvgSinr();
            row[14] = kpi.getAvgPrbUsageDl();
            row[15] = kpi.getThroughputDlMbpsAvg();
            row[16] = kpi.getDropRate();
            row[17] = kpi.getHoSuccessRate();
            row[18] = kpi.getAttachSuccessRate();
            row[19] = UPSERT_OP;
        };
    }

    static StarRocksSinkRowBuilder<AnomalyEvent> cellAnomalyRowBuilder() {
        return (row, event) -> bindRow(row, cellAnomalyValues(event));
    }

    static StarRocksSinkRowBuilder<AnomalyEvent> userAnomalyRowBuilder() {
        return (row, event) -> bindRow(row, userAnomalyValues(event));
    }

    static StarRocksSinkRowBuilder<AnomalyEvent> gridAnomalyRowBuilder() {
        return (row, event) -> bindRow(row, gridAnomalyValues(event));
    }

    static String cellAnomalyId(AnomalyEvent event) {
        return anomalyId("cell", event);
    }

    static String userAnomalyId(AnomalyEvent event) {
        return anomalyId("user", event);
    }

    static String gridAnomalyId(AnomalyEvent event) {
        return anomalyId("grid", event);
    }

    static List<Object> cellAnomalyValues(AnomalyEvent event) {
        return anomalyValues("cell", event);
    }

    static List<Object> userAnomalyValues(AnomalyEvent event) {
        return anomalyValues("user", event);
    }

    static List<Object> gridAnomalyValues(AnomalyEvent event) {
        return anomalyValues("grid", event);
    }

    private static void bindRow(Object[] row, List<Object> values) {
        for (int i = 0; i < values.size(); i++) {
            row[i] = values.get(i);
        }
        row[values.size()] = UPSERT_OP;
    }

    private static List<Object> anomalyValues(String scope, AnomalyEvent event) {
        return Arrays.asList(
            anomalyId(scope, event),
            event.getDetectionTs(),
            event.getEventTs(),
            text(event.getEntityType()),
            text(event.getEntityId()),
            event.getWindowStartTs(),
            event.getWindowEndTs(),
            text(event.getImsi()),
            text(event.getSiteId()),
            text(event.getCellId()),
            text(event.getGridId()),
            event.getLatitude(),
            event.getLongitude(),
            text(event.getAnomalyType()),
            text(event.getSeverity()),
            text(event.getRuleVersion()),
            text(event.getContextJson())
        );
    }

    private static String resolve(
        Map<String, String> env,
        Properties properties,
        String envName,
        String propertyName,
        String defaultValue) {
        String configured = env.get(envName);
        if (configured == null || configured.isBlank()) {
            configured = properties.getProperty(propertyName);
        }
        if (configured == null || configured.isBlank()) {
            return defaultValue;
        }
        return configured.trim();
    }

    private static String resolveOptional(
        Map<String, String> env,
        Properties properties,
        String envName,
        String propertyName) {
        return resolve(env, properties, envName, propertyName, "");
    }

    private static void putIfConfigured(Map<String, String> properties, String key, String value) {
        if (value != null && !value.isBlank()) {
            properties.put(key, value);
        }
    }

    private static void validateOptionalLong(String envName, String value, long minInclusive, long maxInclusive) {
        if (value == null || value.isBlank()) {
            return;
        }
        long parsed;
        try {
            parsed = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(envName + " must be a long value: " + value, e);
        }
        if (parsed < minInclusive || parsed > maxInclusive) {
            throw new IllegalArgumentException(envName + " must be in range [" + minInclusive + ", "
                + maxInclusive + "]: " + value);
        }
    }

    private static void validateSemantic(String semantic) {
        if (!"at-least-once".equals(semantic) && !"exactly-once".equals(semantic)) {
            throw new IllegalArgumentException("FDB_STARROCKS_SINK_SEMANTIC must be at-least-once or exactly-once: "
                + semantic);
        }
    }

    private static String anomalyId(String scope, AnomalyEvent event) {
        return anomalyId(
            scope,
            event.getEventTs(),
            text(event.getEntityType()),
            text(event.getEntityId()),
            text(event.getAnomalyType()),
            text(event.getRuleVersion()),
            sha256(text(event.getContextJson()))
        );
    }

    private static String anomalyId(String prefix, Object... parts) {
        StringBuilder stableKey = new StringBuilder(prefix);
        for (Object part : parts) {
            stableKey.append('\u001F').append(part);
        }
        return prefix + ":" + sha256(stableKey.toString());
    }

    private static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest is unavailable", e);
        }
    }

    private static String text(Object value) {
        return value == null ? "" : value.toString();
    }
}
