package com.fdb.job;

import com.fdb.common.avro.AnomalyEvent;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public final class StarRocksSinks {

    private static final String DEFAULT_JDBC_URL = "jdbc:mysql://starrocks-fe:9030/fdb";
    private static final String DEFAULT_USER = "root";
    private static final String DEFAULT_PASSWORD = "";
    private static final String DEFAULT_DATABASE = "fdb";
    private static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private StarRocksSinks() {}

    public record StarRocksJdbcConfig(String jdbcUrl, String user, String password, String database) {}

    public static String cellAnomalyInsertSql() {
        return cellAnomalyInsertSql("cell_anomaly_events");
    }

    static String cellAnomalyInsertSql(StarRocksJdbcConfig config) {
        return cellAnomalyInsertSql(qualifiedTable(config.database(), "cell_anomaly_events"));
    }

    private static String cellAnomalyInsertSql(String table) {
        return "INSERT INTO " + table + " "
            + "(anomaly_id, detection_ts, cell_id, anomaly_type, event_ts, site_id, grid_id, latitude, longitude, severity, rule_version, context_json) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    public static String gridAnomalyInsertSql() {
        return gridAnomalyInsertSql("grid_anomaly_events");
    }

    static String gridAnomalyInsertSql(StarRocksJdbcConfig config) {
        return gridAnomalyInsertSql(qualifiedTable(config.database(), "grid_anomaly_events"));
    }

    private static String gridAnomalyInsertSql(String table) {
        return "INSERT INTO " + table + " "
            + "(anomaly_id, detection_ts, grid_id, anomaly_type, event_ts, latitude, longitude, severity, rule_version, context_json) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    public static String cellKpiInsertSql() {
        return "";
    }

    public static org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink<AnomalyEvent> cellAnomalySink() {
        StarRocksJdbcConfig config = resolveConfig(System.getenv(), System.getProperties());
        return JdbcSink.<AnomalyEvent>builder()
            .withQueryStatement(cellAnomalyInsertSql(config), (statement, event) ->
                bindValues(statement, cellAnomalyValues(event)))
            .withExecutionOptions(JdbcExecutionOptions.defaults())
            .buildAtLeastOnce(connectionOptions(config));
    }

    public static org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink<AnomalyEvent> gridAnomalySink() {
        StarRocksJdbcConfig config = resolveConfig(System.getenv(), System.getProperties());
        return JdbcSink.<AnomalyEvent>builder()
            .withQueryStatement(gridAnomalyInsertSql(config), (statement, event) ->
                bindValues(statement, gridAnomalyValues(event)))
            .withExecutionOptions(JdbcExecutionOptions.defaults())
            .buildAtLeastOnce(connectionOptions(config));
    }

    static StarRocksJdbcConfig resolveConfig(Map<String, String> env, Properties properties) {
        return new StarRocksJdbcConfig(
            resolve(env, properties, "FDB_STARROCKS_JDBC_URL", "fdb.starrocks.jdbc.url", DEFAULT_JDBC_URL),
            resolve(env, properties, "FDB_STARROCKS_USER", "fdb.starrocks.user", DEFAULT_USER),
            resolve(env, properties, "FDB_STARROCKS_PASSWORD", "fdb.starrocks.password", DEFAULT_PASSWORD),
            resolve(env, properties, "FDB_STARROCKS_DATABASE", "fdb.starrocks.database", DEFAULT_DATABASE)
        );
    }

    static String cellAnomalyId(AnomalyEvent event) {
        return anomalyId(
            "cell",
            event.getEventTs(),
            text(event.getCellId()),
            text(event.getAnomalyType()),
            text(event.getSeverity()),
            text(event.getRuleVersion()),
            sha256(text(event.getContextJson()))
        );
    }

    static String gridAnomalyId(AnomalyEvent event) {
        return anomalyId(
            "grid",
            event.getEventTs(),
            text(event.getGridId()),
            text(event.getAnomalyType()),
            text(event.getSeverity()),
            text(event.getRuleVersion()),
            sha256(text(event.getContextJson()))
        );
    }

    static List<Object> cellAnomalyValues(AnomalyEvent event) {
        return List.of(
            cellAnomalyId(event),
            event.getDetectionTs(),
            text(event.getCellId()),
            text(event.getAnomalyType()),
            event.getEventTs(),
            text(event.getSiteId()),
            text(event.getGridId()),
            event.getLatitude(),
            event.getLongitude(),
            text(event.getSeverity()),
            text(event.getRuleVersion()),
            text(event.getContextJson())
        );
    }

    static List<Object> gridAnomalyValues(AnomalyEvent event) {
        return List.of(
            gridAnomalyId(event),
            event.getDetectionTs(),
            text(event.getGridId()),
            text(event.getAnomalyType()),
            event.getEventTs(),
            event.getLatitude(),
            event.getLongitude(),
            text(event.getSeverity()),
            text(event.getRuleVersion()),
            text(event.getContextJson())
        );
    }

    private static JdbcConnectionOptions connectionOptions(StarRocksJdbcConfig config) {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl(config.jdbcUrl())
            .withDriverName(MYSQL_DRIVER)
            .withUsername(config.user())
            .withPassword(config.password())
            .build();
    }

    private static void bindValues(PreparedStatement statement, List<Object> values) throws SQLException {
        for (int i = 0; i < values.size(); i++) {
            statement.setObject(i + 1, values.get(i));
        }
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

    private static String qualifiedTable(String database, String tableName) {
        if (!SAFE_IDENTIFIER.matcher(database).matches()) {
            throw new IllegalArgumentException("Invalid StarRocks database identifier: " + database);
        }
        return "`" + database + "`." + tableName;
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
