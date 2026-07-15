package com.fdb.job.sink;

import com.fdb.common.avro.AnomalyEvent;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
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
        return anomalyInsertSql("cell_anomaly_events");
    }

    static String cellAnomalyInsertSql(StarRocksJdbcConfig config) {
        return anomalyInsertSql(qualifiedTable(config.database(), "cell_anomaly_events"));
    }

    public static String userAnomalyInsertSql() {
        return anomalyInsertSql("user_anomaly_events");
    }

    static String userAnomalyInsertSql(StarRocksJdbcConfig config) {
        return anomalyInsertSql(qualifiedTable(config.database(), "user_anomaly_events"));
    }

    public static String gridAnomalyInsertSql() {
        return anomalyInsertSql("grid_anomaly_events");
    }

    static String gridAnomalyInsertSql(StarRocksJdbcConfig config) {
        return anomalyInsertSql(qualifiedTable(config.database(), "grid_anomaly_events"));
    }

    private static String anomalyInsertSql(String table) {
        return "INSERT INTO " + table + " "
            + "(anomaly_id, detection_ts, event_ts, entity_type, entity_id, window_start_ts, window_end_ts, "
            + "imsi, site_id, cell_id, grid_id, latitude, longitude, anomaly_type, severity, rule_version, context_json) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    public static String cellKpiInsertSql() {
        return "";
    }

    public static org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink<AnomalyEvent> cellAnomalySink() {
        StarRocksJdbcConfig config = resolveConfig(System.getenv(), System.getProperties());
        return JdbcSink.<AnomalyEvent>builder()
            .withQueryStatement(cellAnomalyInsertSql(config), (statement, event) ->
                bindValues(statement, anomalyValues("cell", event)))
            .withExecutionOptions(JdbcExecutionOptions.defaults())
            .buildAtLeastOnce(connectionOptions(config));
    }

    public static org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink<AnomalyEvent> userAnomalySink() {
        StarRocksJdbcConfig config = resolveConfig(System.getenv(), System.getProperties());
        return JdbcSink.<AnomalyEvent>builder()
            .withQueryStatement(userAnomalyInsertSql(config), (statement, event) ->
                bindValues(statement, anomalyValues("user", event)))
            .withExecutionOptions(JdbcExecutionOptions.defaults())
            .buildAtLeastOnce(connectionOptions(config));
    }

    public static org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink<AnomalyEvent> gridAnomalySink() {
        StarRocksJdbcConfig config = resolveConfig(System.getenv(), System.getProperties());
        return JdbcSink.<AnomalyEvent>builder()
            .withQueryStatement(gridAnomalyInsertSql(config), (statement, event) ->
                bindValues(statement, anomalyValues("grid", event)))
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
        return anomalyId("cell", event);
    }

    static String userAnomalyId(AnomalyEvent event) {
        return anomalyId("user", event);
    }

    static String gridAnomalyId(AnomalyEvent event) {
        return anomalyId("grid", event);
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

    static List<Object> cellAnomalyValues(AnomalyEvent event) {
        return anomalyValues("cell", event);
    }

    static List<Object> userAnomalyValues(AnomalyEvent event) {
        return anomalyValues("user", event);
    }

    static List<Object> gridAnomalyValues(AnomalyEvent event) {
        return anomalyValues("grid", event);
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
