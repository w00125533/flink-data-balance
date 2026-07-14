package com.fdb.observability.service;

import com.fdb.observability.model.AnomalyResultRow;
import com.fdb.observability.model.KpiResultRow;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public class StarRocksQueryService {
  private static final int DEFAULT_LIMIT = 100;
  private static final int MAX_LIMIT = 1000;

  private final ConnectionFactory connectionFactory;

  public StarRocksQueryService() {
    this(defaultConnectionFactory(System.getenv(), System.getProperties()));
  }

  public StarRocksQueryService(ConnectionFactory connectionFactory) {
    this.connectionFactory = connectionFactory;
  }

  public List<KpiResultRow> queryKpi1m(Map<String, String> queryParameters) throws SQLException {
    return queryKpi("kpi_1m", queryParameters);
  }

  public List<KpiResultRow> queryKpi5m(Map<String, String> queryParameters) throws SQLException {
    return queryKpi("kpi_5m", queryParameters);
  }

  public List<AnomalyResultRow> queryCellAnomalies(Map<String, String> queryParameters) throws SQLException {
    return queryAnomalies("cell_anomaly_events", queryParameters);
  }

  public List<AnomalyResultRow> queryGridAnomalies(Map<String, String> queryParameters) throws SQLException {
    return queryAnomalies("grid_anomaly_events", queryParameters);
  }

  private List<KpiResultRow> queryKpi(String table, Map<String, String> queryParameters) throws SQLException {
    QuerySpec spec = new QuerySpec(
        table,
        "window_start_ts",
        "window_end_ts",
        "window_start_ts",
        List.of(
            new Filter("siteId", "site_id"),
            new Filter("cellId", "cell_id"),
            new Filter("gridId", "grid_id"),
            new Filter("joinQuality", "join_quality")));
    return executeQuery(spec, queryParameters, this::readKpiRow);
  }

  private List<AnomalyResultRow> queryAnomalies(String table, Map<String, String> queryParameters)
      throws SQLException {
    QuerySpec spec = new QuerySpec(
        table,
        "detection_ts",
        "detection_ts",
        "detection_ts",
        List.of(
            new Filter("siteId", "site_id"),
            new Filter("cellId", "cell_id"),
            new Filter("gridId", "grid_id"),
            new Filter("severity", "severity"),
            new Filter("anomalyType", "anomaly_type")));
    return executeQuery(spec, queryParameters, this::readAnomalyRow);
  }

  private <T> List<T> executeQuery(QuerySpec spec, Map<String, String> queryParameters, RowMapper<T> mapper)
      throws SQLException {
    List<Object> parameters = new ArrayList<>();
    StringBuilder sql = new StringBuilder("SELECT * FROM ")
        .append(spec.table())
        .append(" WHERE ")
        .append(spec.startColumn())
        .append(" >= ? AND ")
        .append(spec.endColumn())
        .append(" <= ?");
    parameters.add(parseLong(queryParameters.get("startTs"), 0L));
    parameters.add(parseLong(queryParameters.get("endTs"), Long.MAX_VALUE));

    for (Filter filter : spec.filters()) {
      String value = firstPresent(queryParameters, filter.queryName(), filter.columnName());
      if (value != null && !value.isBlank()) {
        sql.append(" AND ").append(filter.columnName()).append(" = ?");
        parameters.add(value);
      }
    }

    sql.append(" ORDER BY ").append(spec.orderColumn()).append(" DESC LIMIT ?");
    parameters.add(parseLimit(queryParameters.get("limit")));

    try (Connection connection = connectionFactory.open();
         PreparedStatement statement = connection.prepareStatement(sql.toString())) {
      bindParameters(statement, parameters);
      try (ResultSet rs = statement.executeQuery()) {
        List<T> rows = new ArrayList<>();
        while (rs.next()) {
          rows.add(mapper.map(rs));
        }
        return rows;
      }
    }
  }

  private KpiResultRow readKpiRow(ResultSet rs) throws SQLException {
    Map<String, String> columns = columns(rs);
    return new KpiResultRow(
        longValue(rs, columns, "window_start_ts", "windowStartTs"),
        longValue(rs, columns, "window_end_ts", "windowEndTs"),
        stringValue(rs, columns, "window_kind", "windowKind"),
        stringValue(rs, columns, "join_quality", "joinQuality"),
        stringValue(rs, columns, "site_id", "siteId"),
        stringValue(rs, columns, "cell_id", "cellId"),
        stringValue(rs, columns, "grid_id", "gridId"),
        longValue(rs, columns, "num_chr_events", "numChrEvents"),
        longValue(rs, columns, "num_users", "numUsers"),
        doubleValue(rs, columns, "avg_rsrp", "avgRsrp"),
        doubleValue(rs, columns, "avg_sinr", "avgSinr"),
        doubleValue(rs, columns, "avg_prb_usage_dl", "avgPrbUsageDl"),
        doubleValue(rs, columns, "throughput_dl_mbps_avg", "throughputDlMbpsAvg"),
        doubleValue(rs, columns, "drop_rate", "dropRate"),
        doubleValue(rs, columns, "ho_success_rate", "hoSuccessRate"),
        doubleValue(rs, columns, "attach_success_rate", "attachSuccessRate"));
  }

  private AnomalyResultRow readAnomalyRow(ResultSet rs) throws SQLException {
    Map<String, String> columns = columns(rs);
    return new AnomalyResultRow(
        longValue(rs, columns, "detection_ts", "detectionTs"),
        longValue(rs, columns, "event_ts", "eventTs"),
        stringValue(rs, columns, "site_id", "siteId"),
        stringValue(rs, columns, "cell_id", "cellId"),
        stringValue(rs, columns, "grid_id", "gridId"),
        stringValue(rs, columns, "anomaly_type", "anomalyType"),
        stringValue(rs, columns, "severity"),
        stringValue(rs, columns, "context_json", "contextJson"),
        doubleValue(rs, columns, "latitude"),
        doubleValue(rs, columns, "longitude"),
        stringValue(rs, columns, "rule_version", "ruleVersion"));
  }

  private static void bindParameters(PreparedStatement statement, List<Object> parameters) throws SQLException {
    for (int i = 0; i < parameters.size(); i++) {
      Object value = parameters.get(i);
      int parameterIndex = i + 1;
      if (value instanceof Long longValue) {
        statement.setLong(parameterIndex, longValue);
      } else if (value instanceof Integer intValue) {
        statement.setInt(parameterIndex, intValue);
      } else {
        statement.setString(parameterIndex, String.valueOf(value));
      }
    }
  }

  private static ConnectionFactory defaultConnectionFactory(Map<String, String> env, Properties properties) {
    String url = setting(env, properties, "FDB_STARROCKS_JDBC_URL", "fdb.starrocks.jdbc.url",
        "jdbc:mysql://starrocks-fe:9030/fdb");
    String user = setting(env, properties, "FDB_STARROCKS_USER", "fdb.starrocks.user", "root");
    String password = setting(env, properties, "FDB_STARROCKS_PASSWORD", "fdb.starrocks.password", "");
    String database = setting(env, properties, "FDB_STARROCKS_DATABASE", "fdb.starrocks.database", "fdb");
    return () -> {
      Connection connection = DriverManager.getConnection(url, user, password);
      if (!database.isBlank()) {
        connection.setCatalog(database);
      }
      return connection;
    };
  }

  private static String setting(Map<String, String> env, Properties properties, String envName, String propertyName,
                                String defaultValue) {
    String value = env.get(envName);
    if (value == null || value.isBlank()) {
      value = properties.getProperty(propertyName);
    }
    return value == null ? defaultValue : value.trim();
  }

  private static long parseLong(String value, long defaultValue) {
    if (value == null || value.isBlank()) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException ignored) {
      return defaultValue;
    }
  }

  private static int parseLimit(String value) {
    long parsed = parseLong(value, DEFAULT_LIMIT);
    if (parsed < 1L) {
      return DEFAULT_LIMIT;
    }
    return (int) Math.min(parsed, MAX_LIMIT);
  }

  private static String firstPresent(Map<String, String> values, String... names) {
    for (String name : names) {
      String value = values.get(name);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  private static Map<String, String> columns(ResultSet rs) throws SQLException {
    ResultSetMetaData metaData = rs.getMetaData();
    Map<String, String> columns = new LinkedHashMap<>();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      String label = metaData.getColumnLabel(i);
      columns.put(label.toLowerCase(Locale.ROOT), label);
    }
    return columns;
  }

  private static String column(Map<String, String> columns, String... candidates) {
    for (String candidate : candidates) {
      String label = columns.get(candidate.toLowerCase(Locale.ROOT));
      if (label != null) {
        return label;
      }
    }
    return null;
  }

  private static String stringValue(ResultSet rs, Map<String, String> columns, String... candidates)
      throws SQLException {
    String column = column(columns, candidates);
    return column == null ? null : rs.getString(column);
  }

  private static Long longValue(ResultSet rs, Map<String, String> columns, String... candidates)
      throws SQLException {
    String column = column(columns, candidates);
    if (column == null) {
      return null;
    }
    Object value = rs.getObject(column);
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.longValue();
    }
    if (value instanceof Timestamp timestamp) {
      return timestamp.toInstant().toEpochMilli();
    }
    if (value instanceof Instant instant) {
      return instant.toEpochMilli();
    }
    try {
      return Long.parseLong(value.toString());
    } catch (NumberFormatException e) {
      throw invalidNumericValue(column, value, e);
    }
  }

  private static Double doubleValue(ResultSet rs, Map<String, String> columns, String... candidates)
      throws SQLException {
    String column = column(columns, candidates);
    if (column == null) {
      return null;
    }
    Object value = rs.getObject(column);
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.doubleValue();
    }
    try {
      return Double.parseDouble(value.toString());
    } catch (NumberFormatException e) {
      throw invalidNumericValue(column, value, e);
    }
  }

  private static Object objectValue(ResultSet rs, Map<String, String> columns, String... candidates)
      throws SQLException {
    String column = column(columns, candidates);
    return column == null ? null : rs.getObject(column);
  }

  private static SQLException invalidNumericValue(String column, Object value, NumberFormatException cause) {
    return new SQLException("Invalid numeric value for column '" + column + "': '" + value + "'", cause);
  }

  @FunctionalInterface
  public interface ConnectionFactory {
    Connection open() throws SQLException;
  }

  private record Filter(String queryName, String columnName) {
  }

  private record QuerySpec(
      String table,
      String startColumn,
      String endColumn,
      String orderColumn,
      List<Filter> filters
  ) {
  }

  private interface RowMapper<T> {
    T map(ResultSet rs) throws SQLException;
  }
}
