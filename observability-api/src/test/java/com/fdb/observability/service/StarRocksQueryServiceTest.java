package com.fdb.observability.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fdb.observability.model.AnomalyResultRow;
import com.fdb.observability.model.KpiResultRow;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StarRocksQueryServiceTest {

  @Test
  void kpiQueryUsesPreparedPredicatesInStableParameterOrder() throws Exception {
    CapturingJdbc jdbc = new CapturingJdbc();
    StarRocksQueryService service = new StarRocksQueryService(jdbc::connection);

    service.queryKpi1m(Map.of(
        "startTs", "1000",
        "endTs", "61000",
        "cellId", "CELL-001",
        "siteId", "SITE-001",
        "joinQuality", "JOINED",
        "limit", "25"));

    assertThat(jdbc.sql()).isEqualTo("SELECT * FROM kpi_1m WHERE window_start_ts >= ?"
        + " AND window_end_ts <= ? AND site_id = ? AND cell_id = ? AND join_quality = ?"
        + " ORDER BY window_start_ts DESC LIMIT ?");
    assertThat(jdbc.parameters()).containsExactly(1000L, 61000L, "SITE-001", "CELL-001", "JOINED", 25);
  }

  @Test
  void anomalyQueryUsesPreparedPredicatesAndCapsLimit() throws Exception {
    CapturingJdbc jdbc = new CapturingJdbc();
    StarRocksQueryService service = new StarRocksQueryService(jdbc::connection);

    service.queryGridAnomalies(Map.of(
        "startTs", "900",
        "endTs", "2000",
        "gridId", "wx4g0e",
        "severity", "HIGH",
        "anomalyType", "LOW_SIGNAL",
        "limit", "5000"));

    assertThat(jdbc.sql()).isEqualTo("SELECT * FROM grid_anomaly_events WHERE detection_ts >= ?"
        + " AND detection_ts <= ? AND grid_id = ? AND severity = ? AND anomaly_type = ?"
        + " ORDER BY detection_ts DESC LIMIT ?");
    assertThat(jdbc.parameters()).containsExactly(900L, 2000L, "wx4g0e", "HIGH", "LOW_SIGNAL", 1000);
  }

  @Test
  void userAnomalyQueryUsesEntityPredicatesInStableParameterOrder() throws Exception {
    CapturingJdbc jdbc = new CapturingJdbc();
    StarRocksQueryService service = new StarRocksQueryService(jdbc::connection);

    service.queryUserAnomalies(Map.of(
        "startTs", "100",
        "endTs", "900",
        "entityType", "USER",
        "entityId", "460001234567890",
        "imsi", "460001234567890",
        "cellId", "CELL-001",
        "severity", "HIGH",
        "anomalyType", "USER_QOE_BAD",
        "limit", "50"));

    assertThat(jdbc.sql()).isEqualTo("SELECT * FROM user_anomaly_events WHERE detection_ts >= ?"
        + " AND detection_ts <= ? AND entity_type = ? AND entity_id = ? AND imsi = ? AND cell_id = ?"
        + " AND severity = ? AND anomaly_type = ? ORDER BY detection_ts DESC LIMIT ?");
    assertThat(jdbc.parameters()).containsExactly(
        100L, 900L, "USER", "460001234567890", "460001234567890", "CELL-001", "HIGH", "USER_QOE_BAD", 50);
  }

  @Test
  void mapsEntityAwareAnomalyColumns() throws Exception {
    RowJdbc jdbc = new RowJdbc(row(
        "detection_ts", 1000L,
        "event_ts", 900L,
        "entity_type", "USER",
        "entity_id", "460001234567890",
        "window_start_ts", 100L,
        "window_end_ts", 900L,
        "imsi", "460001234567890",
        "site_id", "SITE-001",
        "cell_id", "CELL-001",
        "grid_id", "wx4g0e",
        "anomaly_type", "USER_QOE_BAD",
        "severity", "HIGH",
        "context_json", "{\"metric\":\"latencyMs\"}",
        "latitude", 39.9d,
        "longitude", 116.4d,
        "rule_version", "v1.0"));
    StarRocksQueryService service = new StarRocksQueryService(jdbc::connection);

    List<AnomalyResultRow> rows = service.queryUserAnomalies(Map.of());

    assertThat(rows).singleElement().satisfies(row -> {
      assertThat(row.detectionTs()).isEqualTo(1000L);
      assertThat(row.eventTs()).isEqualTo(900L);
      assertThat(row.entityType()).isEqualTo("USER");
      assertThat(row.entityId()).isEqualTo("460001234567890");
      assertThat(row.windowStartTs()).isEqualTo(100L);
      assertThat(row.windowEndTs()).isEqualTo(900L);
      assertThat(row.imsi()).isEqualTo("460001234567890");
      assertThat(row.siteId()).isEqualTo("SITE-001");
      assertThat(row.cellId()).isEqualTo("CELL-001");
      assertThat(row.gridId()).isEqualTo("wx4g0e");
      assertThat(row.anomalyType()).isEqualTo("USER_QOE_BAD");
      assertThat(row.severity()).isEqualTo("HIGH");
      assertThat(row.contextJson()).isEqualTo("{\"metric\":\"latencyMs\"}");
      assertThat(row.latitude()).isEqualTo(39.9d);
      assertThat(row.longitude()).isEqualTo(116.4d);
      assertThat(row.ruleVersion()).isEqualTo("v1.0");
    });
  }

  @Test
  void wrapsNonNumericLongColumnMappingFailureAsSqlException() {
    RowJdbc jdbc = new RowJdbc(row("window_start_ts", "not-a-long"));
    StarRocksQueryService service = new StarRocksQueryService(jdbc::connection);

    assertThatThrownBy(() -> service.queryKpi1m(Map.of()))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("window_start_ts")
        .hasMessageContaining("not-a-long");
  }

  @Test
  void wrapsNonNumericDoubleColumnMappingFailureAsSqlException() {
    RowJdbc jdbc = new RowJdbc(row(
        "window_start_ts", 1000L,
        "window_end_ts", 61_000L,
        "avg_rsrp", "not-a-double"));
    StarRocksQueryService service = new StarRocksQueryService(jdbc::connection);

    assertThatThrownBy(() -> service.queryKpi1m(Map.of()))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("avg_rsrp")
        .hasMessageContaining("not-a-double");
  }

  @Test
  void mapsCaseInsensitiveColumnLabels() throws Exception {
    RowJdbc jdbc = new RowJdbc(row(
        "WINDOW_START_TS", 1000L,
        "WINDOW_END_TS", 61_000L,
        "WINDOW_KIND", "MIN_1",
        "AVG_RSRP", -92.5d));
    StarRocksQueryService service = new StarRocksQueryService(jdbc::connection);

    List<KpiResultRow> rows = service.queryKpi1m(Map.of());

    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).windowStartTs()).isEqualTo(1000L);
    assertThat(rows.get(0).windowEndTs()).isEqualTo(61_000L);
    assertThat(rows.get(0).windowKind()).isEqualTo("MIN_1");
    assertThat(rows.get(0).avgRsrp()).isEqualTo(-92.5d);
  }

  private static final class CapturingJdbc {
    private String sql;
    private final List<Object> parameters = new ArrayList<>();

    Connection connection() {
      return proxy(Connection.class, (proxy, method, args) -> {
        if ("prepareStatement".equals(method.getName())) {
          sql = (String) args[0];
          return preparedStatement();
        }
        if ("close".equals(method.getName())) {
          return null;
        }
        return defaultValue(method.getReturnType());
      });
    }

    String sql() {
      return sql;
    }

    List<Object> parameters() {
      return parameters;
    }

    private PreparedStatement preparedStatement() {
      return proxy(PreparedStatement.class, (proxy, method, args) -> {
        switch (method.getName()) {
          case "setLong", "setString", "setInt" -> {
            parameters.add(args[1]);
            return null;
          }
          case "executeQuery" -> {
            return emptyResultSet();
          }
          case "close" -> {
            return null;
          }
          default -> {
            return defaultValue(method.getReturnType());
          }
        }
      });
    }

    private ResultSet emptyResultSet() {
      return proxy(ResultSet.class, (proxy, method, args) -> {
        if ("next".equals(method.getName())) {
          return false;
        }
        if ("close".equals(method.getName())) {
          return null;
        }
        return defaultValue(method.getReturnType());
      });
    }
  }

  private static final class RowJdbc {
    private final Map<String, Object> row;
    private boolean consumed;

    private RowJdbc(Map<String, Object> row) {
      this.row = row;
    }

    Connection connection() {
      return proxy(Connection.class, (proxy, method, args) -> {
        if ("prepareStatement".equals(method.getName())) {
          return preparedStatement();
        }
        if ("close".equals(method.getName())) {
          return null;
        }
        return defaultValue(method.getReturnType());
      });
    }

    private PreparedStatement preparedStatement() {
      return proxy(PreparedStatement.class, (proxy, method, args) -> {
        switch (method.getName()) {
          case "executeQuery" -> {
            return resultSet();
          }
          case "close", "setLong", "setString", "setInt" -> {
            return null;
          }
          default -> {
            return defaultValue(method.getReturnType());
          }
        }
      });
    }

    private ResultSet resultSet() {
      return proxy(ResultSet.class, (proxy, method, args) -> {
        switch (method.getName()) {
          case "next" -> {
            if (consumed) {
              return false;
            }
            consumed = true;
            return true;
          }
          case "getMetaData" -> {
            return metaData();
          }
          case "getObject", "getString" -> {
            return row.get((String) args[0]);
          }
          case "close" -> {
            return null;
          }
          default -> {
            return defaultValue(method.getReturnType());
          }
        }
      });
    }

    private ResultSetMetaData metaData() {
      List<String> labels = new ArrayList<>(row.keySet());
      return proxy(ResultSetMetaData.class, (proxy, method, args) -> switch (method.getName()) {
        case "getColumnCount" -> labels.size();
        case "getColumnLabel" -> labels.get((int) args[0] - 1);
        default -> defaultValue(method.getReturnType());
      });
    }
  }

  private static Map<String, Object> row(Object... keysAndValues) {
    Map<String, Object> row = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      row.put((String) keysAndValues[i], keysAndValues[i + 1]);
    }
    return row;
  }

  @SuppressWarnings("unchecked")
  private static <T> T proxy(Class<T> type, InvocationHandler handler) {
    return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
  }

  private static Object defaultValue(Class<?> type) throws SQLException {
    if (type == boolean.class) {
      return false;
    }
    if (type == int.class) {
      return 0;
    }
    if (type == long.class) {
      return 0L;
    }
    if (type == double.class) {
      return 0.0d;
    }
    if (type == float.class) {
      return 0.0f;
    }
    return null;
  }
}
