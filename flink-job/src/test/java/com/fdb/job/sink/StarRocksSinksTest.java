package com.fdb.job.sink;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.EntityType;
import com.fdb.common.avro.Severity;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StarRocksSinksTest {

    @Test
    void insert_sql_targets_internal_anomaly_tables_only() {
        String columns = "(anomaly_id, detection_ts, event_ts, entity_type, entity_id, window_start_ts, window_end_ts, "
            + "imsi, site_id, cell_id, grid_id, latitude, longitude, anomaly_type, severity, rule_version, context_json)";
        assertThat(StarRocksSinks.cellAnomalyInsertSql())
            .contains("INSERT INTO cell_anomaly_events")
            .contains(columns);
        assertThat(StarRocksSinks.userAnomalyInsertSql())
            .contains("INSERT INTO user_anomaly_events")
            .contains(columns);
        assertThat(StarRocksSinks.gridAnomalyInsertSql())
            .contains("INSERT INTO grid_anomaly_events")
            .contains(columns);
        assertThat(StarRocksSinks.cellKpiInsertSql()).isEmpty();
    }

    @Test
    void insert_sql_can_qualify_tables_with_configured_database() {
        StarRocksSinks.StarRocksJdbcConfig config =
            new StarRocksSinks.StarRocksJdbcConfig("jdbc:mysql://starrocks-fe:9030", "root", "", "analytics_db");

        assertThat(StarRocksSinks.cellAnomalyInsertSql(config))
            .startsWith("INSERT INTO `analytics_db`.cell_anomaly_events");
        assertThat(StarRocksSinks.userAnomalyInsertSql(config))
            .startsWith("INSERT INTO `analytics_db`.user_anomaly_events");
        assertThat(StarRocksSinks.gridAnomalyInsertSql(config))
            .startsWith("INSERT INTO `analytics_db`.grid_anomaly_events");
    }

    @Test
    void insert_sql_rejects_unsafe_database_identifiers() {
        StarRocksSinks.StarRocksJdbcConfig config =
            new StarRocksSinks.StarRocksJdbcConfig("jdbc:mysql://starrocks-fe:9030", "root", "", "fdb;DROP");

        assertThatThrownBy(() -> StarRocksSinks.cellAnomalyInsertSql(config))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("StarRocks database");
    }

    @Test
    void starrocks_config_uses_defaults() {
        StarRocksSinks.StarRocksJdbcConfig config =
            StarRocksSinks.resolveConfig(Map.of(), new Properties());

        assertThat(config.jdbcUrl()).isEqualTo("jdbc:mysql://starrocks-fe:9030/fdb");
        assertThat(config.user()).isEqualTo("root");
        assertThat(config.password()).isEmpty();
        assertThat(config.database()).isEqualTo("fdb");
    }

    @Test
    void starrocks_config_prefers_environment_over_properties() {
        Properties properties = new Properties();
        properties.setProperty("fdb.starrocks.jdbc.url", "jdbc:mysql://property-fe:9030/property_db");
        properties.setProperty("fdb.starrocks.user", "property-user");
        properties.setProperty("fdb.starrocks.password", "property-password");
        properties.setProperty("fdb.starrocks.database", "property_db");

        StarRocksSinks.StarRocksJdbcConfig config = StarRocksSinks.resolveConfig(
            Map.of(
                "FDB_STARROCKS_JDBC_URL", " jdbc:mysql://env-fe:9030/env_db ",
                "FDB_STARROCKS_USER", " env-user ",
                "FDB_STARROCKS_PASSWORD", " env-password ",
                "FDB_STARROCKS_DATABASE", " env_db "
            ),
            properties);

        assertThat(config.jdbcUrl()).isEqualTo("jdbc:mysql://env-fe:9030/env_db");
        assertThat(config.user()).isEqualTo("env-user");
        assertThat(config.password()).isEqualTo("env-password");
        assertThat(config.database()).isEqualTo("env_db");
    }

    @Test
    void starrocks_config_uses_properties_when_environment_missing() {
        Properties properties = new Properties();
        properties.setProperty("fdb.starrocks.jdbc.url", " jdbc:mysql://property-fe:9030/property_db ");
        properties.setProperty("fdb.starrocks.user", " property-user ");
        properties.setProperty("fdb.starrocks.password", " property-password ");
        properties.setProperty("fdb.starrocks.database", " property_db ");

        StarRocksSinks.StarRocksJdbcConfig config =
            StarRocksSinks.resolveConfig(Map.of(), properties);

        assertThat(config.jdbcUrl()).isEqualTo("jdbc:mysql://property-fe:9030/property_db");
        assertThat(config.user()).isEqualTo("property-user");
        assertThat(config.password()).isEqualTo("property-password");
        assertThat(config.database()).isEqualTo("property_db");
    }

    @Test
    void cell_anomaly_values_match_starrocks_column_order() {
        AnomalyEvent event = anomalyEvent();
        String anomalyId = StarRocksSinks.cellAnomalyId(event);

        assertThat(StarRocksSinks.cellAnomalyValues(event)).containsExactly(
            anomalyId,
            1_700_000_010_000L,
            1_700_000_000_000L,
            "CELL",
            "cell-a",
            1_699_999_940_000L,
            1_700_000_000_000L,
            "460001234567890",
            "site-a",
            "cell-a",
            "grid-a",
            31.2304,
            121.4737,
            "CELL_RADIO_BAD",
            "HIGH",
            "rules-v1",
            "{\"reason\":\"rsrp\"}"
        );
    }

    @Test
    void user_anomaly_values_match_starrocks_column_order() {
        AnomalyEvent event = userAnomalyEvent();
        String anomalyId = StarRocksSinks.userAnomalyId(event);

        assertThat(StarRocksSinks.userAnomalyValues(event)).containsExactly(
            anomalyId,
            1_700_000_010_000L,
            1_700_000_000_000L,
            "USER",
            "460001234567890",
            1_699_999_400_000L,
            1_700_000_000_000L,
            "460001234567890",
            "site-a",
            "cell-a",
            "grid-a",
            31.2304,
            121.4737,
            "USER_FAILURE",
            "HIGH",
            "rules-v1",
            "{\"reason\":\"attach\"}"
        );
    }

    @Test
    void grid_anomaly_values_match_starrocks_column_order() {
        AnomalyEvent event = anomalyEvent();
        String anomalyId = StarRocksSinks.gridAnomalyId(event);

        assertThat(StarRocksSinks.gridAnomalyValues(event)).containsExactly(
            anomalyId,
            1_700_000_010_000L,
            1_700_000_000_000L,
            "CELL",
            "cell-a",
            1_699_999_940_000L,
            1_700_000_000_000L,
            "460001234567890",
            "site-a",
            "cell-a",
            "grid-a",
            31.2304,
            121.4737,
            "CELL_RADIO_BAD",
            "HIGH",
            "rules-v1",
            "{\"reason\":\"rsrp\"}"
        );
    }

    @Test
    void anomaly_values_fallback_to_empty_strings_for_null_text_fields() {
        AnomalyEvent event = AnomalyEvent.newBuilder(anomalyEvent())
            .setImsi(null)
            .setSiteId(null)
            .setCellId(null)
            .setGridId(null)
            .build();

        List<Object> cellValues = StarRocksSinks.cellAnomalyValues(event);
        assertThat(cellValues).containsSubsequence("", "", "", "");
        assertThat(cellValues.get(10)).isEqualTo("");

        List<Object> gridValues = StarRocksSinks.gridAnomalyValues(event);
        assertThat(gridValues.get(7)).isEqualTo("");
        assertThat(gridValues.get(8)).isEqualTo("");
        assertThat(gridValues.get(9)).isEqualTo("");
        assertThat(gridValues.get(10)).isEqualTo("");
    }

    @Test
    void anomaly_id_is_stable_for_duplicate_delivery_and_ignores_detection_timestamp() {
        AnomalyEvent first = anomalyEvent();
        AnomalyEvent retry = AnomalyEvent.newBuilder(first)
            .setDetectionTs(first.getDetectionTs() + 60_000L)
            .build();

        assertThat(StarRocksSinks.cellAnomalyId(first))
            .isEqualTo(StarRocksSinks.cellAnomalyId(first))
            .isEqualTo(StarRocksSinks.cellAnomalyId(retry))
            .startsWith("cell:");
        assertThat(StarRocksSinks.gridAnomalyId(first))
            .isEqualTo(StarRocksSinks.gridAnomalyId(retry))
            .startsWith("grid:");
        assertThat(StarRocksSinks.userAnomalyId(userAnomalyEvent()))
            .startsWith("user:");
    }

    @Test
    void anomaly_id_changes_when_stable_key_fields_change() {
        AnomalyEvent event = anomalyEvent();
        String base = StarRocksSinks.cellAnomalyId(event);

        assertThat(StarRocksSinks.cellAnomalyId(AnomalyEvent.newBuilder(event)
            .setEventTs(event.getEventTs() + 1L)
            .build())).isNotEqualTo(base);
        assertThat(StarRocksSinks.cellAnomalyId(AnomalyEvent.newBuilder(event)
            .setEntityId("cell-b")
            .build())).isNotEqualTo(base);
        assertThat(StarRocksSinks.cellAnomalyId(AnomalyEvent.newBuilder(event)
            .setContextJson("{\"reason\":\"different\"}")
            .build())).isNotEqualTo(base);
    }

    @Test
    void starrocks_anomaly_tables_use_primary_key_anomaly_id() throws Exception {
        String ddl = readStarRocksDdl();

        assertThat(ddl).contains("anomaly_id VARCHAR(128) NOT NULL");
        assertThat(ddl).contains("entity_type VARCHAR(16) NOT NULL");
        assertThat(ddl).contains("entity_id VARCHAR(128) NOT NULL");
        assertThat(ddl).contains("window_start_ts BIGINT NOT NULL");
        assertThat(ddl).contains("CREATE TABLE IF NOT EXISTS user_anomaly_events");
        assertThat(ddl).contains("PRIMARY KEY(anomaly_id)");
        assertThat(ddl).doesNotContain("DUPLICATE KEY(detection_ts, cell_id, anomaly_type)");
        assertThat(ddl).doesNotContain("DUPLICATE KEY(detection_ts, grid_id, anomaly_type)");
    }

    private static AnomalyEvent anomalyEvent() {
        return AnomalyEvent.newBuilder()
            .setDetectionTs(1_700_000_010_000L)
            .setEventTs(1_700_000_000_000L)
            .setEntityType(EntityType.CELL)
            .setEntityId("cell-a")
            .setWindowStartTs(1_699_999_940_000L)
            .setWindowEndTs(1_700_000_000_000L)
            .setImsi("460001234567890")
            .setSiteId("site-a")
            .setCellId("cell-a")
            .setGridId("grid-a")
            .setLatitude(31.2304)
            .setLongitude(121.4737)
            .setAnomalyType(AnomalyType.CELL_RADIO_BAD)
            .setSeverity(Severity.HIGH)
            .setRuleVersion("rules-v1")
            .setContextJson("{\"reason\":\"rsrp\"}")
            .build();
    }

    private static AnomalyEvent userAnomalyEvent() {
        return AnomalyEvent.newBuilder()
            .setDetectionTs(1_700_000_010_000L)
            .setEventTs(1_700_000_000_000L)
            .setEntityType(EntityType.USER)
            .setEntityId("460001234567890")
            .setWindowStartTs(1_699_999_400_000L)
            .setWindowEndTs(1_700_000_000_000L)
            .setImsi("460001234567890")
            .setSiteId("site-a")
            .setCellId("cell-a")
            .setGridId("grid-a")
            .setLatitude(31.2304)
            .setLongitude(121.4737)
            .setAnomalyType(AnomalyType.USER_FAILURE)
            .setSeverity(Severity.HIGH)
            .setRuleVersion("rules-v1")
            .setContextJson("{\"reason\":\"attach\"}")
            .build();
    }

    private static String readStarRocksDdl() throws Exception {
        Path fromRoot = Path.of("scripts", "init-starrocks.sql");
        if (Files.exists(fromRoot)) {
            return Files.readString(fromRoot);
        }
        return Files.readString(Path.of("..", "scripts", "init-starrocks.sql"));
    }
}
