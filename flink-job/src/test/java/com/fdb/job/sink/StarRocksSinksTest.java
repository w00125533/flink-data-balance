package com.fdb.job.sink;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.AnomalyType;
import com.fdb.common.avro.CellKpi;
import com.fdb.common.avro.EntityType;
import com.fdb.common.avro.JoinQuality;
import com.fdb.common.avro.Severity;
import com.fdb.common.avro.WindowKind;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StarRocksSinksTest {

    @Test
    void connector_config_defaults_to_stream_load_endpoint_and_exactly_once() {
        StarRocksSinks.StarRocksConnectorConfig config =
            StarRocksSinks.resolveConnectorConfig(Map.of("FDB_RUN_ID", "run-a"), new Properties());

        assertThat(config.jdbcUrl()).isEqualTo("jdbc:mysql://starrocks-fe:9030");
        assertThat(config.loadUrl()).isEqualTo("starrocks-fe:8030");
        assertThat(config.database()).isEqualTo("fdb");
        assertThat(config.semantic()).isEqualTo("exactly-once");
        assertThat(config.labelPrefix()).isEqualTo("fdb-run-a");
    }

    @Test
    void connector_config_prefers_connector_specific_environment() {
        Properties properties = new Properties();
        properties.setProperty("fdb.starrocks.connector.jdbc.url", "jdbc:mysql://property-fe:9030");
        properties.setProperty("fdb.starrocks.load.url", "property-fe:8030");

        StarRocksSinks.StarRocksConnectorConfig config = StarRocksSinks.resolveConnectorConfig(
            Map.of(
                "FDB_STARROCKS_CONNECTOR_JDBC_URL", " jdbc:mysql://fe1:9030,fe2:9030 ",
                "FDB_STARROCKS_LOAD_URL", " fe1:8030;fe2:8030 ",
                "FDB_STARROCKS_USER", " sink-user ",
                "FDB_STARROCKS_PASSWORD", " sink-password ",
                "FDB_STARROCKS_DATABASE", " sink_db ",
                "FDB_STARROCKS_SINK_SEMANTIC", " at-least-once ",
                "FDB_STARROCKS_SINK_LABEL_PREFIX", " custom-label "
            ),
            properties);

        assertThat(config.jdbcUrl()).isEqualTo("jdbc:mysql://fe1:9030,fe2:9030");
        assertThat(config.loadUrl()).isEqualTo("fe1:8030;fe2:8030");
        assertThat(config.user()).isEqualTo("sink-user");
        assertThat(config.password()).isEqualTo("sink-password");
        assertThat(config.database()).isEqualTo("sink_db");
        assertThat(config.semantic()).isEqualTo("at-least-once");
        assertThat(config.labelPrefix()).isEqualTo("custom-label");
    }

    @Test
    void connector_properties_include_table_and_unique_label_suffix() {
        StarRocksSinks.StarRocksConnectorConfig config = new StarRocksSinks.StarRocksConnectorConfig(
            "jdbc:mysql://fe:9030", "fe:8030", "root", "", "fdb", "exactly-once", "fdb-run-a");

        assertThat(StarRocksSinks.connectorProperties(config, "cell_kpi", "kpi-1m"))
            .containsEntry("jdbc-url", "jdbc:mysql://fe:9030")
            .containsEntry("load-url", "fe:8030")
            .containsEntry("database-name", "fdb")
            .containsEntry("table-name", "cell_kpi")
            .containsEntry("sink.semantic", "exactly-once")
            .containsEntry("sink.label-prefix", "fdb-run-a-kpi-1m")
            .containsEntry("sink.version", "AUTO")
            .containsEntry("sink.sanitize-error-log", "true");
    }

    @Test
    void cell_kpi_schema_matches_starrocks_table_order() {
        assertThat(StarRocksSinks.cellKpiSchema().getFieldNames())
            .containsExactly(
                "window_start_ts",
                "window_kind",
                "cell_id",
                "window_end_ts",
                "join_quality",
                "site_id",
                "grid_id",
                "num_chr_events",
                "num_users",
                "rsrp_sample_count",
                "sinr_sample_count",
                "attach_attempts",
                "avg_rsrp",
                "avg_sinr",
                "avg_prb_usage_dl",
                "throughput_dl_mbps_avg",
                "drop_rate",
                "ho_success_rate",
                "attach_success_rate");
    }

    @Test
    void cell_kpi_row_builder_matches_starrocks_table_order() {
        Object[] row = new Object[StarRocksSinks.cellKpiSchema().getFieldCount() + 1];

        StarRocksSinks.cellKpiRowBuilder().accept(row, cellKpi());

        assertThat(row).containsExactly(
            1_700_000_000_000L,
            "MIN_1",
            "cell-a",
            1_700_000_060_000L,
            "JOINED",
            "site-a",
            "grid-a",
            100L,
            10L,
            80L,
            75L,
            50L,
            -95.5f,
            12.5f,
            0.42f,
            123.5f,
            0.01f,
            0.98f,
            0.99f,
            StarRocksSinks.UPSERT_OP);
    }

    @Test
    void anomaly_schema_and_row_builder_match_starrocks_table_order() {
        Object[] row = new Object[StarRocksSinks.anomalySchema().getFieldCount() + 1];
        AnomalyEvent event = anomalyEvent();

        StarRocksSinks.cellAnomalyRowBuilder().accept(row, event);

        List<Object> expected = new ArrayList<>(StarRocksSinks.cellAnomalyValues(event));
        expected.add(StarRocksSinks.UPSERT_OP);
        assertThat(row).containsExactlyElementsOf(expected);
    }

    @Test
    void connector_config_uses_properties_when_environment_missing() {
        Properties properties = new Properties();
        properties.setProperty("fdb.starrocks.connector.jdbc.url", " jdbc:mysql://property-fe:9030 ");
        properties.setProperty("fdb.starrocks.load.url", " property-fe:8030 ");
        properties.setProperty("fdb.starrocks.user", " property-user ");
        properties.setProperty("fdb.starrocks.password", " property-password ");
        properties.setProperty("fdb.starrocks.database", " property_db ");
        properties.setProperty("fdb.starrocks.sink.semantic", " at-least-once ");
        properties.setProperty("fdb.starrocks.sink.label-prefix", " property-label ");

        StarRocksSinks.StarRocksConnectorConfig config =
            StarRocksSinks.resolveConnectorConfig(Map.of(), properties);

        assertThat(config.jdbcUrl()).isEqualTo("jdbc:mysql://property-fe:9030");
        assertThat(config.loadUrl()).isEqualTo("property-fe:8030");
        assertThat(config.user()).isEqualTo("property-user");
        assertThat(config.password()).isEqualTo("property-password");
        assertThat(config.database()).isEqualTo("property_db");
        assertThat(config.semantic()).isEqualTo("at-least-once");
        assertThat(config.labelPrefix()).isEqualTo("property-label");
    }

    @Test
    void connector_config_rejects_unsupported_semantic() {
        assertThatThrownBy(() -> StarRocksSinks.resolveConnectorConfig(
            Map.of("FDB_STARROCKS_SINK_SEMANTIC", "best-effort"),
            new Properties()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("FDB_STARROCKS_SINK_SEMANTIC");
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

    @Test
    void starrocks_init_converges_existing_cell_kpi_tables_to_connector_schema() throws Exception {
        String deploy = readDeployScript();

        assertThat(deploy)
            .contains("ADD COLUMN join_quality VARCHAR(16) NOT NULL DEFAULT 'JOINED' AFTER window_end_ts")
            .contains("ADD COLUMN rsrp_sample_count BIGINT NOT NULL DEFAULT \"0\" AFTER num_users")
            .contains("ADD COLUMN sinr_sample_count BIGINT NOT NULL DEFAULT \"0\" AFTER rsrp_sample_count")
            .contains("ADD COLUMN attach_attempts BIGINT NOT NULL DEFAULT \"0\" AFTER sinr_sample_count");
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

    private static CellKpi cellKpi() {
        return CellKpi.newBuilder()
            .setWindowStartTs(1_700_000_000_000L)
            .setWindowEndTs(1_700_000_060_000L)
            .setWindowKind(WindowKind.MIN_1)
            .setJoinQuality(JoinQuality.JOINED)
            .setSiteId("site-a")
            .setCellId("cell-a")
            .setGridId("grid-a")
            .setNumChrEvents(100L)
            .setNumUsers(10L)
            .setRsrpSampleCount(80L)
            .setSinrSampleCount(75L)
            .setAttachAttempts(50L)
            .setAvgRsrp(-95.5f)
            .setAvgSinr(12.5f)
            .setAvgPrbUsageDl(0.42f)
            .setThroughputDlMbpsAvg(123.5f)
            .setDropRate(0.01f)
            .setHoSuccessRate(0.98f)
            .setAttachSuccessRate(0.99f)
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

    private static String readDeployScript() throws Exception {
        Path fromRoot = Path.of("scripts", "deploy.sh");
        if (Files.exists(fromRoot)) {
            return Files.readString(fromRoot);
        }
        return Files.readString(Path.of("..", "scripts", "deploy.sh"));
    }
}
