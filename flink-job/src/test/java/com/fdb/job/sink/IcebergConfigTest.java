package com.fdb.job.sink;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class IcebergConfigTest {

    @Test
    void defaults_to_enabled_hive_catalog_table() {
        IcebergConfig config = IcebergConfig.resolve(Map.of(), new Properties());

        assertThat(config.enabled()).isTrue();
        assertThat(config.warehouse()).isEqualTo("hdfs://namenode:8020/warehouse/iceberg");
        assertThat(config.catalogName()).isEqualTo("fdb_iceberg");
        assertThat(config.database()).isEqualTo("iceberg_db");
        assertThat(config.table()).isEqualTo("cell_kpi");
        assertThat(config.metastoreUri()).isEqualTo("thrift://hive-metastore:9083");
        assertThat(config.cellAnomalyTable()).isEqualTo("cell_anomaly_events");
        assertThat(config.userAnomalyTable()).isEqualTo("user_anomaly_events");
        assertThat(config.gridAnomalyTable()).isEqualTo("grid_anomaly_events");
    }

    @Test
    void environment_overrides_properties() {
        Properties properties = new Properties();
        properties.setProperty("fdb.iceberg.enabled", "false");
        properties.setProperty("fdb.iceberg.warehouse", "file:///property");
        properties.setProperty("fdb.iceberg.catalog", "property_catalog");
        properties.setProperty("fdb.iceberg.database", "property_db");
        properties.setProperty("fdb.iceberg.kpi.table", "property_kpi_table");
        properties.setProperty("fdb.iceberg.cell.anomaly.table", "property_cell_anomaly_table");
        properties.setProperty("fdb.iceberg.user.anomaly.table", "property_user_anomaly_table");
        properties.setProperty("fdb.iceberg.grid.anomaly.table", "property_grid_anomaly_table");
        properties.setProperty("fdb.iceberg.metastore.uri", "thrift://property:9083");

        IcebergConfig config = IcebergConfig.resolve(Map.of(
            "FDB_ICEBERG_ENABLED", "true",
            "FDB_ICEBERG_WAREHOUSE", "file:///env",
            "FDB_ICEBERG_CATALOG", "env_catalog",
            "FDB_ICEBERG_DATABASE", "env_db",
            "FDB_ICEBERG_KPI_TABLE", "env_kpi_table",
            "FDB_ICEBERG_CELL_ANOMALY_TABLE", "env_cell_anomaly_table",
            "FDB_ICEBERG_USER_ANOMALY_TABLE", "env_user_anomaly_table",
            "FDB_ICEBERG_GRID_ANOMALY_TABLE", "env_grid_anomaly_table",
            "FDB_ICEBERG_METASTORE_URI", "thrift://env:9083"
        ), properties);

        assertThat(config.enabled()).isTrue();
        assertThat(config.warehouse()).isEqualTo("file:///env");
        assertThat(config.catalogName()).isEqualTo("env_catalog");
        assertThat(config.database()).isEqualTo("env_db");
        assertThat(config.table()).isEqualTo("env_kpi_table");
        assertThat(config.metastoreUri()).isEqualTo("thrift://env:9083");
        assertThat(config.cellAnomalyTable()).isEqualTo("env_cell_anomaly_table");
        assertThat(config.userAnomalyTable()).isEqualTo("env_user_anomaly_table");
        assertThat(config.gridAnomalyTable()).isEqualTo("env_grid_anomaly_table");
    }

    @Test
    void kpi_table_falls_back_to_legacy_table_configuration() {
        IcebergConfig config = IcebergConfig.resolve(
            Map.of("FDB_ICEBERG_TABLE", "legacy_cell_kpi"), new Properties());

        assertThat(config.table()).isEqualTo("legacy_cell_kpi");
    }

    @Test
    void invalid_enabled_value_falls_back_to_default() {
        IcebergConfig config = IcebergConfig.resolve(
            Map.of("FDB_ICEBERG_ENABLED", "maybe"), new Properties());

        assertThat(config.enabled()).isTrue();
    }
}
