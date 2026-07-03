package com.fdb.job;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class IcebergConfigTest {

    @Test
    void defaults_to_enabled_hadoop_catalog_table() {
        IcebergConfig config = IcebergConfig.resolve(Map.of(), new Properties());

        assertThat(config.enabled()).isTrue();
        assertThat(config.warehouse()).isEqualTo("hdfs://namenode:8020/warehouse/iceberg");
        assertThat(config.catalogName()).isEqualTo("fdb_iceberg");
        assertThat(config.database()).isEqualTo("fdb");
        assertThat(config.table()).isEqualTo("cell_kpi");
    }

    @Test
    void environment_overrides_properties() {
        Properties properties = new Properties();
        properties.setProperty("fdb.iceberg.enabled", "false");
        properties.setProperty("fdb.iceberg.warehouse", "file:///property");
        properties.setProperty("fdb.iceberg.catalog", "property_catalog");
        properties.setProperty("fdb.iceberg.database", "property_db");
        properties.setProperty("fdb.iceberg.table", "property_table");

        IcebergConfig config = IcebergConfig.resolve(Map.of(
            "FDB_ICEBERG_ENABLED", "true",
            "FDB_ICEBERG_WAREHOUSE", "file:///env",
            "FDB_ICEBERG_CATALOG", "env_catalog",
            "FDB_ICEBERG_DATABASE", "env_db",
            "FDB_ICEBERG_TABLE", "env_table"
        ), properties);

        assertThat(config.enabled()).isTrue();
        assertThat(config.warehouse()).isEqualTo("file:///env");
        assertThat(config.catalogName()).isEqualTo("env_catalog");
        assertThat(config.database()).isEqualTo("env_db");
        assertThat(config.table()).isEqualTo("env_table");
    }

    @Test
    void invalid_enabled_value_falls_back_to_default() {
        IcebergConfig config = IcebergConfig.resolve(
            Map.of("FDB_ICEBERG_ENABLED", "maybe"), new Properties());

        assertThat(config.enabled()).isTrue();
    }
}
