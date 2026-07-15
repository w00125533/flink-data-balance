package com.fdb.job.sink;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Map;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class IcebergSinksTest {

    @Test
    void builds_cell_kpi_table_identifier_schema_and_partition_spec() {
        IcebergConfig config = new IcebergConfig(
            true, "hdfs://namenode:8020/warehouse/iceberg", "fdb_iceberg", "iceberg_db", "cell_kpi",
            "thrift://hive-metastore:9083");

        TableIdentifier identifier = IcebergSinks.cellKpiIdentifier(config);
        Schema schema = IcebergSinks.cellKpiSchema();
        PartitionSpec spec = IcebergSinks.cellKpiPartitionSpec(schema);

        assertThat(identifier.toString()).isEqualTo("iceberg_db.cell_kpi");
        assertThat(IcebergSinks.tableIdentifier(config)).isEqualTo(identifier);
        assertThat(schema.columns()).hasSize(21);
        assertThat(schema.columns().stream().map(field -> field.name()))
            .containsExactly(
                "window_start_ts", "window_end_ts", "site_id", "cell_id", "grid_id",
                "num_chr_events", "num_users", "rsrp_sample_count", "sinr_sample_count",
                "attach_attempts", "avg_rsrp", "avg_sinr", "avg_prb_usage_dl",
                "throughput_dl_mbps_avg", "drop_rate", "ho_success_rate",
                "attach_success_rate", "join_quality", "window_kind", "dt", "hour");
        assertThat(spec.fields().stream().map(field -> field.name()))
            .containsExactly("window_kind", "dt", "hour");
        assertThat(IcebergSinks.tableProperties()).containsEntry("format-version", "2");
        assertThat(IcebergSinks.tableProperties())
            .containsEntry("write.metadata.delete-after-commit.enabled", "true")
            .containsEntry("write.metadata.previous-versions-max", "20");
    }

    @Test
    void builds_independent_business_table_identifiers() {
        IcebergConfig config = new IcebergConfig(
            true, "hdfs://namenode:8020/warehouse/iceberg", "fdb_iceberg", "iceberg_db", "cell_kpi",
            "thrift://hive-metastore:9083", "cell_anomaly_events", "user_anomaly_events",
            "grid_anomaly_events");

        assertThat(IcebergSinks.cellKpiIdentifier(config).toString()).isEqualTo("iceberg_db.cell_kpi");
        assertThat(IcebergSinks.cellAnomalyIdentifier(config).toString())
            .isEqualTo("iceberg_db.cell_anomaly_events");
        assertThat(IcebergSinks.userAnomalyIdentifier(config).toString())
            .isEqualTo("iceberg_db.user_anomaly_events");
        assertThat(IcebergSinks.gridAnomalyIdentifier(config).toString())
            .isEqualTo("iceberg_db.grid_anomaly_events");
    }

    @Test
    void builds_anomaly_schema_and_partition_spec() {
        Schema schema = IcebergSinks.anomalySchema();
        PartitionSpec spec = IcebergSinks.anomalyPartitionSpec(schema);

        assertThat(schema.columns()).hasSize(18);
        assertThat(schema.columns().stream().map(field -> field.name()))
            .containsExactly(
                "detection_ts", "event_ts", "entity_type", "entity_id", "window_start_ts",
                "window_end_ts", "imsi", "site_id", "cell_id", "grid_id", "latitude",
                "longitude", "anomaly_type", "severity", "rule_version", "context_json",
                "dt", "hour");
        assertThat(spec.fields().stream().map(field -> field.name()))
            .containsExactly("dt", "hour");
    }

    @Test
    void hive_catalog_has_configuration(@TempDir Path warehouseDir) {
        IcebergConfig config = new IcebergConfig(
            true, warehouseDir.toUri().toString(), "fdb_iceberg", "iceberg_db", "cell_kpi",
            "thrift://hive-metastore:9083");

        HiveCatalog catalog = IcebergSinks.hiveCatalog(config);

        assertThat(catalog.getConf()).isNotNull();
        assertThat(IcebergSinks.catalogProperties(config))
            .containsEntry("warehouse", warehouseDir.toUri().toString())
            .containsEntry("uri", "thrift://hive-metastore:9083");
    }

    @Test
    void finds_metadata_retention_properties_missing_from_existing_table() {
        assertThat(IcebergSinks.missingTableProperties(Map.of("format-version", "2")))
            .containsEntry("write.metadata.delete-after-commit.enabled", "true")
            .containsEntry("write.metadata.previous-versions-max", "20");
    }
}
