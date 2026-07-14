package com.fdb.job;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Map;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class IcebergSinksTest {

    @Test
    void builds_cell_kpi_table_identifier_schema_and_partition_spec() {
        IcebergConfig config = new IcebergConfig(
            true, "hdfs://namenode:8020/warehouse/iceberg", "fdb_iceberg", "fdb", "cell_kpi");

        TableIdentifier identifier = IcebergSinks.tableIdentifier(config);
        Schema schema = IcebergSinks.cellKpiSchema();
        PartitionSpec spec = IcebergSinks.cellKpiPartitionSpec(schema);

        assertThat(identifier.toString()).isEqualTo("fdb.cell_kpi");
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
    void hadoop_catalog_has_configuration(@TempDir Path warehouseDir) {
        IcebergConfig config = new IcebergConfig(
            true, warehouseDir.toUri().toString(), "fdb_iceberg", "fdb", "cell_kpi");

        HadoopCatalog catalog = IcebergSinks.hadoopCatalog(config);

        assertThat(catalog.getConf()).isNotNull();
    }

    @Test
    void finds_metadata_retention_properties_missing_from_existing_table() {
        assertThat(IcebergSinks.missingTableProperties(Map.of("format-version", "2")))
            .containsEntry("write.metadata.delete-after-commit.enabled", "true")
            .containsEntry("write.metadata.previous-versions-max", "20");
    }
}
