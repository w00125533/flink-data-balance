package com.fdb.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

import java.util.Map;

public final class IcebergSinks {

    private IcebergSinks() {}

    static TableIdentifier tableIdentifier(IcebergConfig config) {
        return TableIdentifier.of(config.database(), config.table());
    }

    static Schema cellKpiSchema() {
        return new Schema(
            Types.NestedField.required(1, "window_start_ts", Types.LongType.get()),
            Types.NestedField.required(2, "window_end_ts", Types.LongType.get()),
            Types.NestedField.required(3, "site_id", Types.StringType.get()),
            Types.NestedField.required(4, "cell_id", Types.StringType.get()),
            Types.NestedField.required(5, "grid_id", Types.StringType.get()),
            Types.NestedField.required(6, "num_chr_events", Types.LongType.get()),
            Types.NestedField.required(7, "num_users", Types.LongType.get()),
            Types.NestedField.required(8, "avg_rsrp", Types.FloatType.get()),
            Types.NestedField.required(9, "avg_sinr", Types.FloatType.get()),
            Types.NestedField.required(10, "avg_prb_usage_dl", Types.FloatType.get()),
            Types.NestedField.required(11, "throughput_dl_mbps_avg", Types.FloatType.get()),
            Types.NestedField.required(12, "drop_rate", Types.FloatType.get()),
            Types.NestedField.required(13, "ho_success_rate", Types.FloatType.get()),
            Types.NestedField.required(14, "attach_success_rate", Types.FloatType.get()),
            Types.NestedField.required(15, "window_kind", Types.StringType.get()),
            Types.NestedField.required(16, "dt", Types.StringType.get()),
            Types.NestedField.required(17, "hour", Types.StringType.get()));
    }

    static PartitionSpec cellKpiPartitionSpec(Schema schema) {
        return PartitionSpec.builderFor(schema)
            .identity("window_kind")
            .identity("dt")
            .identity("hour")
            .build();
    }

    static Map<String, String> tableProperties() {
        return Map.of(
            "format-version", "2",
            "write.metadata.delete-after-commit.enabled", "true",
            "write.metadata.previous-versions-max", "20");
    }

    static Map<String, String> missingTableProperties(Map<String, String> existingProperties) {
        return tableProperties().entrySet().stream()
            .filter(entry -> !entry.getValue().equals(existingProperties.get(entry.getKey())))
            .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    static Table ensureTable(IcebergConfig config) {
        HadoopCatalog catalog = hadoopCatalog(config);
        Namespace namespace = Namespace.of(config.database());
        try {
            catalog.createNamespace(namespace);
        } catch (AlreadyExistsException ignored) {
            // Existing namespace is the normal path after the first run.
        }
        TableIdentifier identifier = tableIdentifier(config);
        if (catalog.tableExists(identifier)) {
            Table table = catalog.loadTable(identifier);
            Map<String, String> missingProperties = missingTableProperties(table.properties());
            if (!missingProperties.isEmpty()) {
                org.apache.iceberg.UpdateProperties update = table.updateProperties();
                missingProperties.forEach(update::set);
                update.commit();
                return catalog.loadTable(identifier);
            }
            return table;
        }
        Schema schema = cellKpiSchema();
        return catalog.createTable(identifier, schema, cellKpiPartitionSpec(schema), tableProperties());
    }

    static HadoopCatalog hadoopCatalog(IcebergConfig config) {
        HadoopCatalog catalog = new HadoopCatalog();
        catalog.setConf(new Configuration());
        catalog.initialize(config.catalogName(), Map.of("warehouse", config.warehouse()));
        return catalog;
    }

    public static DataStreamSink<Void> appendCellKpiSink(DataStream<RowData> stream, IcebergConfig config) {
        ensureTable(config);
        CatalogLoader catalogLoader = CatalogLoader.hadoop(
            config.catalogName(), new Configuration(), Map.of("warehouse", config.warehouse()));
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier(config));
        return FlinkSink.forRowData(stream)
            .tableLoader(tableLoader)
            .append();
    }
}
