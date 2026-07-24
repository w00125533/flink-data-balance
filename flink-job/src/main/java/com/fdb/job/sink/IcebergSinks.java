package com.fdb.job.sink;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.CellKpi;
import com.fdb.job.metrics.ConnectorSinkMetrics;
import com.fdb.job.metrics.InstrumentedIcebergSink;
import com.fdb.job.metrics.MetricRuntimeConfig;
import com.fdb.job.metrics.SinkLatencyProbe;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
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
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.types.Types;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public final class IcebergSinks {

    private IcebergSinks() {}

    static TableIdentifier tableIdentifier(IcebergConfig config) {
        return cellKpiIdentifier(config);
    }

    static TableIdentifier cellKpiIdentifier(IcebergConfig config) {
        return TableIdentifier.of(config.database(), config.table());
    }

    static TableIdentifier cellAnomalyIdentifier(IcebergConfig config) {
        return TableIdentifier.of(config.database(), config.cellAnomalyTable());
    }

    static TableIdentifier userAnomalyIdentifier(IcebergConfig config) {
        return TableIdentifier.of(config.database(), config.userAnomalyTable());
    }

    static TableIdentifier gridAnomalyIdentifier(IcebergConfig config) {
        return TableIdentifier.of(config.database(), config.gridAnomalyTable());
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
            Types.NestedField.required(8, "rsrp_sample_count", Types.LongType.get()),
            Types.NestedField.required(9, "sinr_sample_count", Types.LongType.get()),
            Types.NestedField.required(10, "attach_attempts", Types.LongType.get()),
            Types.NestedField.required(11, "avg_rsrp", Types.FloatType.get()),
            Types.NestedField.required(12, "avg_sinr", Types.FloatType.get()),
            Types.NestedField.required(13, "avg_prb_usage_dl", Types.FloatType.get()),
            Types.NestedField.required(14, "throughput_dl_mbps_avg", Types.FloatType.get()),
            Types.NestedField.required(15, "drop_rate", Types.FloatType.get()),
            Types.NestedField.required(16, "ho_success_rate", Types.FloatType.get()),
            Types.NestedField.required(17, "attach_success_rate", Types.FloatType.get()),
            Types.NestedField.required(18, "join_quality", Types.StringType.get()),
            Types.NestedField.required(19, "window_kind", Types.StringType.get()),
            Types.NestedField.required(20, "dt", Types.StringType.get()),
            Types.NestedField.required(21, "hour", Types.StringType.get()));
    }

    static PartitionSpec cellKpiPartitionSpec(Schema schema) {
        return PartitionSpec.builderFor(schema)
            .identity("window_kind")
            .identity("dt")
            .identity("hour")
            .build();
    }

    static Schema anomalySchema() {
        return new Schema(
            Types.NestedField.required(1, "detection_ts", Types.LongType.get()),
            Types.NestedField.required(2, "event_ts", Types.LongType.get()),
            Types.NestedField.required(3, "entity_type", Types.StringType.get()),
            Types.NestedField.required(4, "entity_id", Types.StringType.get()),
            Types.NestedField.required(5, "window_start_ts", Types.LongType.get()),
            Types.NestedField.required(6, "window_end_ts", Types.LongType.get()),
            Types.NestedField.optional(7, "imsi", Types.StringType.get()),
            Types.NestedField.optional(8, "site_id", Types.StringType.get()),
            Types.NestedField.optional(9, "cell_id", Types.StringType.get()),
            Types.NestedField.optional(10, "grid_id", Types.StringType.get()),
            Types.NestedField.optional(11, "latitude", Types.DoubleType.get()),
            Types.NestedField.optional(12, "longitude", Types.DoubleType.get()),
            Types.NestedField.required(13, "anomaly_type", Types.StringType.get()),
            Types.NestedField.required(14, "severity", Types.StringType.get()),
            Types.NestedField.required(15, "rule_version", Types.StringType.get()),
            Types.NestedField.required(16, "context_json", Types.StringType.get()),
            Types.NestedField.required(17, "dt", Types.StringType.get()),
            Types.NestedField.required(18, "hour", Types.StringType.get()));
    }

    static PartitionSpec anomalyPartitionSpec(Schema schema) {
        return PartitionSpec.builderFor(schema)
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
        Schema schema = cellKpiSchema();
        return ensureTable(config, cellKpiIdentifier(config), schema, cellKpiPartitionSpec(schema));
    }

    static Table ensureTable(IcebergConfig config, TableIdentifier identifier, Schema schema, PartitionSpec spec) {
        HiveCatalog catalog = hiveCatalog(config);
        Namespace namespace = Namespace.of(config.database());
        try {
            catalog.createNamespace(namespace, Map.of("location", config.warehouse() + "/" + config.database()));
        } catch (AlreadyExistsException ignored) {
            // Existing namespace is the normal path after the first run.
        }
        if (catalog.tableExists(identifier)) {
            Table table;
            try {
                table = catalog.loadTable(identifier);
            } catch (NotFoundException e) {
                catalog.dropTable(identifier, false);
                return createTable(catalog, identifier, schema, spec);
            }
            Map<String, String> missingProperties = missingTableProperties(table.properties());
            if (!missingProperties.isEmpty()) {
                org.apache.iceberg.UpdateProperties update = table.updateProperties();
                missingProperties.forEach(update::set);
                update.commit();
                return catalog.loadTable(identifier);
            }
            return table;
        }
        return createTable(catalog, identifier, schema, spec);
    }

    private static Table createTable(HiveCatalog catalog, TableIdentifier identifier, Schema schema,
                                     PartitionSpec spec) {
        return catalog.createTable(identifier, schema, spec, tableProperties());
    }

    static HiveCatalog hiveCatalog(IcebergConfig config) {
        HiveCatalog catalog = new HiveCatalog();
        catalog.setConf(new Configuration());
        catalog.initialize(config.catalogName(), catalogProperties(config));
        return catalog;
    }

    static Map<String, String> catalogProperties(IcebergConfig config) {
        return Map.of(
            "warehouse", config.warehouse(),
            "uri", config.metastoreUri());
    }

    public static DataStreamSink<?> appendCellKpiSink(DataStream<RowData> stream, IcebergConfig config) {
        Schema schema = cellKpiSchema();
        return appendRowDataSink(stream, config, cellKpiIdentifier(config), schema, cellKpiPartitionSpec(schema));
    }

    static DataStreamSink<?> appendRowDataSink(DataStream<RowData> stream, IcebergConfig config,
                                               TableIdentifier identifier, Schema schema, PartitionSpec spec) {
        return appendRowDataSink(stream, config, identifier, schema, spec, null);
    }

    static DataStreamSink<?> appendRowDataSink(DataStream<RowData> stream, IcebergConfig config,
                                               TableIdentifier identifier, Schema schema, PartitionSpec spec,
                                               ConnectorSinkMetrics connectorMetrics) {
        ensureTable(config, identifier, schema, spec);
        CatalogLoader catalogLoader = CatalogLoader.hive(
            config.catalogName(), new Configuration(), catalogProperties(config));
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, identifier);
        IcebergSink.Builder builder = IcebergSink.forRowData(stream).tableLoader(tableLoader);
        if (connectorMetrics == null) {
            return builder.append();
        }
        return stream.sinkTo(new InstrumentedIcebergSink(buildIcebergSink(builder), connectorMetrics));
    }

    @SuppressWarnings("unchecked")
    static Sink<RowData> buildIcebergSink(IcebergSink.Builder builder) {
        try {
            Method build = builder.getClass().getDeclaredMethod("build");
            build.setAccessible(true);
            return (Sink<RowData>) build.invoke(builder);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalStateException("Iceberg connector sink instrumentation requires IcebergSink.Builder#build", e);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new IllegalStateException("Failed to build Iceberg connector sink for instrumentation", cause);
        }
    }

    public static void appendBusinessResultSinks(
        DataStream<CellKpi> kpi1m,
        DataStream<CellKpi> kpi5m,
        DataStream<AnomalyEvent> cellAnomalies,
        DataStream<AnomalyEvent> userAnomalies,
        DataStream<AnomalyEvent> gridAnomalies,
        IcebergConfig config,
        MetricRuntimeConfig metricConfig) {
        Schema kpiSchema = cellKpiSchema();
        PartitionSpec kpiSpec = cellKpiPartitionSpec(kpiSchema);
        Schema anomalySchema = anomalySchema();
        PartitionSpec anomalySpec = anomalyPartitionSpec(anomalySchema);

        String kpi1mStage = ResultSinks.kpiStageId(com.fdb.job.config.ResultSinkType.ICEBERG, "1m");
        String kpi5mStage = ResultSinks.kpiStageId(com.fdb.job.config.ResultSinkType.ICEBERG, "5m");
        String cellAnomalyStage = ResultSinks.anomalyStageId(com.fdb.job.config.ResultSinkType.ICEBERG, "cell");
        String userAnomalyStage = ResultSinks.anomalyStageId(com.fdb.job.config.ResultSinkType.ICEBERG, "user");
        String gridAnomalyStage = ResultSinks.anomalyStageId(com.fdb.job.config.ResultSinkType.ICEBERG, "grid");

        DataStream<RowData> icebergKpi1m = kpi1m
            .process(new SinkLatencyProbe<>(kpi1mStage, "Cell KPI 1m Iceberg Sink", "iceberg",
                "kpi_1m", "MIN_1", 100, metricConfig), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain()
            .name(kpi1mStage)
            .map(new CellKpiIcebergMapper())
            .returns(new GenericTypeInfo<>(RowData.class))
            .name("cell-kpi-1m-iceberg-map");
        appendRowDataSink(icebergKpi1m, config, cellKpiIdentifier(config), kpiSchema, kpiSpec,
            connectorMetrics(kpi1mStage, "Cell KPI 1m Iceberg Sink", "iceberg", "kpi_1m", "MIN_1",
                metricConfig))
            .name("cell-kpi-1m-iceberg-sink");

        DataStream<RowData> icebergKpi5m = kpi5m
            .process(new SinkLatencyProbe<>(kpi5mStage, "Cell KPI 5m Iceberg Sink", "iceberg",
                "kpi_5m", "MIN_5", 100, metricConfig), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain()
            .name(kpi5mStage)
            .map(new CellKpiIcebergMapper())
            .returns(new GenericTypeInfo<>(RowData.class))
            .name("cell-kpi-5m-iceberg-map");
        appendRowDataSink(icebergKpi5m, config, cellKpiIdentifier(config), kpiSchema, kpiSpec,
            connectorMetrics(kpi5mStage, "Cell KPI 5m Iceberg Sink", "iceberg", "kpi_5m", "MIN_5",
                metricConfig))
            .name("cell-kpi-5m-iceberg-sink");

        DataStream<RowData> icebergCellAnomalies = cellAnomalies
            .process(new SinkLatencyProbe<>(cellAnomalyStage, "Cell Anomaly Iceberg Sink", "iceberg",
                "cell_anomaly_events", "ANOMALY", 100, metricConfig), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name(cellAnomalyStage)
            .map(new AnomalyEventIcebergMapper())
            .returns(new GenericTypeInfo<>(RowData.class))
            .name("cell-anomaly-iceberg-map");
        appendRowDataSink(icebergCellAnomalies, config, cellAnomalyIdentifier(config), anomalySchema, anomalySpec,
            connectorMetrics(cellAnomalyStage, "Cell Anomaly Iceberg Sink", "iceberg",
                "cell_anomaly_events", "ANOMALY", metricConfig))
            .name("cell-anomaly-iceberg-sink");

        DataStream<RowData> icebergUserAnomalies = userAnomalies
            .process(new SinkLatencyProbe<>(userAnomalyStage, "User Anomaly Iceberg Sink", "iceberg",
                "user_anomaly_events", "ANOMALY", 100, metricConfig), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name(userAnomalyStage)
            .map(new AnomalyEventIcebergMapper())
            .returns(new GenericTypeInfo<>(RowData.class))
            .name("user-anomaly-iceberg-map");
        appendRowDataSink(icebergUserAnomalies, config, userAnomalyIdentifier(config), anomalySchema, anomalySpec,
            connectorMetrics(userAnomalyStage, "User Anomaly Iceberg Sink", "iceberg",
                "user_anomaly_events", "ANOMALY", metricConfig))
            .name("user-anomaly-iceberg-sink");

        DataStream<RowData> icebergGridAnomalies = gridAnomalies
            .process(new SinkLatencyProbe<>(gridAnomalyStage, "Grid Anomaly Iceberg Sink", "iceberg",
                "grid_anomaly_events", "ANOMALY", 100, metricConfig), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name(gridAnomalyStage)
            .map(new AnomalyEventIcebergMapper())
            .returns(new GenericTypeInfo<>(RowData.class))
            .name("grid-anomaly-iceberg-map");
        appendRowDataSink(icebergGridAnomalies, config, gridAnomalyIdentifier(config), anomalySchema, anomalySpec,
            connectorMetrics(gridAnomalyStage, "Grid Anomaly Iceberg Sink", "iceberg",
                "grid_anomaly_events", "ANOMALY", metricConfig))
            .name("grid-anomaly-iceberg-sink");
    }

    private static ConnectorSinkMetrics connectorMetrics(String stageId, String displayName, String sinkType,
                                                         String dataset, String windowKind,
                                                         MetricRuntimeConfig metricConfig) {
        return new ConnectorSinkMetrics(stageId, displayName, sinkType, dataset, windowKind, 100, metricConfig);
    }
}
