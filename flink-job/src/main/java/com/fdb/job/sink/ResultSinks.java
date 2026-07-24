package com.fdb.job.sink;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.CellKpi;
import com.fdb.job.config.ResultSinkConfig;
import com.fdb.job.config.ResultSinkType;
import com.fdb.job.metrics.ConnectorSinkMetrics;
import com.fdb.job.metrics.InstrumentedFileSink;
import com.fdb.job.metrics.InstrumentedSinkFunction;
import com.fdb.job.metrics.MetricRuntimeConfig;
import com.fdb.job.metrics.SinkLatencyProbe;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

public final class ResultSinks {

    private ResultSinks() {}

    public static List<String> businessStageIds(ResultSinkType sinkType) {
        if (sinkType == ResultSinkType.NONE) {
            return List.of();
        }
        return List.of(
            kpiStageId(sinkType, "1m"),
            kpiStageId(sinkType, "5m"),
            anomalyStageId(sinkType, "cell"),
            anomalyStageId(sinkType, "user"),
            anomalyStageId(sinkType, "grid"));
    }

    public static String kpiStageId(ResultSinkType sinkType, String window) {
        return sinkType.configValue() + "-kpi-" + window;
    }

    public static String anomalyStageId(ResultSinkType sinkType, String anomalyScope) {
        return sinkType.configValue() + "-" + anomalyScope + "-anomaly";
    }

    public static void attachBusinessResultSinks(
        DataStream<CellKpi> cellKpi1m,
        DataStream<CellKpi> cellKpi5m,
        DataStream<AnomalyEvent> cellAnomalies,
        DataStream<AnomalyEvent> userAnomalies,
        DataStream<AnomalyEvent> gridAnomalies,
        ResultSinkConfig config,
        String bootstrap,
        IcebergConfig icebergConfig,
        MetricRuntimeConfig metricConfig) {
        switch (config.resultSink()) {
            case STARROCKS -> attachStarRocksSinks(cellKpi1m, cellKpi5m, cellAnomalies, userAnomalies, gridAnomalies,
                metricConfig);
            case ICEBERG -> {
                ensureIcebergEnabled(icebergConfig);
                IcebergSinks.appendBusinessResultSinks(
                    cellKpi1m, cellKpi5m, cellAnomalies, userAnomalies, gridAnomalies, icebergConfig, metricConfig);
            }
            case HIVE -> attachHiveSinks(cellKpi1m, cellKpi5m, cellAnomalies, userAnomalies, gridAnomalies,
                metricConfig);
            case KAFKA -> attachKafkaSinks(cellKpi1m, cellKpi5m, cellAnomalies, userAnomalies, gridAnomalies, bootstrap,
                metricConfig);
            case NONE -> {
            }
        }
    }

    private static void ensureIcebergEnabled(IcebergConfig icebergConfig) {
        if (!icebergConfig.enabled()) {
            throw new IllegalStateException(
                "Conflicting result sink configuration: FDB_RESULT_SINK=iceberg requires "
                    + "FDB_ICEBERG_ENABLED=true, but FDB_ICEBERG_ENABLED=false");
        }
    }

    private static void attachStarRocksSinks(
        DataStream<CellKpi> cellKpi1m,
        DataStream<CellKpi> cellKpi5m,
        DataStream<AnomalyEvent> cellAnomalies,
        DataStream<AnomalyEvent> userAnomalies,
        DataStream<AnomalyEvent> gridAnomalies,
        MetricRuntimeConfig metricConfig) {
        String kpi1mStage = kpiStageId(ResultSinkType.STARROCKS, "1m");
        String kpi5mStage = kpiStageId(ResultSinkType.STARROCKS, "5m");
        String cellAnomalyStage = anomalyStageId(ResultSinkType.STARROCKS, "cell");
        String userAnomalyStage = anomalyStageId(ResultSinkType.STARROCKS, "user");
        String gridAnomalyStage = anomalyStageId(ResultSinkType.STARROCKS, "grid");
        cellKpi1m
            .process(new SinkLatencyProbe<>(kpi1mStage, "Cell KPI 1m StarRocks Sink", "starrocks",
                "kpi_1m", "MIN_1", 100, metricConfig), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain()
            .name(kpi1mStage)
            .addSink(new InstrumentedSinkFunction<>(
                StarRocksSinks.cellKpiSink("kpi-1m"),
                connectorMetrics(kpi1mStage, "Cell KPI 1m StarRocks Sink", "starrocks",
                    "kpi_1m", "MIN_1", metricConfig)))
            .name("cell-kpi-starrocks-connector-sink");
        cellKpi5m
            .process(new SinkLatencyProbe<>(kpi5mStage, "Cell KPI 5m StarRocks Sink", "starrocks",
                "kpi_5m", "MIN_5", 100, metricConfig), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain()
            .name(kpi5mStage)
            .addSink(new InstrumentedSinkFunction<>(
                StarRocksSinks.cellKpiSink("kpi-5m"),
                connectorMetrics(kpi5mStage, "Cell KPI 5m StarRocks Sink", "starrocks",
                    "kpi_5m", "MIN_5", metricConfig)))
            .name("cell-kpi-5m-starrocks-connector-sink");
        cellAnomalies
            .process(new SinkLatencyProbe<>(cellAnomalyStage, "Cell Anomaly StarRocks Sink", "starrocks",
                "cell_anomaly_events", "ANOMALY", 100, metricConfig), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name(cellAnomalyStage)
            .addSink(new InstrumentedSinkFunction<>(
                StarRocksSinks.cellAnomalySink(),
                connectorMetrics(cellAnomalyStage, "Cell Anomaly StarRocks Sink", "starrocks",
                    "cell_anomaly_events", "ANOMALY", metricConfig)))
            .name("cell-anomaly-starrocks-connector-sink");
        userAnomalies
            .process(new SinkLatencyProbe<>(userAnomalyStage, "User Anomaly StarRocks Sink", "starrocks",
                "user_anomaly_events", "ANOMALY", 100, metricConfig), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name(userAnomalyStage)
            .addSink(new InstrumentedSinkFunction<>(
                StarRocksSinks.userAnomalySink(),
                connectorMetrics(userAnomalyStage, "User Anomaly StarRocks Sink", "starrocks",
                    "user_anomaly_events", "ANOMALY", metricConfig)))
            .name("user-anomaly-starrocks-connector-sink");
        gridAnomalies
            .process(new SinkLatencyProbe<>(gridAnomalyStage, "Grid Anomaly StarRocks Sink", "starrocks",
                "grid_anomaly_events", "ANOMALY", 100, metricConfig), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name(gridAnomalyStage)
            .addSink(new InstrumentedSinkFunction<>(
                StarRocksSinks.gridAnomalySink(),
                connectorMetrics(gridAnomalyStage, "Grid Anomaly StarRocks Sink", "starrocks",
                    "grid_anomaly_events", "ANOMALY", metricConfig)))
            .name("grid-anomaly-starrocks-connector-sink");
    }

    private static void attachHiveSinks(
        DataStream<CellKpi> cellKpi1m,
        DataStream<CellKpi> cellKpi5m,
        DataStream<AnomalyEvent> cellAnomalies,
        DataStream<AnomalyEvent> userAnomalies,
        DataStream<AnomalyEvent> gridAnomalies,
        MetricRuntimeConfig metricConfig) {
        String kpi1mStage = kpiStageId(ResultSinkType.HIVE, "1m");
        String kpi5mStage = kpiStageId(ResultSinkType.HIVE, "5m");
        String cellAnomalyStage = anomalyStageId(ResultSinkType.HIVE, "cell");
        String userAnomalyStage = anomalyStageId(ResultSinkType.HIVE, "user");
        String gridAnomalyStage = anomalyStageId(ResultSinkType.HIVE, "grid");
        cellKpi1m
            .process(new SinkLatencyProbe<>(kpi1mStage, "Cell KPI 1m Hive Sink", "hive",
                "kpi_1m", "MIN_1", 100, metricConfig), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain()
            .name(kpi1mStage)
            .sinkTo(new InstrumentedFileSink<>(
                HiveSinks.cellKpiSink("MIN_1"),
                connectorMetrics(kpi1mStage, "Cell KPI 1m Hive Sink", "hive",
                    "kpi_1m", "MIN_1", metricConfig)))
            .name("cell-kpi-hive-sink");
        cellKpi5m
            .process(new SinkLatencyProbe<>(kpi5mStage, "Cell KPI 5m Hive Sink", "hive",
                "kpi_5m", "MIN_5", 100, metricConfig), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain()
            .name(kpi5mStage)
            .sinkTo(new InstrumentedFileSink<>(
                HiveSinks.cellKpiSink("MIN_5"),
                connectorMetrics(kpi5mStage, "Cell KPI 5m Hive Sink", "hive",
                    "kpi_5m", "MIN_5", metricConfig)))
            .name("cell-kpi-5m-hive-sink");
        cellAnomalies
            .process(new SinkLatencyProbe<>(cellAnomalyStage, "Cell Anomaly Hive Sink", "hive",
                "cell_anomaly_events", "ANOMALY", 100, metricConfig), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name(cellAnomalyStage)
            .sinkTo(new InstrumentedFileSink<>(
                HiveSinks.cellAnomalySink(),
                connectorMetrics(cellAnomalyStage, "Cell Anomaly Hive Sink", "hive",
                    "cell_anomaly_events", "ANOMALY", metricConfig)))
            .name("cell-anomaly-hive-sink");
        userAnomalies
            .process(new SinkLatencyProbe<>(userAnomalyStage, "User Anomaly Hive Sink", "hive",
                "user_anomaly_events", "ANOMALY", 100, metricConfig), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name(userAnomalyStage)
            .sinkTo(new InstrumentedFileSink<>(
                HiveSinks.userAnomalySink(),
                connectorMetrics(userAnomalyStage, "User Anomaly Hive Sink", "hive",
                    "user_anomaly_events", "ANOMALY", metricConfig)))
            .name("user-anomaly-hive-sink");
        gridAnomalies
            .process(new SinkLatencyProbe<>(gridAnomalyStage, "Grid Anomaly Hive Sink", "hive",
                "grid_anomaly_events", "ANOMALY", 100, metricConfig), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name(gridAnomalyStage)
            .sinkTo(new InstrumentedFileSink<>(
                HiveSinks.gridAnomalySink(),
                connectorMetrics(gridAnomalyStage, "Grid Anomaly Hive Sink", "hive",
                    "grid_anomaly_events", "ANOMALY", metricConfig)))
            .name("grid-anomaly-hive-sink");
    }

    private static void attachKafkaSinks(
        DataStream<CellKpi> cellKpi1m,
        DataStream<CellKpi> cellKpi5m,
        DataStream<AnomalyEvent> cellAnomalies,
        DataStream<AnomalyEvent> userAnomalies,
        DataStream<AnomalyEvent> gridAnomalies,
        String bootstrap,
        MetricRuntimeConfig metricConfig) {
        cellKpi1m
            .process(new SinkLatencyProbe<>("kafka-kpi-1m", "Cell KPI 1m Kafka Sink", "kafka",
                "kpi_1m", "MIN_1", 100, metricConfig), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain()
            .name("kafka-kpi-1m")
            .sinkTo(KafkaResultSinks.cellKpiSink(bootstrap, "cell-kpi-1m"))
            .name("cell-kpi-kafka-sink");
        cellKpi5m
            .process(new SinkLatencyProbe<>("kafka-kpi-5m", "Cell KPI 5m Kafka Sink", "kafka",
                "kpi_5m", "MIN_5", 100, metricConfig), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain()
            .name("kafka-kpi-5m")
            .sinkTo(KafkaResultSinks.cellKpiSink(bootstrap, "cell-kpi-5m"))
            .name("cell-kpi-5m-kafka-sink");
        cellAnomalies
            .process(new SinkLatencyProbe<>("kafka-cell-anomaly", "Cell Anomaly Kafka Sink", "kafka",
                "cell_anomaly_events", "ANOMALY", 100, metricConfig), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name("kafka-cell-anomaly")
            .sinkTo(KafkaResultSinks.anomalySink(bootstrap, "cell-anomaly-events"))
            .name("cell-anomaly-kafka-sink");
        userAnomalies
            .process(new SinkLatencyProbe<>("kafka-user-anomaly", "User Anomaly Kafka Sink", "kafka",
                "user_anomaly_events", "ANOMALY", 100, metricConfig), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name("kafka-user-anomaly")
            .sinkTo(KafkaResultSinks.anomalySink(bootstrap, "user-anomaly-events"))
            .name("user-anomaly-kafka-sink");
        gridAnomalies
            .process(new SinkLatencyProbe<>("kafka-grid-anomaly", "Grid Anomaly Kafka Sink", "kafka",
                "grid_anomaly_events", "ANOMALY", 100, metricConfig), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name("kafka-grid-anomaly")
            .sinkTo(KafkaResultSinks.anomalySink(bootstrap, "grid-anomaly-events"))
            .name("grid-anomaly-kafka-sink");
    }

    private static ConnectorSinkMetrics connectorMetrics(String stageId, String displayName, String sinkType,
                                                         String dataset, String windowKind,
                                                         MetricRuntimeConfig metricConfig) {
        return new ConnectorSinkMetrics(stageId, displayName, sinkType, dataset, windowKind, 100, metricConfig);
    }
}
