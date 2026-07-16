package com.fdb.job.sink;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.CellKpi;
import com.fdb.job.config.ResultSinkConfig;
import com.fdb.job.config.ResultSinkType;
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
        cellKpi1m
            .process(new SinkLatencyProbe<>("starrocks-kpi-1m", "Cell KPI 1m StarRocks Sink", "starrocks",
                "kpi_1m", "MIN_1", 100, metricConfig), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain()
            .name("starrocks-kpi-1m")
            .addSink(StarRocksSinks.cellKpiSink("kpi-1m"))
            .name("cell-kpi-starrocks-connector-sink");
        cellKpi5m
            .process(new SinkLatencyProbe<>("starrocks-kpi-5m", "Cell KPI 5m StarRocks Sink", "starrocks",
                "kpi_5m", "MIN_5", 100, metricConfig), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain()
            .name("starrocks-kpi-5m")
            .addSink(StarRocksSinks.cellKpiSink("kpi-5m"))
            .name("cell-kpi-5m-starrocks-connector-sink");
        cellAnomalies
            .process(new SinkLatencyProbe<>("starrocks-cell-anomaly", "Cell Anomaly StarRocks Sink", "starrocks",
                "cell_anomaly_events", "ANOMALY", 100, metricConfig), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name("starrocks-cell-anomaly")
            .addSink(StarRocksSinks.cellAnomalySink())
            .name("cell-anomaly-starrocks-connector-sink");
        userAnomalies
            .process(new SinkLatencyProbe<>("starrocks-user-anomaly", "User Anomaly StarRocks Sink", "starrocks",
                "user_anomaly_events", "ANOMALY", 100, metricConfig), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name("starrocks-user-anomaly")
            .addSink(StarRocksSinks.userAnomalySink())
            .name("user-anomaly-starrocks-connector-sink");
        gridAnomalies
            .process(new SinkLatencyProbe<>("starrocks-grid-anomaly", "Grid Anomaly StarRocks Sink", "starrocks",
                "grid_anomaly_events", "ANOMALY", 100, metricConfig), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name("starrocks-grid-anomaly")
            .addSink(StarRocksSinks.gridAnomalySink())
            .name("grid-anomaly-starrocks-connector-sink");
    }

    private static void attachHiveSinks(
        DataStream<CellKpi> cellKpi1m,
        DataStream<CellKpi> cellKpi5m,
        DataStream<AnomalyEvent> cellAnomalies,
        DataStream<AnomalyEvent> userAnomalies,
        DataStream<AnomalyEvent> gridAnomalies,
        MetricRuntimeConfig metricConfig) {
        cellKpi1m
            .process(new SinkLatencyProbe<>("hive-kpi-1m", "Cell KPI 1m Hive Sink", "hive",
                "kpi_1m", "MIN_1", 100, metricConfig), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain()
            .name("hive-kpi-1m")
            .sinkTo(HiveSinks.cellKpiSink("MIN_1"))
            .name("cell-kpi-hive-sink");
        cellKpi5m
            .process(new SinkLatencyProbe<>("hive-kpi-5m", "Cell KPI 5m Hive Sink", "hive",
                "kpi_5m", "MIN_5", 100, metricConfig), new GenericTypeInfo<>(CellKpi.class))
            .startNewChain()
            .name("hive-kpi-5m")
            .sinkTo(HiveSinks.cellKpiSink("MIN_5"))
            .name("cell-kpi-5m-hive-sink");
        cellAnomalies
            .process(new SinkLatencyProbe<>("hive-cell-anomaly", "Cell Anomaly Hive Sink", "hive",
                "cell_anomaly_events", "ANOMALY", 100, metricConfig), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name("hive-cell-anomaly")
            .sinkTo(HiveSinks.cellAnomalySink())
            .name("cell-anomaly-hive-sink");
        userAnomalies
            .process(new SinkLatencyProbe<>("hive-user-anomaly", "User Anomaly Hive Sink", "hive",
                "user_anomaly_events", "ANOMALY", 100, metricConfig), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name("hive-user-anomaly")
            .sinkTo(HiveSinks.userAnomalySink())
            .name("user-anomaly-hive-sink");
        gridAnomalies
            .process(new SinkLatencyProbe<>("hive-grid-anomaly", "Grid Anomaly Hive Sink", "hive",
                "grid_anomaly_events", "ANOMALY", 100, metricConfig), new GenericTypeInfo<>(AnomalyEvent.class))
            .startNewChain()
            .name("hive-grid-anomaly")
            .sinkTo(HiveSinks.gridAnomalySink())
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
}
