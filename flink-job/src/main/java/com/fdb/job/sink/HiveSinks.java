package com.fdb.job.sink;

import com.fdb.common.avro.AnomalyEvent;
import com.fdb.common.avro.CellKpi;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;

public final class HiveSinks {

    static final String KPI_BUCKET_FORMAT = "'dt='yyyy-MM-dd'/hour='HH";
    static final OutputFileConfig PARQUET_OUTPUT_FILE_CONFIG = OutputFileConfig
        .builder()
        .withPartSuffix(".parquet")
        .build();

    private HiveSinks() {}

    private static String warehousePath() {
        return System.getenv().getOrDefault("FDB_HIVE_WAREHOUSE",
            "hdfs://namenode:8020/warehouse/fdb");
    }

    public static FileSink<CellKpi> cellKpiSink(String windowKind) {
        String outputPath = warehousePath() + "/cell_kpi/window_kind=" + windowKind;

        return FileSink
            .forBulkFormat(new Path(outputPath), AvroParquetWriters.forSpecificRecord(CellKpi.class))
            .withBucketAssigner(new DateTimeBucketAssigner<>(KPI_BUCKET_FORMAT))
            .withOutputFileConfig(PARQUET_OUTPUT_FILE_CONFIG)
            .build();
    }

    static String cellAnomalyOutputPath(String warehousePath) {
        return warehousePath + "/cell_anomaly_events";
    }

    static String userAnomalyOutputPath(String warehousePath) {
        return warehousePath + "/user_anomaly_events";
    }

    static String gridAnomalyOutputPath(String warehousePath) {
        return warehousePath + "/grid_anomaly_events";
    }

    public static FileSink<AnomalyEvent> cellAnomalySink() {
        return anomalySink(cellAnomalyOutputPath(warehousePath()));
    }

    public static FileSink<AnomalyEvent> userAnomalySink() {
        return anomalySink(userAnomalyOutputPath(warehousePath()));
    }

    public static FileSink<AnomalyEvent> gridAnomalySink() {
        return anomalySink(gridAnomalyOutputPath(warehousePath()));
    }

    private static FileSink<AnomalyEvent> anomalySink(String outputPath) {
        return FileSink
            .forBulkFormat(new Path(outputPath), AvroParquetWriters.forSpecificRecord(AnomalyEvent.class))
            .withBucketAssigner(new DateTimeBucketAssigner<>(KPI_BUCKET_FORMAT))
            .withOutputFileConfig(PARQUET_OUTPUT_FILE_CONFIG)
            .build();
    }
}
