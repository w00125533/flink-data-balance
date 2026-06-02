package com.fdb.job;

import com.fdb.common.avro.CellKpi;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import java.nio.file.Paths;

public final class HiveSinks {

    static final String KPI_BUCKET_FORMAT = "'dt='yyyy-MM-dd'/hour='HH";
    static final OutputFileConfig PARQUET_OUTPUT_FILE_CONFIG = OutputFileConfig
        .builder()
        .withPartSuffix(".parquet")
        .build();

    private HiveSinks() {}

    private static String warehousePath() {
        return System.getenv().getOrDefault("FDB_HIVE_WAREHOUSE",
            Paths.get("docker", "data", "warehouse").toAbsolutePath().toUri().toString());
    }

    public static FileSink<CellKpi> cellKpiSink(String windowKind) {
        String outputPath = warehousePath() + "/cell_kpi/window_kind=" + windowKind;

        return FileSink
            .forBulkFormat(new Path(outputPath), AvroParquetWriters.forSpecificRecord(CellKpi.class))
            .withBucketAssigner(new DateTimeBucketAssigner<>(KPI_BUCKET_FORMAT))
            .withOutputFileConfig(PARQUET_OUTPUT_FILE_CONFIG)
            .build();
    }
}
