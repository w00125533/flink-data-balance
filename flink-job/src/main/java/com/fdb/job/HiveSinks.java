package com.fdb.job;

import com.fdb.common.avro.CellKpi;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;

public final class HiveSinks {

    private HiveSinks() {}

    private static final String DEFAULT_WAREHOUSE = "/user/hive/warehouse/fdb.db";

    private static String warehousePath() {
        return System.getenv().getOrDefault("FDB_HIVE_WAREHOUSE", DEFAULT_WAREHOUSE);
    }

    public static FileSink<CellKpi> cellKpi1mSink() {
        String outputPath = warehousePath() + "/cell_kpi_1m";

        return FileSink
            .forBulkFormat(new Path(outputPath), AvroParquetWriters.forSpecificRecord(CellKpi.class))
            .withBucketAssigner(new DateTimeBucketAssigner<>("dt=yyyy-MM-dd/hour=HH"))
            .build();
    }
}
