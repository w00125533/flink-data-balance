package com.fdb.job.sink;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static org.assertj.core.api.Assertions.assertThat;

class HiveSinksTest {

    @Test
    void kpi_bucket_format_treats_partition_names_as_literals() {
        DateTimeFormatter formatter = DateTimeFormatter
            .ofPattern(HiveSinks.KPI_BUCKET_FORMAT)
            .withZone(ZoneId.of("UTC"));

        assertThat(formatter.format(Instant.parse("2026-06-02T07:00:00Z")))
            .isEqualTo("dt=2026-06-02/hour=07");
    }

    @Test
    void builds_anomaly_output_paths_under_warehouse() {
        String warehouse = "hdfs://namenode:8020/warehouse/fdb";

        assertThat(HiveSinks.cellAnomalyOutputPath(warehouse))
            .isEqualTo("hdfs://namenode:8020/warehouse/fdb/cell_anomaly_events");
        assertThat(HiveSinks.gridAnomalyOutputPath(warehouse))
            .isEqualTo("hdfs://namenode:8020/warehouse/fdb/grid_anomaly_events");
    }
}
