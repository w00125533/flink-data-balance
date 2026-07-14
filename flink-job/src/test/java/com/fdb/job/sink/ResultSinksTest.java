package com.fdb.job.sink;

import com.fdb.job.config.ResultSinkConfig;
import com.fdb.job.config.ResultSinkType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ResultSinksTest {

    @Test
    void builds_business_stage_ids_by_sink_type_and_result_kind() {
        assertThat(ResultSinks.kpiStageId(ResultSinkType.STARROCKS, "1m"))
            .isEqualTo("starrocks-kpi-1m");
        assertThat(ResultSinks.kpiStageId(ResultSinkType.ICEBERG, "5m"))
            .isEqualTo("iceberg-kpi-5m");
        assertThat(ResultSinks.anomalyStageId(ResultSinkType.HIVE, "cell"))
            .isEqualTo("hive-cell-anomaly");
        assertThat(ResultSinks.anomalyStageId(ResultSinkType.KAFKA, "grid"))
            .isEqualTo("kafka-grid-anomaly");
    }

    @Test
    void lists_no_business_stages_for_none_sink() {
        assertThat(ResultSinks.businessStageIds(ResultSinkType.NONE)).isEmpty();
    }

    @Test
    void lists_all_business_stages_for_selected_sink() {
        assertThat(ResultSinks.businessStageIds(ResultSinkType.STARROCKS))
            .containsExactly(
                "starrocks-kpi-1m",
                "starrocks-kpi-5m",
                "starrocks-cell-anomaly",
                "starrocks-grid-anomaly");
    }

    @Test
    void fails_fast_when_iceberg_sink_is_selected_but_iceberg_is_disabled() {
        ResultSinkConfig config = new ResultSinkConfig(
            ResultSinkType.ICEBERG, true, true, true, 5_000L, false, "run-a", "");
        IcebergConfig icebergConfig = new IcebergConfig(
            false,
            "hdfs://namenode:8020/warehouse/iceberg",
            "fdb_iceberg",
            "iceberg_db",
            "cell_kpi",
            "thrift://hive-metastore:9083");

        assertThatThrownBy(() -> ResultSinks.attachBusinessResultSinks(
            null, null, null, null, config, "localhost:9092", icebergConfig))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("FDB_RESULT_SINK=iceberg")
            .hasMessageContaining("FDB_ICEBERG_ENABLED=false");
    }
}
