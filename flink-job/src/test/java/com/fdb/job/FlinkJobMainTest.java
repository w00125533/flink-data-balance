package com.fdb.job;

import com.fdb.job.config.ResultSinkConfig;
import com.fdb.job.config.ResultSinkType;
import com.fdb.job.metrics.MetricRuntimeConfig;
import com.fdb.job.metrics.StageMetricsProbe;
import com.fdb.job.model.InputEnvelope;
import com.fdb.job.model.RoutedEnvelope;
import com.fdb.job.sink.IcebergConfig;
import com.fdb.common.avro.PmStat;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlinkJobMainTest {

    @Test
    void resolve_parallelism_defaults_to_four_slots() {
        assertThat(FlinkJobMain.resolveParallelism(Map.of(), new Properties())).isEqualTo(4);
    }

    @Test
    void resolve_parallelism_prefers_environment() {
        Properties properties = new Properties();
        properties.setProperty("fdb.flink.parallelism", "2");

        assertThat(FlinkJobMain.resolveParallelism(
            Map.of("FDB_FLINK_PARALLELISM", "4"), properties))
            .isEqualTo(4);
    }

    @Test
    void resolve_parallelism_uses_property_when_environment_missing() {
        Properties properties = new Properties();
        properties.setProperty("fdb.flink.parallelism", "3");

        assertThat(FlinkJobMain.resolveParallelism(Map.of(), properties)).isEqualTo(3);
    }

    @Test
    void resolve_parallelism_falls_back_for_invalid_values() {
        assertThat(FlinkJobMain.resolveParallelism(
            Map.of("FDB_FLINK_PARALLELISM", "nope"), new Properties()))
            .isEqualTo(4);
        assertThat(FlinkJobMain.resolveParallelism(
            Map.of("FDB_FLINK_PARALLELISM", "0"), new Properties()))
            .isEqualTo(4);
        assertThat(FlinkJobMain.resolveParallelism(
            Map.of("FDB_FLINK_PARALLELISM", "-2"), new Properties()))
            .isEqualTo(4);
    }

    @Test
    void resolve_checkpoint_interval_defaults_to_thirty_seconds() {
        assertThat(FlinkJobMain.resolveCheckpointIntervalMs(Map.of(), new Properties()))
            .isEqualTo(30_000L);
    }

    @Test
    void resolve_checkpoint_interval_prefers_environment() {
        Properties properties = new Properties();
        properties.setProperty("fdb.flink.checkpoint.interval.ms", "60000");

        assertThat(FlinkJobMain.resolveCheckpointIntervalMs(
            Map.of("FDB_FLINK_CHECKPOINT_INTERVAL_MS", "10000"), properties))
            .isEqualTo(10_000L);
    }

    @Test
    void resolve_checkpoint_interval_falls_back_for_invalid_values() {
        assertThat(FlinkJobMain.resolveCheckpointIntervalMs(
            Map.of("FDB_FLINK_CHECKPOINT_INTERVAL_MS", "nope"), new Properties()))
            .isEqualTo(30_000L);
        assertThat(FlinkJobMain.resolveCheckpointIntervalMs(
            Map.of("FDB_FLINK_CHECKPOINT_INTERVAL_MS", "0"), new Properties()))
            .isEqualTo(30_000L);
    }

    @Test
    void resolve_chr_and_pm_watermark_out_of_orderness_defaults_to_two_seconds() {
        assertThat(FlinkJobMain.resolveChrWatermarkOutOfOrderness(Map.of(), new Properties()))
            .isEqualTo(Duration.ofSeconds(2));
        assertThat(FlinkJobMain.resolvePmWatermarkOutOfOrderness(Map.of(), new Properties()))
            .isEqualTo(Duration.ofSeconds(2));
    }

    @Test
    void resolve_watermark_out_of_orderness_prefers_environment_and_falls_back_for_invalid_values() {
        Properties properties = new Properties();
        properties.setProperty("fdb.chr.watermark.out.of.order.ms", "1500");
        properties.setProperty("fdb.pm.watermark.out.of.order.ms", "1600");

        assertThat(FlinkJobMain.resolveChrWatermarkOutOfOrderness(
            Map.of("FDB_CHR_WATERMARK_OUT_OF_ORDER_MS", "1000"), properties))
            .isEqualTo(Duration.ofSeconds(1));
        assertThat(FlinkJobMain.resolvePmWatermarkOutOfOrderness(
            Map.of("FDB_PM_WATERMARK_OUT_OF_ORDER_MS", "nope"), properties))
            .isEqualTo(Duration.ofSeconds(2));
    }

    @Test
    void resolve_kpi_join_wait_defaults_to_ten_seconds() {
        assertThat(FlinkJobMain.resolveKpiJoinWait(Map.of(), new Properties()))
            .isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void resolve_kpi_join_wait_prefers_environment_and_falls_back_for_invalid_values() {
        Properties properties = new Properties();
        properties.setProperty("fdb.kpi.join.wait.ms", "20000");

        assertThat(FlinkJobMain.resolveKpiJoinWait(
            Map.of("FDB_KPI_JOIN_WAIT_MS", "15000"), properties))
            .isEqualTo(Duration.ofSeconds(15));
        assertThat(FlinkJobMain.resolveKpiJoinWait(
            Map.of("FDB_KPI_JOIN_WAIT_MS", "-1"), properties))
            .isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void effective_checkpoint_interval_keeps_starrocks_interval() {
        assertThat(FlinkJobMain.effectiveCheckpointIntervalMs(
            ResultSinkType.STARROCKS, 240_000L))
            .isEqualTo(240_000L);
    }

    @Test
    void effective_checkpoint_interval_caps_selected_file_sinks() {
        assertThat(FlinkJobMain.effectiveCheckpointIntervalMs(
            ResultSinkType.HIVE, 240_000L))
            .isEqualTo(180_000L);
        assertThat(FlinkJobMain.effectiveCheckpointIntervalMs(
            ResultSinkType.ICEBERG, 240_000L))
            .isEqualTo(180_000L);
    }

    @Test
    void resolve_checkpoint_storage_defaults_to_filesystem_path() {
        assertThat(FlinkJobMain.resolveCheckpointStorage(Map.of(), new Properties()))
            .isEqualTo("file:///tmp/fdb-checkpoints");
    }

    @Test
    void resolve_checkpoint_storage_prefers_environment() {
        Properties properties = new Properties();
        properties.setProperty("fdb.flink.checkpoint.dir", "file:///property-checkpoints");

        assertThat(FlinkJobMain.resolveCheckpointStorage(
            Map.of("FDB_FLINK_CHECKPOINT_DIR", " file:///env-checkpoints "), properties))
            .isEqualTo("file:///env-checkpoints");
    }

    @Test
    void resolve_iceberg_config_uses_defaults() {
        IcebergConfig config = FlinkJobMain.resolveIcebergConfig(Map.of(), new Properties());

        assertThat(config.enabled()).isTrue();
        assertThat(config.warehouse()).isEqualTo("hdfs://namenode:8020/warehouse/iceberg");
        assertThat(config.database()).isEqualTo("iceberg_db");
    }

    @Test
    void resolve_iceberg_config_can_disable_sink() {
        IcebergConfig config = FlinkJobMain.resolveIcebergConfig(
            Map.of("FDB_ICEBERG_ENABLED", "false"), new Properties());

        assertThat(config.enabled()).isFalse();
    }

    @Test
    void resolve_dynamic_balancing_defaults_to_disabled() {
        assertThat(FlinkJobMain.resolveDynamicBalancingEnabled(Map.of(), new Properties())).isFalse();
    }

    @Test
    void resolve_dynamic_balancing_prefers_environment() {
        Properties properties = new Properties();
        properties.setProperty("fdb.dynamic.balancing.enabled", "false");

        assertThat(FlinkJobMain.resolveDynamicBalancingEnabled(
            Map.of("FDB_DYNAMIC_BALANCING_ENABLED", "true"), properties))
            .isTrue();
    }

    @Test
    void resolve_dynamic_balancing_uses_property_when_environment_missing() {
        Properties properties = new Properties();
        properties.setProperty("fdb.dynamic.balancing.enabled", "true");

        assertThat(FlinkJobMain.resolveDynamicBalancingEnabled(Map.of(), properties)).isTrue();
    }

    @Test
    void resolve_kafka_consumer_properties_prefers_environment() {
        Properties properties = new Properties();
        properties.setProperty("fdb.kafka.fetch.max.bytes", "8388608");
        properties.setProperty("fdb.kafka.max.partition.fetch.bytes", "2097152");
        properties.setProperty("fdb.kafka.max.poll.records", "1000");

        Properties consumerProperties = FlinkJobMain.resolveKafkaConsumerProperties(Map.of(
            "FDB_KAFKA_FETCH_MAX_BYTES", "4194304",
            "FDB_KAFKA_MAX_PARTITION_FETCH_BYTES", "1048576",
            "FDB_KAFKA_MAX_POLL_RECORDS", "500"), properties);

        assertThat(consumerProperties)
            .containsEntry("fetch.max.bytes", "4194304")
            .containsEntry("max.partition.fetch.bytes", "1048576")
            .containsEntry("max.poll.records", "500");
    }

    @Test
    void resolve_kafka_consumer_properties_ignores_blank_and_invalid_values() {
        Properties consumerProperties = FlinkJobMain.resolveKafkaConsumerProperties(Map.of(
            "FDB_KAFKA_FETCH_MAX_BYTES", " ",
            "FDB_KAFKA_MAX_PARTITION_FETCH_BYTES", "nope",
            "FDB_KAFKA_MAX_POLL_RECORDS", "0"), new Properties());

        assertThat(consumerProperties).isEmpty();
    }

    @Test
    void stage_metrics_probe_uses_configured_emit_interval() throws Exception {
        ResultSinkConfig resultSinkConfig = new ResultSinkConfig(
            ResultSinkType.STARROCKS, true, true, true, 7_000L, false, "run-a", "");
        MetricRuntimeConfig metricConfig = new MetricRuntimeConfig("run-a", "starrocks", 4, true);

        StageMetricsProbe<Object> probe = FlinkJobMain.stageMetricsProbe(
            "stage-a", "Stage A", "healthy", resultSinkConfig, metricConfig);

        Field emitInterval = StageMetricsProbe.class.getDeclaredField("emitIntervalMs");
        emitInterval.setAccessible(true);
        assertThat(emitInterval.getLong(probe)).isEqualTo(7_000L);
    }

    @Test
    void direct_routing_preserves_cell_state_key_without_dynamic_topics() {
        InputEnvelope envelope = new InputEnvelope(123L, "cell-a") {};

        RoutedEnvelope routed = FlinkJobMain.directRoute(envelope);

        assertThat(routed.envelope()).isSameAs(envelope);
        assertThat(routed.stateKey()).isEqualTo("cell-a");
        assertThat(routed.vbucketId()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void pm_event_timestamp_rejects_zero_window_end() {
        assertThatThrownBy(() -> FlinkJobMain.pmEventTimestamp(pm(0L, 0L)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("windowEndTs");
    }

    @Test
    void pm_event_timestamp_rejects_min_value_window_end() {
        assertThatThrownBy(() -> FlinkJobMain.pmEventTimestamp(pm(Long.MIN_VALUE - 1L, Long.MIN_VALUE)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("windowEndTs");
    }

    @Test
    void pm_event_timestamp_rejects_non_positive_window_duration() {
        assertThatThrownBy(() -> FlinkJobMain.pmEventTimestamp(pm(60_000L, 60_000L)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("windowStartTs");
        assertThatThrownBy(() -> FlinkJobMain.pmEventTimestamp(pm(60_000L, 59_999L)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("windowStartTs");
    }

    private static PmStat pm(long start, long end) {
        return PmStat.newBuilder()
            .setSiteId("site-a")
            .setCellId("cell-a")
            .setWindowStartTs(start)
            .setWindowEndTs(end)
            .setPrbUsageDl(0.6f)
            .setPrbUsageUl(0.25f)
            .setActiveUsers(42)
            .setAvgRsrp(-92.0f)
            .setAvgRsrq(-8.0f)
            .setAvgSinr(12.0f)
            .setAvgCqi(10.0f)
            .setAvgMcs(18.0f)
            .setAvgBler(0.02f)
            .setThroughputDlMbps(110.0f)
            .setThroughputUlMbps(30.0f)
            .setDroppedConnections(0)
            .setHandoverSuccess(4)
            .setHandoverFailure(1)
            .setPrachAttempt(9)
            .setPrachFailure(1)
            .setRrcEstabAttempt(20)
            .setRrcEstabSuccess(19)
            .setAvgLatencyMs(18.0f)
            .setPacketLossRate(0.001f)
            .build();
    }
}
