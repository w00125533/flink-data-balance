package com.fdb.job;

import com.fdb.job.model.InputEnvelope;
import com.fdb.job.model.RoutedEnvelope;
import com.fdb.job.sink.IcebergConfig;
import com.fdb.common.avro.PmStat;
import org.junit.jupiter.api.Test;

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
    void resolve_checkpoint_interval_defaults_to_one_minute() {
        assertThat(FlinkJobMain.resolveCheckpointIntervalMs(Map.of(), new Properties()))
            .isEqualTo(60_000L);
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
            .isEqualTo(60_000L);
        assertThat(FlinkJobMain.resolveCheckpointIntervalMs(
            Map.of("FDB_FLINK_CHECKPOINT_INTERVAL_MS", "0"), new Properties()))
            .isEqualTo(60_000L);
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
