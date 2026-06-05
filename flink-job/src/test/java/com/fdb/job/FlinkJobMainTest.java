package com.fdb.job;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class FlinkJobMainTest {

    @Test
    void resolve_parallelism_defaults_to_single_slot() {
        assertThat(FlinkJobMain.resolveParallelism(Map.of(), new Properties())).isEqualTo(1);
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
            .isEqualTo(1);
        assertThat(FlinkJobMain.resolveParallelism(
            Map.of("FDB_FLINK_PARALLELISM", "0"), new Properties()))
            .isEqualTo(1);
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
        assertThat(config.warehouse()).isEqualTo("file:///warehouse/iceberg");
    }

    @Test
    void resolve_iceberg_config_can_disable_sink() {
        IcebergConfig config = FlinkJobMain.resolveIcebergConfig(
            Map.of("FDB_ICEBERG_ENABLED", "false"), new Properties());

        assertThat(config.enabled()).isFalse();
    }
}
