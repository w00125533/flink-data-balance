package com.fdb.job.config;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class ResultSinkConfigTest {

    @Test
    void defaults_to_starrocks_with_dlq_and_metrics_enabled() {
        ResultSinkConfig config = ResultSinkConfig.resolve(Map.of(), new Properties());

        assertThat(config.resultSink()).isEqualTo(ResultSinkType.STARROCKS);
        assertThat(config.dlqEnabled()).isTrue();
        assertThat(config.metricsEnabled()).isTrue();
        assertThat(config.metricsHistoryEnabled()).isTrue();
        assertThat(config.metricsEmitIntervalMs()).isEqualTo(5_000L);
        assertThat(config.reportOnStop()).isFalse();
        assertThat(config.runId()).isNotBlank();
    }

    @Test
    void resolves_environment_over_properties() {
        Properties properties = new Properties();
        properties.setProperty("fdb.result.sink", "hive");
        properties.setProperty("fdb.dlq.enabled", "true");
        properties.setProperty("fdb.metrics.enabled", "true");
        properties.setProperty("fdb.metrics.history.enabled", "true");
        properties.setProperty("fdb.metrics.emit.interval.ms", "1000");
        properties.setProperty("fdb.report.on.stop", "false");
        properties.setProperty("fdb.run.id", "property-run");
        properties.setProperty("fdb.run.label", "property-label");

        ResultSinkConfig config = ResultSinkConfig.resolve(Map.of(
            "FDB_RESULT_SINK", "iceberg",
            "FDB_DLQ_ENABLED", "false",
            "FDB_METRICS_ENABLED", "false",
            "FDB_METRICS_HISTORY_ENABLED", "false",
            "FDB_METRICS_EMIT_INTERVAL_MS", "7000",
            "FDB_REPORT_ON_STOP", "true",
            "FDB_RUN_ID", "run-a",
            "FDB_RUN_LABEL", "iceberg-p4"), properties);

        assertThat(config.resultSink()).isEqualTo(ResultSinkType.ICEBERG);
        assertThat(config.dlqEnabled()).isFalse();
        assertThat(config.metricsEnabled()).isFalse();
        assertThat(config.metricsHistoryEnabled()).isFalse();
        assertThat(config.metricsEmitIntervalMs()).isEqualTo(7_000L);
        assertThat(config.reportOnStop()).isTrue();
        assertThat(config.runId()).isEqualTo("run-a");
        assertThat(config.runLabel()).isEqualTo("iceberg-p4");
    }

    @Test
    void invalid_sink_and_invalid_or_zero_interval_fall_back_to_safe_defaults() {
        ResultSinkConfig invalidInterval = ResultSinkConfig.resolve(Map.of(
            "FDB_RESULT_SINK", "not-a-sink",
            "FDB_METRICS_EMIT_INTERVAL_MS", "not-a-number"), new Properties());
        ResultSinkConfig zeroInterval = ResultSinkConfig.resolve(Map.of(
            "FDB_METRICS_EMIT_INTERVAL_MS", "0"), new Properties());

        assertThat(invalidInterval.resultSink()).isEqualTo(ResultSinkType.STARROCKS);
        assertThat(invalidInterval.metricsEmitIntervalMs()).isEqualTo(5_000L);
        assertThat(zeroInterval.metricsEmitIntervalMs()).isEqualTo(5_000L);
    }

    @Test
    void boolean_options_accept_on_off_and_invalid_values_fall_back_to_defaults() {
        ResultSinkConfig offConfig = ResultSinkConfig.resolve(Map.of(
            "FDB_METRICS_ENABLED", " off ",
            "FDB_DLQ_ENABLED", "OFF",
            "FDB_REPORT_ON_STOP", "on"), new Properties());
        ResultSinkConfig invalidConfig = ResultSinkConfig.resolve(Map.of(
            "FDB_METRICS_ENABLED", "maybe",
            "FDB_REPORT_ON_STOP", "maybe"), new Properties());

        assertThat(offConfig.metricsEnabled()).isFalse();
        assertThat(offConfig.dlqEnabled()).isFalse();
        assertThat(offConfig.reportOnStop()).isTrue();
        assertThat(invalidConfig.metricsEnabled()).isTrue();
        assertThat(invalidConfig.reportOnStop()).isFalse();
    }

    @Test
    void file_sink_checkpoint_interval_is_capped_at_three_minutes() {
        assertThat(ResultSinkConfig.effectiveCheckpointIntervalMs(ResultSinkType.HIVE, 240_000L))
            .isEqualTo(180_000L);
        assertThat(ResultSinkConfig.effectiveCheckpointIntervalMs(ResultSinkType.ICEBERG, 240_000L))
            .isEqualTo(180_000L);
        assertThat(ResultSinkConfig.effectiveCheckpointIntervalMs(ResultSinkType.STARROCKS, 240_000L))
            .isEqualTo(240_000L);
    }
}
