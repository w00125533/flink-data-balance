package com.fdb.job.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdb.common.metrics.StageMetricSample;
import org.junit.jupiter.api.Test;

class WindowMaterializationProbeTest {
    @Test
    void emits_window_materialization_sample_with_window_end_label_and_source_event_latency() {
        MetricRuntimeConfig metricConfig = new MetricRuntimeConfig("run-a", "iceberg", 6, true);

        StageMetricSample sample = WindowMaterializationProbe.sample(
            "window-chr-1m", "CHR 1m Materialization", "chr-1m", "MIN_1",
            60_000L, 100_000L, 1_000L, metricConfig, 2, 123_456L);

        assertThat(sample.stageId()).isEqualTo("window-chr-1m");
        assertThat(sample.sinkType()).isEqualTo("window-materialization");
        assertThat(sample.dataset()).isEqualTo("chr-1m");
        assertThat(sample.windowKind()).isEqualTo("MIN_1@60000");
        assertThat(sample.records()).isEqualTo(1_000L);
        assertThat(sample.latencyP50Ms()).isEqualTo(23_456L);
        assertThat(sample.latencyP95Ms()).isEqualTo(23_456L);
        assertThat(sample.latencyP99Ms()).isEqualTo(23_456L);
        assertThat(sample.runId()).isEqualTo("run-a");
        assertThat(sample.resultSink()).isEqualTo("iceberg");
        assertThat(sample.parallelism()).isEqualTo(6);
        assertThat(sample.subtaskIndex()).isEqualTo(2);
    }

    @Test
    void clamps_window_materialization_latency_to_zero_when_processing_time_is_behind_window_end() {
        MetricRuntimeConfig metricConfig = new MetricRuntimeConfig("run-a", "iceberg", 6, true);

        StageMetricSample sample = WindowMaterializationProbe.sample(
            "window-chr-1m", "CHR 1m Materialization", "chr-1m", "MIN_1",
            60_000L, 80_000L, 1_000L, metricConfig, 2, 59_000L);

        assertThat(sample.latencyP50Ms()).isZero();
        assertThat(sample.latencyP95Ms()).isZero();
        assertThat(sample.latencyP99Ms()).isZero();
    }

    @Test
    void emits_completed_window_only_after_a_newer_window_is_seen() {
        MetricRuntimeConfig metricConfig = new MetricRuntimeConfig("run-a", "iceberg", 6, true);
        WindowMaterializationProbe<Object> probe = new WindowMaterializationProbe<>(
            "window-chr-1m", "CHR 1m Materialization", "chr-1m", "MIN_1",
            ignored -> 0L, ignored -> 0L, metricConfig);

        assertThat(probe.recordWindowMaterialization(60_000L, 30_000L, 1_000L)).isEmpty();
        assertThat(probe.recordWindowMaterialization(60_000L, 40_000L, 1_001L)).isEmpty();

        assertThat(probe.recordWindowMaterialization(120_000L, 90_000L, 1_002L))
            .singleElement()
            .satisfies(sample -> {
                assertThat(sample.windowKind()).isEqualTo("MIN_1@60000");
                assertThat(sample.records()).isEqualTo(2L);
                assertThat(sample.latencyP95Ms()).isZero();
            });

        assertThat(probe.recordWindowMaterialization(120_000L, 100_000L, 1_003L)).isEmpty();
        assertThat(probe.flushWindowMaterializationSamples(1_004L))
            .singleElement()
            .satisfies(sample -> {
                assertThat(sample.windowKind()).isEqualTo("MIN_1@120000");
                assertThat(sample.records()).isEqualTo(2L);
            });
    }
}
