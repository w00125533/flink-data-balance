package com.fdb.job.metrics;

import com.fdb.common.metrics.StageMetricSample;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class StageMetricsProbeTest {

    @Test
    void builds_stage_metric_sample_from_record_counts() {
        StageMetricsProbe<String> probe = new StageMetricsProbe<>(
            "kafka", "Kafka Topics", "healthy", 5_000L);
        probe.record("a", 1_000L);
        probe.record("b", 2_000L);

        List<StageMetricSample> samples = probe.drainDueSamples(6_000L);

        assertThat(samples).singleElement().satisfies(sample -> {
            assertThat(sample.stageId()).isEqualTo("kafka");
            assertThat(sample.displayName()).isEqualTo("Kafka Topics");
            assertThat(sample.inEps()).isEqualTo(0.4);
            assertThat(sample.outEps()).isEqualTo(0.4);
            assertThat(sample.status()).isEqualTo("healthy");
        });
    }

    @Test
    void tags_stage_metric_sample_with_run_metadata() {
        StageMetricsProbe<String> probe = new StageMetricsProbe<>(
            "kafka", "Kafka Topics", "healthy", 5_000L,
            new MetricRuntimeConfig("run-a", "starrocks", 4, true));
        probe.record("a", 1_000L);
        probe.record("b", 2_000L);

        List<StageMetricSample> samples = probe.drainDueSamples(6_000L);

        assertThat(samples).singleElement().satisfies(sample -> {
            assertThat(sample.runId()).isEqualTo("run-a");
            assertThat(sample.resultSink()).isEqualTo("starrocks");
            assertThat(sample.parallelism()).isEqualTo(4);
        });
    }

    @Test
    void metrics_disabled_does_not_prevent_drain_due_samples() {
        StageMetricsProbe<String> probe = new StageMetricsProbe<>(
            "kafka", "Kafka Topics", "healthy", 5_000L,
            new MetricRuntimeConfig("run-a", "starrocks", 4, false));
        probe.record("a", 1_000L);

        List<StageMetricSample> samples = probe.drainDueSamples(6_000L);

        assertThat(samples).singleElement().satisfies(sample -> {
            assertThat(sample.stageId()).isEqualTo("kafka");
            assertThat(sample.runId()).isEqualTo("run-a");
        });
    }

    @Test
    void records_latency_percentiles_from_event_timestamp() {
        StageMetricsProbe<String> probe = new StageMetricsProbe<>(
            "chr-source", "CHR Source", "healthy", 5_000L,
            new MetricRuntimeConfig("run-a", "starrocks", 4, false),
            Long::parseLong);

        probe.record("1000", 2_000L, 1_500L);
        probe.record("1300", 2_000L, 1_500L);
        probe.record("1800", 2_000L, 1_500L);

        List<StageMetricSample> samples = probe.drainDueSamples(7_000L);

        assertThat(samples).singleElement().satisfies(sample -> {
            assertThat(sample.latencyP50Ms()).isEqualTo(700L);
            assertThat(sample.latencyP95Ms()).isEqualTo(1_000L);
            assertThat(sample.latencyP99Ms()).isEqualTo(1_000L);
            assertThat(sample.watermarkLagMs()).isEqualTo(500L);
        });
    }

    @Test
    void snapshot_checkpoint_drains_sparse_stage_samples_without_later_records() throws Exception {
        StageMetricsProbe<String> probe = new StageMetricsProbe<>(
            "kpi-5m", "KPI 5m Rollup", "healthy", 5_000L,
            new MetricRuntimeConfig("run-a", "starrocks", 4, false),
            value -> 1_000L);

        probe.record("only-value", 1_000L);

        probe.snapshotState(null);

        assertThat(longField(probe, "eventsSinceLastEmit")).isZero();
        assertThat(longField(probe, "lastEmitAtMs")).isGreaterThanOrEqualTo(1_000L);
    }

    private static long longField(StageMetricsProbe<?> probe, String name) throws Exception {
        Field field = StageMetricsProbe.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.getLong(probe);
    }
}
