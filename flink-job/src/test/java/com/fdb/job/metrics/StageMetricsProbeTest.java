package com.fdb.job.metrics;

import com.fdb.common.metrics.StageMetricSample;
import org.junit.jupiter.api.Test;

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
}
