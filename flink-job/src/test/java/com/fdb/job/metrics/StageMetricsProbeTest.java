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
}
