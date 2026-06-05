package com.fdb.common.metrics;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StageMetricSampleTest {

    @Test
    void round_trips_stage_metric_sample_as_json() {
        StageMetricSample sample = StageMetricSample.stage(
            "enrichment", "Enrichment Process", "healthy", 120.5, 118.0, 42, 300, 2, 1_717_400_000_000L);

        StageMetricSample parsed = StageMetricSample.fromJson(sample.toJson());

        assertThat(parsed.stageId()).isEqualTo("enrichment");
        assertThat(parsed.displayName()).isEqualTo("Enrichment Process");
        assertThat(parsed.status()).isEqualTo("healthy");
        assertThat(parsed.inEps()).isEqualTo(120.5);
        assertThat(parsed.outEps()).isEqualTo(118.0);
        assertThat(parsed.latencyP95Ms()).isEqualTo(42);
        assertThat(parsed.watermarkLagMs()).isEqualTo(300);
        assertThat(parsed.errorCount()).isEqualTo(2);
        assertThat(parsed.updatedAtEpochMs()).isEqualTo(1_717_400_000_000L);
    }

    @Test
    void creates_sink_metric_sample_with_sink_labels() {
        StageMetricSample sample = StageMetricSample.sink(
            "iceberg-sink", "Iceberg Sink", "warning", "iceberg", "1m", 340, 420, 1_717_400_000_000L);

        assertThat(sample.stageId()).isEqualTo("iceberg-sink");
        assertThat(sample.sink()).isEqualTo("iceberg");
        assertThat(sample.window()).isEqualTo("1m");
        assertThat(sample.rowsWritten()).isEqualTo(340);
        assertThat(sample.outEps()).isEqualTo(340.0);
        assertThat(sample.latencyP95Ms()).isEqualTo(420);
    }
}
