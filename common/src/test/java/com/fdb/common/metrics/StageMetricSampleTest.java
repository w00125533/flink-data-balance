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
    void creates_stage_latency_metric_sample_with_percentiles() {
        StageMetricSample sample = StageMetricSample.stageLatency(
            "kpi-1m", "KPI 1m Full Join", "healthy", 12.0, 10.0,
            8L, 20L, 40L, 500L, 0L, 1_717_400_000_000L);

        assertThat(sample.stageId()).isEqualTo("kpi-1m");
        assertThat(sample.latencyP50Ms()).isEqualTo(8L);
        assertThat(sample.latencyP95Ms()).isEqualTo(20L);
        assertThat(sample.latencyP99Ms()).isEqualTo(40L);
        assertThat(sample.watermarkLagMs()).isEqualTo(500L);
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

    @Test
    void round_trips_sink_latency_metric_sample_as_json() {
        StageMetricSample sample = StageMetricSample.sinkLatency(
            "iceberg-cell-kpi-1m", "Iceberg KPI 1m Sink", "healthy", "iceberg", "kpi_1m", "MIN_1",
            120L, 12000L, 45L, 30L, 45L, 80L, 0L, "", 42L, 1717400000000L);

        StageMetricSample parsed = StageMetricSample.fromJson(sample.toJson());

        assertThat(parsed.stageId()).isEqualTo("iceberg-cell-kpi-1m");
        assertThat(parsed.displayName()).isEqualTo("Iceberg KPI 1m Sink");
        assertThat(parsed.status()).isEqualTo("healthy");
        assertThat(parsed.sinkType()).isEqualTo("iceberg");
        assertThat(parsed.dataset()).isEqualTo("kpi_1m");
        assertThat(parsed.windowKind()).isEqualTo("MIN_1");
        assertThat(parsed.records()).isEqualTo(120L);
        assertThat(parsed.bytes()).isEqualTo(12000L);
        assertThat(parsed.durationMs()).isEqualTo(45L);
        assertThat(parsed.latencyP50Ms()).isEqualTo(30L);
        assertThat(parsed.latencyP95Ms()).isEqualTo(45L);
        assertThat(parsed.latencyP99Ms()).isEqualTo(80L);
        assertThat(parsed.failureCount()).isEqualTo(0L);
        assertThat(parsed.errorMessage()).isEqualTo("");
        assertThat(parsed.checkpointId()).isEqualTo(42L);
        assertThat(parsed.updatedAtEpochMs()).isEqualTo(1717400000000L);
    }

    @Test
    void deserializes_legacy_sink_json_with_absent_sink_latency_fields() {
        String legacyJson = """
            {
              "stageId": "iceberg-sink",
              "displayName": "Iceberg Sink",
              "status": "warning",
              "inEps": 340.0,
              "outEps": 340.0,
              "latencyP95Ms": 420,
              "watermarkLagMs": 0,
              "errorCount": 0,
              "rowsWritten": 340,
              "rebalanceTotal": 0,
              "source": "",
              "sink": "iceberg",
              "window": "1m",
              "updatedAtEpochMs": 1717400000000
            }
            """;

        StageMetricSample parsed = StageMetricSample.fromJson(legacyJson);

        assertThat(parsed.checkpointId()).isEqualTo(-1L);
        assertThat(parsed.sinkType()).isEqualTo("");
        assertThat(parsed.dataset()).isEqualTo("");
        assertThat(parsed.windowKind()).isEqualTo("");
        assertThat(parsed.errorMessage()).isEqualTo("");
        assertThat(parsed.records()).isEqualTo(340L);
        assertThat(parsed.rowsWritten()).isEqualTo(340L);
        assertThat(parsed.sink()).isEqualTo("iceberg");
        assertThat(parsed.window()).isEqualTo("1m");
    }

    @Test
    void deserializes_legacy_json_with_default_run_metadata() {
        String legacyJson = """
            {
              "stageId": "iceberg-sink",
              "displayName": "Iceberg Sink",
              "status": "warning",
              "inEps": 340.0,
              "outEps": 340.0,
              "latencyP95Ms": 420,
              "watermarkLagMs": 0,
              "errorCount": 0,
              "rowsWritten": 340,
              "rebalanceTotal": 0,
              "source": "",
              "sink": "iceberg",
              "window": "1m",
              "updatedAtEpochMs": 1717400000000
            }
            """;

        StageMetricSample parsed = StageMetricSample.fromJson(legacyJson);

        assertThat(parsed.runId()).isEqualTo("unknown-run");
        assertThat(parsed.resultSink()).isEqualTo("");
        assertThat(parsed.parallelism()).isEqualTo(-1);
    }

    @Test
    void round_trips_sink_latency_run_metadata_as_json() {
        StageMetricSample sample = StageMetricSample.sinkLatency(
                "starrocks-cell-kpi-1m", "StarRocks KPI 1m Sink", "healthy", "starrocks", "kpi_1m", "MIN_1",
                120L, 12000L, 45L, 30L, 45L, 80L, 0L, "", 42L, 1717400000000L)
            .withRunMetadata("run-a", "starrocks", 4);

        StageMetricSample parsed = StageMetricSample.fromJson(sample.toJson());

        assertThat(parsed.runId()).isEqualTo("run-a");
        assertThat(parsed.resultSink()).isEqualTo("starrocks");
        assertThat(parsed.parallelism()).isEqualTo(4);
    }
}
