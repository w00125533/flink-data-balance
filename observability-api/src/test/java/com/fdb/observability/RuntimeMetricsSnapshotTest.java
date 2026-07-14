package com.fdb.observability;

import com.fdb.common.metrics.StageMetricSample;
import com.fdb.observability.service.ObservabilitySnapshotService;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RuntimeMetricsSnapshotTest {

  @Test
  void prometheus_metrics_are_rendered_from_runtime_samples() {
    ObservabilitySnapshotService service = new ObservabilitySnapshotService();
    service.applyMetricSample(StageMetricSample.stage(
        "enrichment", "Enrichment Process", "healthy", 100.0, 95.0, 35, 250, 1, 1_717_400_000_000L));
    service.applyMetricSample(StageMetricSample.sinkLatency(
        "iceberg-kpi-1m", "Cell KPI 1m Iceberg Sink", "warning", "iceberg", "kpi_1m", "MIN_1",
        340, 12_000, 500, 210, 420, 520, 0, "", -1, 1_717_400_001_000L));

    String metrics = ObservabilityApiMain.toPrometheusMetrics(service);

    assertThat(metrics).contains(
        "fdb_stage_in_eps{stage=\"enrichment\"} 100.0",
        "fdb_stage_out_eps{stage=\"enrichment\"} 95.0",
        "fdb_stage_latency_ms{stage=\"enrichment\",quantile=\"p95\"} 35",
        "fdb_stage_watermark_lag_ms{stage=\"enrichment\"} 250",
        "fdb_stage_error_total{stage=\"enrichment\"} 1",
        "fdb_sink_write_rows_total{sink=\"iceberg-kpi-1m\",window=\"1m\"} 340",
        "fdb_sink_write_latency_ms{sink=\"iceberg-kpi-1m\",window=\"1m\"} 420");
  }
}
