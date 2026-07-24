package com.fdb.job.metrics;

import com.fdb.common.metrics.StageMetricSample;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectorSinkMetricsTest {

    @Test
    void reports_write_and_commit_latency_as_independent_scopes() {
        ConnectorSinkMetrics metrics = new ConnectorSinkMetrics(
            "starrocks-kpi-1m",
            "Cell KPI 1m StarRocks Sink",
            "starrocks",
            "kpi_1m",
            "MIN_1",
            2,
            new MetricRuntimeConfig("run-a", "starrocks", 4, false));

        metrics.recordWriteDurationNanos(5_000_000L);
        metrics.recordWriteDurationNanos(7_000_000L);
        metrics.recordCommitDurationNanos(100_000_000L, 42L);

        StageMetricSample write = metrics.writeSample(1_717_400_000_000L);
        StageMetricSample commit = metrics.commitSample(1_717_400_000_100L);

        assertThat(write.stageId()).isEqualTo("starrocks-kpi-1m.connector-write");
        assertThat(write.displayName()).isEqualTo("Cell KPI 1m StarRocks Sink Connector Write");
        assertThat(write.records()).isEqualTo(2);
        assertThat(write.latencyP95Ms()).isEqualTo(7L);
        assertThat(write.checkpointId()).isEqualTo(-1L);

        assertThat(commit.stageId()).isEqualTo("starrocks-kpi-1m.connector-commit");
        assertThat(commit.displayName()).isEqualTo("Cell KPI 1m StarRocks Sink Connector Commit");
        assertThat(commit.records()).isEqualTo(1);
        assertThat(commit.latencyP95Ms()).isEqualTo(100L);
        assertThat(commit.checkpointId()).isEqualTo(42L);
    }

    @Test
    void tags_connector_samples_with_run_metadata() {
        ConnectorSinkMetrics metrics = new ConnectorSinkMetrics(
            "iceberg-kpi-1m",
            "Cell KPI 1m Iceberg Sink",
            "iceberg",
            "kpi_1m",
            "MIN_1",
            1,
            new MetricRuntimeConfig("run-b", "iceberg", 6, false));

        metrics.open(3);
        metrics.recordCommitDurationNanos(12_000_000L, 100L);

        StageMetricSample commit = metrics.commitSample(1_717_400_000_000L);

        assertThat(commit.runId()).isEqualTo("run-b");
        assertThat(commit.resultSink()).isEqualTo("iceberg");
        assertThat(commit.parallelism()).isEqualTo(6);
        assertThat(commit.subtaskIndex()).isEqualTo(3);
    }
}
