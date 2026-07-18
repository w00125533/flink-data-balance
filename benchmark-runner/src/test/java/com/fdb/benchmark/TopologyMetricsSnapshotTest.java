package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TopologyMetricsSnapshotTest {
  @TempDir Path tempDir;

  @Test
  void reads_topology_metrics_file_and_builds_run_relative_path() throws Exception {
    BenchmarkRunPlan plan = new BenchmarkRunPlan(
        "bench-a", BenchmarkSink.STARROCKS, 1000,
        0.3, 300, 0.1, 100, "bench-a-starrocks-cells1000-chr-eps0.3", "benchmark-starrocks");
    Path metricsFile = TopologyMetricsSnapshot.metricsFile(tempDir, plan);
    Files.createDirectories(metricsFile.getParent());
    Files.writeString(metricsFile, """
        {
          "generatedRecords": 6000,
          "siteCount": 1000,
          "frequencyBandCount": 4,
          "generationDurationMs": 120,
          "publishDurationMs": 450,
          "totalDurationMs": 570,
          "publishedRecords": 6000,
          "publishFailures": 0,
          "minLatitude": 30.1,
          "maxLatitude": 31.2,
          "minLongitude": 120.3,
          "maxLongitude": 121.4
        }
        """);

    TopologyMetricsSnapshot snapshot = TopologyMetricsSnapshot.read(metricsFile);

    assertThat(metricsFile).isEqualTo(tempDir.resolve(
        "bench-a/runs/bench-a-starrocks-cells1000-chr-eps0.3/topology-metrics.json"));
    assertThat(snapshot.generatedRecords()).isEqualTo(6000);
    assertThat(snapshot.siteCount()).isEqualTo(1000);
    assertThat(snapshot.publishDurationMs()).isEqualTo(450);
    assertThat(snapshot.publishFailures()).isZero();
  }

  @Test
  void missing_topology_metrics_file_returns_empty_snapshot() throws Exception {
    assertThat(TopologyMetricsSnapshot.read(tempDir.resolve("missing.json")).present()).isFalse();
  }
}
