package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SourceMetricsSnapshotTest {
  @TempDir Path tempDir;

  @Test
  void reads_chr_pm_cfg_metrics_and_computes_delivery() throws Exception {
    BenchmarkRunPlan plan = new BenchmarkRunPlan("bench", BenchmarkSink.STARROCKS, 1000, 300,
        "bench-starrocks-cells1000-eps300", "starrocks");
    Path runDir = SourceMetricsSnapshot.runDir(tempDir, plan);
    Files.createDirectories(runDir);
    Files.writeString(runDir.resolve("chr-source-metrics.json"),
        "{\"source\":\"chr\",\"targetEps\":300,\"published\":600,\"observedEps\":294.0,"
            + "\"durationMs\":2000,\"backlogRecords\":12,\"undeliveredRecords\":12}");
    Files.writeString(runDir.resolve("pm-source-metrics.json"),
        "{\"source\":\"pm\",\"targetEps\":100,\"published\":1000,\"observedEps\":100.0,"
            + "\"durationMs\":10000,\"backlogRecords\":0,\"undeliveredRecords\":0}");
    Files.writeString(runDir.resolve("cfg-source-metrics.json"),
        "{\"source\":\"cfg\",\"targetEps\":250,\"published\":1000,\"observedEps\":1000.0}");

    SourceMetricsSnapshot snapshot = SourceMetricsSnapshot.read(tempDir, plan);

    assertThat(snapshot.present()).isTrue();
    assertThat(snapshot.chrTargetEps()).isEqualTo(300);
    assertThat(snapshot.chrPublished()).isEqualTo(600);
    assertThat(snapshot.chrObservedEps()).isEqualTo(294.0);
    assertThat(snapshot.chrDurationMs()).isEqualTo(2000);
    assertThat(snapshot.chrBacklogRecords()).isEqualTo(12);
    assertThat(snapshot.chrUndeliveredRecords()).isEqualTo(12);
    assertThat(snapshot.hasChrMetrics()).isTrue();
    assertThat(snapshot.pmDurationMs()).isEqualTo(10000);
    assertThat(snapshot.pmTargetEps()).isEqualTo(100);
    assertThat(snapshot.pmBacklogRecords()).isZero();
    assertThat(snapshot.pmUndeliveredRecords()).isZero();
    assertThat(snapshot.cfgTargetEps()).isEqualTo(250);
    assertThat(snapshot.cfgDurationMs()).isZero();
    assertThat(snapshot.cfgBacklogRecords()).isZero();
    assertThat(snapshot.cfgUndeliveredRecords()).isZero();
    assertThat(snapshot.producerDeliveryRatio()).isEqualTo(0.98);
    assertThat(snapshot.pmTotalPerCell(1000)).isEqualTo(1.0);
    assertThat(snapshot.cfgTotalPerCell(1000)).isEqualTo(1.0);
  }

  @Test
  void skips_bad_source_file_and_keeps_other_sources() throws Exception {
    BenchmarkRunPlan plan = new BenchmarkRunPlan("bench", BenchmarkSink.STARROCKS, 1000, 300,
        "bench-starrocks-cells1000-eps300", "starrocks");
    Path runDir = SourceMetricsSnapshot.runDir(tempDir, plan);
    Files.createDirectories(runDir);
    Files.writeString(runDir.resolve("chr-source-metrics.json"), "{\"source\":\"chr\",");
    Files.writeString(runDir.resolve("pm-source-metrics.json"),
        "{\"source\":\"pm\",\"published\":1000,\"observedEps\":100.0,\"durationMs\":10000}");

    SourceMetricsSnapshot snapshot = SourceMetricsSnapshot.read(tempDir, plan);

    assertThat(snapshot.present()).isTrue();
    assertThat(snapshot.hasChrMetrics()).isFalse();
    assertThat(snapshot.chrPublished()).isZero();
    assertThat(snapshot.pmTargetEps()).isZero();
    assertThat(snapshot.pmPublished()).isEqualTo(1000);
    assertThat(snapshot.pmDurationMs()).isEqualTo(10000);
  }

  @Test
  void retries_when_metrics_path_is_temporarily_not_a_file() throws Exception {
    BenchmarkRunPlan plan = new BenchmarkRunPlan("bench", BenchmarkSink.STARROCKS, 1000, 300,
        "bench-starrocks-cells1000-eps300", "starrocks");
    Path runDir = SourceMetricsSnapshot.runDir(tempDir, plan);
    Path chrMetrics = runDir.resolve("chr-source-metrics.json");
    Files.createDirectories(chrMetrics);
    Files.writeString(runDir.resolve("pm-source-metrics.json"),
        "{\"source\":\"pm\",\"published\":1000,\"observedEps\":100.0,\"durationMs\":10000}");

    Thread replacer = new Thread(() -> {
      try {
        Thread.sleep(150L);
        Files.delete(chrMetrics);
        Files.writeString(chrMetrics,
            "{\"source\":\"chr\",\"targetEps\":300,\"published\":600,\"observedEps\":294.0}");
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    });
    replacer.start();

    SourceMetricsSnapshot snapshot;
    try {
      snapshot = SourceMetricsSnapshot.read(tempDir, plan);
    } finally {
      replacer.join();
    }

    assertThat(snapshot.present()).isTrue();
    assertThat(snapshot.chrTargetEps()).isEqualTo(300);
    assertThat(snapshot.chrPublished()).isEqualTo(600);
    assertThat(snapshot.pmPublished()).isEqualTo(1000);
  }

  @Test
  void throws_when_metrics_path_is_directory() throws Exception {
    BenchmarkRunPlan plan = new BenchmarkRunPlan("bench", BenchmarkSink.STARROCKS, 1000, 300,
        "bench-starrocks-cells1000-eps300", "starrocks");
    Path runDir = SourceMetricsSnapshot.runDir(tempDir, plan);
    Files.createDirectories(runDir.resolve("chr-source-metrics.json"));

    assertThatThrownBy(() -> SourceMetricsSnapshot.read(tempDir, plan))
        .isInstanceOf(java.io.IOException.class);
  }
}
