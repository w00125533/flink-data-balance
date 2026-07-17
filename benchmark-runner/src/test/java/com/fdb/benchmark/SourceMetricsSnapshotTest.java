package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

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
        "{\"source\":\"chr\",\"targetEps\":300,\"published\":600,\"observedEps\":294.0}");
    Files.writeString(runDir.resolve("pm-source-metrics.json"),
        "{\"source\":\"pm\",\"published\":1000,\"observedEps\":100.0}");
    Files.writeString(runDir.resolve("cfg-source-metrics.json"),
        "{\"source\":\"cfg\",\"published\":1000,\"observedEps\":1000.0}");

    SourceMetricsSnapshot snapshot = SourceMetricsSnapshot.read(tempDir, plan);

    assertThat(snapshot.present()).isTrue();
    assertThat(snapshot.chrTargetEps()).isEqualTo(300);
    assertThat(snapshot.chrPublished()).isEqualTo(600);
    assertThat(snapshot.chrObservedEps()).isEqualTo(294.0);
    assertThat(snapshot.producerDeliveryRatio()).isEqualTo(0.98);
    assertThat(snapshot.pmTotalPerCell(1000)).isEqualTo(1.0);
    assertThat(snapshot.cfgTotalPerCell(1000)).isEqualTo(1.0);
  }
}
