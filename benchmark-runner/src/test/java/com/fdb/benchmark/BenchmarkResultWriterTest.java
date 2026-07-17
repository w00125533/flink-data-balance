package com.fdb.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BenchmarkResultWriterTest {
  @TempDir Path tempDir;

  @Test
  void writes_config_results_and_csv() throws Exception {
    BenchmarkConfig config = BenchmarkConfig.from("local", Map.of(
        "FDB_BENCHMARK_ID", "bench-a",
        "FDB_BENCHMARK_SINKS", "none",
        "FDB_BENCHMARK_CELL_LEVELS", "1000"));
    BenchmarkRunResult result = sampleResult(config);

    new BenchmarkResultWriter(tempDir).write(config, List.of(result));

    assertThat(tempDir.resolve("bench-a/benchmark-config.json")).exists();
    assertThat(tempDir.resolve("bench-a/benchmark-results.json")).exists();
    Path sourceMetrics = tempDir.resolve("bench-a/runs/bench-a-none-cells1000-eps300/source-metrics.json");
    assertThat(sourceMetrics).exists();
    assertThat(Files.readString(sourceMetrics))
        .contains("\"present\" : true")
        .contains("\"chrTargetEps\" : 300")
        .contains("\"chrPublished\" : 600");
    String csv = Files.readString(tempDir.resolve("bench-a/benchmark-summary.csv"));
    assertThat(csv).contains("sink,cellLevel,targetChrEps,status");
    assertThat(csv).contains("none,1000,300,STABLE");
  }

  static BenchmarkRunResult sampleResult(BenchmarkConfig config) {
    BenchmarkRunPlan plan = BenchmarkMatrix.expand(config).get(0);
    return new BenchmarkRunResult(plan, BenchmarkStatus.STABLE, "all thresholds healthy",
        new FlinkSnapshot("RUNNING", 0, 10_000, 0, 300, 300, 1, 4),
        new FdbMetricsSnapshot(1, 2, 3, 4, 0, 5),
        new StorageSnapshot(true, "healthy", 10, 0, 0),
        new SourceMetricsSnapshot(true, 300, 600, 294.0, 1000, 100.0, 1000, 1000.0));
  }
}
